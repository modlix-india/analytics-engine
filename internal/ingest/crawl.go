package ingest

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
)

// NameCrawl is one page fetched by a known crawler, as reported by the server that answered
// it. Crawlers do not run the beacon (most AI crawlers run no JavaScript at all), so the only
// place their visits can be seen is the server that rendered the page.
const NameCrawl = "$crawl"

// CrawlSiteSuffix files crawler visits under a site of their own, beside the site's visitors
// rather than among them.
//
// That is the whole isolation story: every existing widget reads the site's own partition, so
// none of them can count a crawler as a page view, an event, an entry page or a visit, and
// nothing that already works had to learn to skip a new event name. The crawl widgets read
// CrawlSite(site), computed here from the site the caller was authorised for, so the
// boundary on reads is unchanged. A '.' cannot appear in a Modlix site key (app and client
// codes have none), so the suffix cannot collide with a real site.
const CrawlSiteSuffix = ".crawl"

// CrawlSite is where a site's crawler visits are kept.
func CrawlSite(site string) string { return site + CrawlSiteSuffix }

// ChannelCrawler marks a crawler visit by a bot that is not an AI one (Googlebot, bingbot).
// AI bots carry ChannelAI.
const ChannelCrawler = "crawler"

// crawlBatch is what a rendering server sends to POST /c.
//
// Unlike /i this is not a public contract with deployed browsers: one caller, our own SSR, and
// it is authenticated. The keys are still short, for the same reason as the beacon's.
type crawlBatch struct {
	Batch []crawlHit `json:"b"`
}

type crawlHit struct {
	Site   string `json:"s"`   // the site key, e.g. appcode_CLIENTCODE
	Path   string `json:"u"`   // the path that was asked for
	Page   string `json:"g"`   // the page name, when the server knows it
	Bot    string `json:"bot"` // the crawler's product token, e.g. GPTBot
	AI     bool   `json:"ai"`  // an AI crawler or assistant fetcher
	Status int    `json:"st"`  // the HTTP status the page answered with
}

type CrawlOptions struct {
	Sink Sink
	Log  *slog.Logger

	// Secret is the bearer token the rendering server sends. Empty refuses everything; the
	// route is not registered at all without one, and this is the second line.
	Secret string

	MaxBatchEvents int
	MaxBodyBytes   int64

	OnEvent func(outcome string)
	Now     func() time.Time
}

type CrawlIngester struct {
	opt CrawlOptions
}

func NewCrawl(o CrawlOptions) *CrawlIngester {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxBatchEvents <= 0 {
		o.MaxBatchEvents = 500
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20
	}
	return &CrawlIngester{opt: o}
}

// Handle serves POST /c.
//
// The site is taken from the body, which is safe only because the caller is authenticated:
// the server that sends this resolved the site itself, from the same security lookup the
// gateway uses. 403 on a wrong secret rather than /i's uniform 204, because the only caller
// is our own server and a misconfigured one should be diagnosable from its own logs.
func (c *CrawlIngester) Handle(w http.ResponseWriter, r *http.Request) {
	if !c.authorised(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var req crawlBatch
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, c.opt.MaxBodyBytes)).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if len(req.Batch) > c.opt.MaxBatchEvents {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}

	now := c.opt.Now().UnixMilli()
	var seq int64
	for idx := range req.Batch {
		h := &req.Batch[idx]
		site, bot, path, ok := cleanCrawlHit(h)
		if !ok {
			c.count("crawl_dropped_invalid")
			continue
		}

		channel := ChannelCrawler
		if h.AI {
			channel = ChannelAI
		}
		status := h.Status
		if status < 100 || status > 599 {
			status = 0
		}

		e := event.Event{
			TSServer: now + seq,
			TSClient: now + seq,
			Site:     CrawlSite(site),
			Name:     NameCrawl,
			Path:     path,
			Page:     clip(h.Page, 128),
			Label:    bot,
			Channel:  channel,
			Platform: "web",
			Props:    `{"status":` + strconv.Itoa(status) + `}`,
		}
		seq++

		if err := c.opt.Sink.Enqueue(event.Encode(nil, &e)); err != nil {
			c.count("crawl_dropped_sink")
			continue
		}
		c.count("crawl_accepted")
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *CrawlIngester) authorised(r *http.Request) bool {
	if c.opt.Secret == "" {
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(c.opt.Secret)) == 1
}

func (c *CrawlIngester) count(outcome string) {
	if c.opt.OnEvent != nil {
		c.opt.OnEvent(outcome)
	}
}

// cleanCrawlHit validates one hit and normalises its path the way page URLs are normalised
// on /i, so "/pricing" and "/pricing/?x=1" are one row in a crawl report as in a traffic one.
func cleanCrawlHit(h *crawlHit) (site, bot, path string, ok bool) {
	site = strings.TrimSpace(h.Site)
	if site == "" || len(site) > 128 || strings.HasSuffix(site, CrawlSiteSuffix) {
		return "", "", "", false
	}
	for _, r := range site {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return "", "", "", false
		}
	}

	bot = strings.TrimSpace(h.Bot)
	if bot == "" || len(bot) > 64 {
		return "", "", "", false
	}
	for _, r := range bot {
		if r < 0x20 || r > 0x7e {
			return "", "", "", false
		}
	}

	path = h.Path
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	if !strings.HasPrefix(path, "/") {
		return "", "", "", false
	}
	if len(path) > 1 {
		path = strings.TrimRight(path, "/")
		if path == "" {
			path = "/"
		}
	}
	return site, bot, clip(path, 512), true
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
