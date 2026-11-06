package query

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/modlix-india/analytics-engine/internal/event"
	"github.com/modlix-india/analytics-engine/internal/ingest"
)

// The answer engine widgets: which crawlers fetched which pages, and how many visits came from
// an AI answer. Both read raw Parquet through a narrow projection, because both need two
// dimensions at once (bot and page, assistant and landing page) and a rollup holds one.
//
// Neither is large. Crawler visits are a few thousand rows a week on a busy site, and the
// referral scan keeps only the rows that came from an assistant.

const (
	WidgetCrawlerVisits = "crawlerVisits"
	WidgetAIReferrals   = "aiReferrals"
)

// Crawl is the crawlerVisits answer.
type Crawl struct {
	// Hits is every crawler page fetch in the range, AIHits the ones by AI crawlers.
	Hits   int64 `json:"hits"`
	AIHits int64 `json:"aiHits"`

	Bots  []CrawlBot  `json:"bots"`
	Pages []CrawlPage `json:"pages"`
	Days  []CrawlDay  `json:"days"`
}

type CrawlBot struct {
	Bot   string `json:"bot"`
	AI    bool   `json:"ai"`
	Hits  int64  `json:"hits"`
	Pages int    `json:"pages"`
	Last  string `json:"last"`
}

// CrawlPage is one bot on one page. Errors counts the fetches that answered 4xx or 5xx, which
// is the actionable part: a crawler that keeps getting a 404 is reading a page that is gone.
type CrawlPage struct {
	Bot    string `json:"bot"`
	AI     bool   `json:"ai"`
	Path   string `json:"path"`
	Hits   int64  `json:"hits"`
	Errors int64  `json:"errors,omitempty"`
	Status int    `json:"status"` // the latest status this bot got for this page
	Last   string `json:"last"`
}

type CrawlDay struct {
	Date   string `json:"date"`
	Hits   int64  `json:"hits"`
	AIHits int64  `json:"aiHits"`
}

// AIReferrals is the aiReferrals answer.
type AIReferrals struct {
	// Visits are sessions that arrived from an assistant, Visitors the visitor-days behind
	// them (see VisitorsDaily), Pageviews every page those visits saw.
	Visits    int64 `json:"visits"`
	Visitors  int64 `json:"visitors"`
	Pageviews int64 `json:"pageviews"`

	Landing []AILanding `json:"landing"`
	Days    []AIDay     `json:"days"`
}

type AILanding struct {
	Source string `json:"source"`
	Path   string `json:"path"`
	Visits int64  `json:"visits"`
}

type AIDay struct {
	Date   string `json:"date"`
	Visits int64  `json:"visits"`
}

// crawlRow is the projection crawlerVisits reads.
type crawlRow struct {
	TSServer int64  `parquet:"ts_server,timestamp,delta"`
	Name     string `parquet:"event,dict,zstd"`
	Path     string `parquet:"path,dict,zstd"`
	Label    string `parquet:"label,dict,zstd"`
	Channel  string `parquet:"channel,dict,zstd"`
	Props    string `parquet:"props,json,zstd"`
}

// crawlerVisits lists the pages crawlers fetched, by bot. AIOnly narrows it to AI crawlers.
func (e *Engine) crawlerVisits(ctx context.Context, req Request, loc *time.Location) (*Result, error) {
	site := ingest.CrawlSite(req.Site)

	type pageKey struct{ bot, path string }
	type pageAcc struct {
		ai           bool
		hits, errors int64
		status       int
		last         int64
	}
	pages := map[pageKey]*pageAcc{}
	days := map[string]*CrawlDay{}
	c := &Crawl{}

	for _, date := range utcDates(span{req.From, req.To}) {
		files, err := e.filesIn(ctx, "data", site, date)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rows, err := parquet.ReadFile[crawlRow](f)
			if err != nil {
				return nil, fmt.Errorf("read raw %s: %w", filepath.Base(f), err)
			}
			for i := range rows {
				r := &rows[i]
				if r.Name != ingest.NameCrawl {
					continue
				}
				ts := time.UnixMilli(r.TSServer)
				if ts.Before(req.From) || !ts.Before(req.To) {
					continue
				}
				ai := r.Channel == ingest.ChannelAI
				if req.AIOnly && !ai {
					continue
				}
				status := statusOf(r.Props)

				k := pageKey{r.Label, r.Path}
				a := pages[k]
				if a == nil {
					a = &pageAcc{ai: ai}
					pages[k] = a
				}
				a.hits++
				if status >= 400 {
					a.errors++
				}
				if r.TSServer >= a.last {
					a.last, a.status = r.TSServer, status
				}

				day := ts.In(loc).Format("2006-01-02")
				d := days[day]
				if d == nil {
					d = &CrawlDay{Date: day}
					days[day] = d
				}
				d.Hits++
				c.Hits++
				if ai {
					d.AIHits++
					c.AIHits++
				}
			}
		}
	}

	bots := map[string]*CrawlBot{}
	for k, a := range pages {
		last := time.UnixMilli(a.last).In(loc).Format(time.RFC3339)
		c.Pages = append(c.Pages, CrawlPage{
			Bot: k.bot, AI: a.ai, Path: k.path, Hits: a.hits, Errors: a.errors, Status: a.status, Last: last,
		})
		b := bots[k.bot]
		if b == nil {
			b = &CrawlBot{Bot: k.bot, AI: a.ai}
			bots[k.bot] = b
		}
		b.Hits += a.hits
		b.Pages++
		if last > b.Last {
			b.Last = last
		}
	}
	for _, b := range bots {
		c.Bots = append(c.Bots, *b)
	}
	sort.Slice(c.Bots, func(i, j int) bool {
		if c.Bots[i].Hits != c.Bots[j].Hits {
			return c.Bots[i].Hits > c.Bots[j].Hits
		}
		return c.Bots[i].Bot < c.Bots[j].Bot
	})
	sort.Slice(c.Pages, func(i, j int) bool {
		a, b := c.Pages[i], c.Pages[j]
		if a.Hits != b.Hits {
			return a.Hits > b.Hits
		}
		if a.Bot != b.Bot {
			return a.Bot < b.Bot
		}
		return a.Path < b.Path
	})
	if req.Limit > 0 && len(c.Pages) > req.Limit {
		c.Pages = c.Pages[:req.Limit]
	}
	c.Days = make([]CrawlDay, 0, len(days))
	for _, d := range days {
		c.Days = append(c.Days, *d)
	}
	sort.Slice(c.Days, func(i, j int) bool { return c.Days[i].Date < c.Days[j].Date })

	res := &Result{Widget: req.Widget, Site: req.Site, Crawl: c, Total: &PeriodTotal{Events: c.Hits}}
	// The rows a chart binds to, the same shape as every other breakdown: one per bot.
	for _, b := range c.Bots {
		res.Rows = append(res.Rows, Row{Label: b.Bot, Events: b.Hits})
	}
	return res, nil
}

// statusOf reads the status the crawl ingest put in props. Zero when it is absent.
func statusOf(props string) int {
	if props == "" {
		return 0
	}
	var p struct {
		Status json.Number `json:"status"`
	}
	if json.Unmarshal([]byte(props), &p) != nil {
		return 0
	}
	n, _ := strconv.Atoi(p.Status.String())
	return n
}

// aiRefRow is the projection aiReferrals reads.
type aiRefRow struct {
	TSServer     int64  `parquet:"ts_server,timestamp,delta"`
	Name         string `parquet:"event,dict,zstd"`
	Path         string `parquet:"path,dict,zstd"`
	Visitor      string `parquet:"visitor,zstd"`
	Session      string `parquet:"session,zstd"`
	ReferrerHost string `parquet:"referrer_host,dict,zstd"`
	Channel      string `parquet:"channel,dict,zstd"`
	UTMSource    string `parquet:"utm_source,dict,zstd"`
}

// aiReferrals counts the visits that arrived from an AI answer, by assistant, and where they
// landed.
//
// The assistant is decided again here from the referrer and utm_source rather than read from
// the channel column, so the answer covers events stored before the "ai" channel existed,
// when the same visits were filed as referral or organic. Paid and internal rows are left out,
// as ingest does.
//
// Rows: one per assistant, Events the page views, Visits the sessions, Visitors exact distinct
// visitor ids (visitor-days across a UTC midnight, flagged by VisitorsDaily). Audience is the
// site's whole traffic over the range, so a share can be taken without a second query.
func (e *Engine) aiReferrals(ctx context.Context, req Request, loc *time.Location) (*Result, error) {
	type sessAcc struct {
		source  string
		first   int64
		landing string
	}
	type srcAcc struct {
		views    int64
		sessions map[string]bool
		visitors map[string]bool
	}
	sessions := map[string]*sessAcc{}
	sources := map[string]*srcAcc{}
	allVisitors := map[string]bool{}
	ar := &AIReferrals{}

	for _, date := range utcDates(span{req.From, req.To}) {
		files, err := e.filesIn(ctx, "data", req.Site, date)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rows, err := parquet.ReadFile[aiRefRow](f)
			if err != nil {
				return nil, fmt.Errorf("read raw %s: %w", filepath.Base(f), err)
			}
			for i := range rows {
				r := &rows[i]
				if r.Name != event.NamePageview || r.Channel == "paid" || r.Channel == "internal" {
					continue
				}
				source := ingest.AISource(r.ReferrerHost, r.UTMSource)
				if source == "" {
					continue
				}
				ts := time.UnixMilli(r.TSServer)
				if ts.Before(req.From) || !ts.Before(req.To) {
					continue
				}

				s := sources[source]
				if s == nil {
					s = &srcAcc{sessions: map[string]bool{}, visitors: map[string]bool{}}
					sources[source] = s
				}
				s.views++
				ar.Pageviews++
				if r.Visitor != "" {
					s.visitors[r.Visitor] = true
					allVisitors[r.Visitor] = true
				}
				// A page view with no session (an old SDK, storage refused) is still one
				// arrival; keyed by its own timestamp so it counts once.
				sid := r.Session
				if sid == "" {
					sid = "\x00" + r.Visitor + "\x00" + strconv.FormatInt(r.TSServer, 10)
				}
				s.sessions[sid] = true
				if a := sessions[sid]; a == nil || r.TSServer < a.first {
					sessions[sid] = &sessAcc{source: source, first: r.TSServer, landing: r.Path}
				}
			}
		}
	}

	type landKey struct{ source, path string }
	landing := map[landKey]int64{}
	days := map[string]int64{}
	for _, a := range sessions {
		landing[landKey{a.source, a.landing}]++
		days[time.UnixMilli(a.first).In(loc).Format("2006-01-02")]++
	}
	ar.Visits = int64(len(sessions))
	ar.Visitors = int64(len(allVisitors))
	for k, n := range landing {
		ar.Landing = append(ar.Landing, AILanding{Source: k.source, Path: k.path, Visits: n})
	}
	sort.Slice(ar.Landing, func(i, j int) bool {
		a, b := ar.Landing[i], ar.Landing[j]
		if a.Visits != b.Visits {
			return a.Visits > b.Visits
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Path < b.Path
	})
	if req.Limit > 0 && len(ar.Landing) > req.Limit {
		ar.Landing = ar.Landing[:req.Limit]
	}
	for d, n := range days {
		ar.Days = append(ar.Days, AIDay{Date: d, Visits: n})
	}
	sort.Slice(ar.Days, func(i, j int) bool { return ar.Days[i].Date < ar.Days[j].Date })

	res := &Result{
		Widget: req.Widget, Site: req.Site, AIReferrals: ar,
		Total: &PeriodTotal{Events: ar.Pageviews, Visits: uint64(ar.Visits)},
	}
	for name, s := range sources {
		res.Rows = append(res.Rows, Row{
			Label: name, Events: s.views, Visitors: uint64(len(s.visitors)), Visits: uint64(len(s.sessions)),
		})
	}
	sort.Slice(res.Rows, func(i, j int) bool {
		if res.Rows[i].Visits != res.Rows[j].Visits {
			return res.Rows[i].Visits > res.Rows[j].Visits
		}
		return res.Rows[i].Label < res.Rows[j].Label
	})

	aud, err := e.audience(ctx, req)
	if err != nil {
		return nil, err
	}
	res.Audience = aud
	return res, nil
}
