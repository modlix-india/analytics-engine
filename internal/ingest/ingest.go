// Package ingest accepts events over HTTP, enriches them, and hands them to the WAL.
//
// The endpoint is public, unauthenticated and write-only, which sets the tone for everything
// here: every bound is a defence rather than a tuning knob, every parse assumes hostile input,
// and the handler answers the same way whether an event was stored or deliberately discarded.
package ingest

import (
	"compress/gzip"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
)

// NameClick is a click with a position: the heatmap's raw material.
//
// Distinct from the labelled "click" that autocapture records, and deliberately so. A labelled
// click is a decision somebody made about what matters, and there are few of them; this one
// fires for every click anywhere on the page and exists only to be drawn. Keeping them apart
// means turning heatmaps on cannot change what the funnel and the event list say.
const NameClick = "$click"

// clampCoord bounds a client-supplied coordinate. Negative becomes zero: a click above the
// document is not a thing, and a negative here would be a blob drawn off the top of a canvas.
func clampCoord(v, max int32) int32 {
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}

// canonicalEventName folds the spellings of a page view onto the one name the engine counts.
//
// Every traffic widget filters on event.NamePageview. An event stored as "pageview" is
// therefore not slightly wrong, it is invisible: the file holds it, a raw scan finds it, and
// the dashboard answers zero rows with no error. The first request this engine ever received
// that was not written by its own tests hit exactly that, and nothing in the system said so —
// every fixture in the repo happened to spell it the canonical way.
//
// Deliberately one concept and a closed set of spellings. Custom event names are the caller's
// own vocabulary and are stored verbatim; folding those would be a rename nobody asked for.
func canonicalEventName(name string) string {
	switch name {
	case "pageview", "page_view", "page-view", "pageView", "PageView":
		return event.NamePageview
	}
	return name
}

// Sink is what ingest writes to. The WAL satisfies it.
//
// Deliberately Enqueue and not Append: see the durability note in the wal package. Nothing
// retries a pageview, so making the browser wait for an fsync costs latency and buys no
// safety.
type Sink interface {
	Enqueue(payload []byte) error
}

// batchRequest is the wire format.
//
// Short keys because this is sent from every page view of every site, and the beacon's size is
// paid by the visitor. The per-request fields are the context — URL, referrer, identity — and
// only what genuinely varies per event repeats inside the batch.
//
// Unlike the internal packed encoding, this is a PUBLIC contract: once an SDK is deployed in
// customer pages, old clients keep sending old shapes for a long time. Fields may be added;
// nothing may be renamed or repurposed.
type batchRequest struct {
	URL      string      `json:"u"`
	Referrer string      `json:"r"`
	Visitor  string      `json:"v"`
	Session  string      `json:"s"`
	Exp      string      `json:"x"`
	Variant  string      `json:"n"`
	Batch    []wireEvent `json:"b"`
}

type wireEvent struct {
	Name  string          `json:"e"`
	TS    int64           `json:"t"`
	Label string          `json:"l"`
	Page  string          `json:"g"`
	URL   string          `json:"u"` // overrides the batch URL, for a SPA that navigated
	Props json.RawMessage `json:"p"`

	// Where a click landed. X is ten-thousandths of the viewport width, Y absolute pixels
	// down the document, W the viewport width in css pixels. Only a click carries them.
	X int32 `json:"x"`
	Y int32 `json:"y"`
	W int32 `json:"w"`

	// How far down the page a view got: D percent of the document's height, H the window's
	// height and DH the document's, both in css pixels. Only a $scroll report carries them,
	// and it carries W as well so a depth can be banded by layout the way a click is.
	D  int32 `json:"d"`
	H  int32 `json:"h"`
	DH int32 `json:"dh"`

	// 1 when a click landed on something interactive, 0 when it did not. One bit, and
	// deliberately one bit: a boolean cannot carry a person's own data into an event.
	I int32 `json:"i"`
}

// NameScroll is one view's maximum scroll depth, reported when the view ends.
//
// Separate from pageleave, which a site can switch off independently, and separate from the
// page view itself, which is counted whether or not anyone scrolled. One report per view
// rather than an event per threshold crossed: five times fewer events, and the thresholds stay
// a reading decision rather than being fixed at whatever was chosen on the day.
const NameScroll = "$scroll"

type Options struct {
	Sink     Sink
	Resolver SiteResolver
	Log      *slog.Logger

	MaxBatchEvents int
	MaxBodyBytes   int64

	// VisitorSecret makes the daily visitor salt survive a restart. See rotatingSalt.
	//
	// Empty keeps the old behaviour — a fresh random salt per process — which is correct for
	// a standalone deployment that has nowhere to keep a secret, and wrong for a fleet.
	VisitorSecret string

	// OnEvent reports the outcome of every event, for metrics. An engine that silently
	// discards is indistinguishable from one that is working, so this is not optional
	// instrumentation.
	OnEvent func(outcome string)

	// Now is injectable so tests do not have to sleep.
	Now func() time.Time
}

type Ingester struct {
	opt  Options
	log  *slog.Logger
	now  func() time.Time
	salt *rotatingSalt
}

func New(o Options) *Ingester {
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
	return &Ingester{opt: o, log: o.Log, now: o.Now, salt: newRotatingSalt(o.Now, o.VisitorSecret)}
}

// Handle serves POST /i.
//
// On the status codes: 4xx means the request itself was malformed and an SDK author needs to
// know. 204 means the request was fine and the events were either stored or deliberately
// ignored — an unknown site, a bot — and the caller is told nothing about which. That split
// keeps the endpoint debuggable without letting anyone probe which sites exist.
func (i *Ingester) Handle(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, i.opt.MaxBodyBytes)
	defer body.Close()

	var reader io.Reader = body
	if strings.Contains(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(body)
		if err != nil {
			http.Error(w, "bad gzip", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		reader = gz
	}

	var req batchRequest
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if len(req.Batch) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(req.Batch) > i.opt.MaxBatchEvents {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}

	i.process(r, &req)
	w.WriteHeader(http.StatusNoContent)
}

func (i *Ingester) process(r *http.Request, req *batchRequest) {
	ua := r.Header.Get("User-Agent")

	// Parsed once per request, not once per event: a batch shares one client, and this is the
	// most expensive step in the path.
	info := ParseUA(ua)
	if info.Bot {
		i.count("dropped_bot", len(req.Batch))
		return
	}

	res, ok := i.opt.Resolver.Resolve(r.Context(), Signals{
		UserAgent: ua,
		PageURL:   req.URL,
		Origin:    r.Header.Get("Origin"),
		Referer:   r.Header.Get("Referer"),
	})
	if !ok {
		i.count("dropped_unresolved", len(req.Batch))
		return
	}

	// A resolver that recognises its own client knows better than the generic UA parse.
	platform, appVersion := info.Platform, ""
	if res.Platform != "" {
		platform = res.Platform
	}
	if res.AppVersion != "" {
		appVersion = res.AppVersion
	}

	pageHost, basePath, utm, _ := ParsePageURL(req.URL)
	refHost := ReferrerHost(req.Referrer)
	channel := Channel(refHost, pageHost, utm)

	// Country from the edge. Cloudflare sets this on every request, which is why this engine
	// ships no geo database and needs no licence for one.
	country := r.Header.Get("CF-IPCountry")
	if country == "XX" || country == "T1" {
		// Cloudflare's codes for "unknown" and "Tor". Neither is a country.
		country = ""
	}

	visitor := req.Visitor
	if visitor == "" {
		// No client-side identity — the visitor has not consented to storage, or the SDK is
		// running without it. Derive one instead of dropping the event: a cookieless
		// deployment is a legitimate way to run this, and it is the one that needs no
		// consent banner at all.
		visitor = i.salt.visitorID(clientIP(r), ua, res.Site)
	}

	now := i.now().UnixMilli()

	// Each event in a batch gets its own millisecond, in the order the client sent them.
	//
	// One timestamp for the whole batch would make time a partial order, and everything that
	// depends on event ORDER rather than on counts then becomes ambiguous — a funnel of
	// view → add → buy reported in a single batch would have three identical timestamps, and
	// whether it counts as a conversion would come down to which row a file happened to store
	// first. Spending a millisecond per event makes the order total and reproducible.
	//
	// The distortion is bounded by the batch cap and is typically two or three milliseconds.
	// Client timestamps are kept separately and are not used for this: the client's clock is
	// not trusted, but the order it listed its own events in is the best evidence available.
	var seq int64

	for idx := range req.Batch {
		we := &req.Batch[idx]
		if we.Name == "" {
			i.count("dropped_invalid", 1)
			continue
		}

		path := basePath
		if we.URL != "" {
			// A SPA that navigated mid-batch. Only the path is taken; the campaign context
			// belongs to the landing URL, not to a later route change.
			if _, p, _, ok := ParsePageURL(we.URL); ok {
				path = p
			} else if strings.HasPrefix(we.URL, "/") {
				path = we.URL
			}
		}

		e := event.Event{
			TSServer: now + seq,
			TSClient: we.TS,
			Site:     res.Site,
			Name:     canonicalEventName(we.Name),
			Path:     path,
			Page:     we.Page,
			Label:    we.Label,
			Visitor:  visitor,
			Session:  req.Session,

			ReferrerHost: refHost,
			Channel:      channel,

			UTMSource:   utm.Source,
			UTMMedium:   utm.Medium,
			UTMCampaign: utm.Campaign,
			UTMTerm:     utm.Term,
			UTMContent:  utm.Content,

			Device:     info.Device,
			Browser:    info.Browser,
			OS:         info.OS,
			Platform:   platform,
			AppVersion: appVersion,
			Country:    country,

			Experiment: req.Exp,
			Variant:    req.Variant,

			// Clamped rather than trusted. These come from a public endpoint, and a
			// coordinate outside the viewport is either a bug or someone testing what
			// this accepts; either way it must not reach a renderer that will happily
			// draw a blob a million pixels off the page.
			ClickX:   clampCoord(we.X, 10000),
			ClickY:   clampCoord(we.Y, 200000),
			Viewport: clampCoord(we.W, 20000),

			// Same treatment as the click coordinates, and for the same reason: these arrive
			// on a public endpoint and end up on an axis. A depth of 4000% is either a bug or
			// somebody testing what this accepts, and either way it must not reach a chart.
			Interactive: clampCoord(we.I, 1),

			ScrollPct: clampCoord(we.D, 100),
			ViewportH: clampCoord(we.H, 20000),
			DocH:      clampCoord(we.DH, 2000000),

			Props: string(we.Props),
		}

		seq++

		if err := i.opt.Sink.Enqueue(event.Encode(nil, &e)); err != nil {
			// The disk is refusing writes. Readiness reports it; here it is one counter and
			// no log line per event, because at this point there would be a great many.
			i.count("dropped_sink", 1)
			continue
		}
		i.count("accepted", 1)
	}
}

func (i *Ingester) count(outcome string, n int) {
	if i.opt.OnEvent == nil {
		return
	}
	for range n {
		i.opt.OnEvent(outcome)
	}
}

// rotatingSalt derives a stable-for-today, unrecoverable-tomorrow visitor identifier.
//
// The salt rotates daily and is never written to disk. That is the whole privacy property:
// within a day the same visitor hashes to the same id so sessions and uniques work, and once
// the salt rotates the old ids cannot be linked to anyone — not by us, not by anyone who later
// obtains the data.
//
// # Why the day's salt is derived rather than drawn fresh
//
// It used to be 32 random bytes generated the first time a day was seen, which meant the salt
// changed on every restart as well as at midnight. A deploy at noon therefore split that day's
// visitors into two populations: the same person counted twice, a funnel broken across the
// restart, and nothing anywhere saying so. Deriving the day's salt as HMAC(secret, date) keeps
// the rotation and removes the restart artefact, at the cost of one secret that has to outlive
// the process.
//
// With no secret configured it falls back to the old behaviour, which is right for a
// standalone deployment: a node with nowhere to keep a secret should not invent a stable one,
// and restart splitting is a smaller problem than a predictable salt would be.
//
// A fleet must share the secret. Two nodes with different secrets hash the same visitor
// differently, so a site served by both counts everyone twice — the restart problem again,
// permanently.
type rotatingSalt struct {
	now    func() time.Time
	secret []byte

	mu      sync.Mutex
	day     string
	current []byte
}

func newRotatingSalt(now func() time.Time, secret string) *rotatingSalt {
	return &rotatingSalt{now: now, secret: []byte(secret)}
}

func (s *rotatingSalt) visitorID(ip, ua, site string) string {
	salt := s.today()

	h := sha256.New()
	h.Write(salt)
	// Length-prefixed so that ("ab","c") and ("a","bc") cannot hash alike. Cheap, and the
	// alternative is a collision nobody would ever find by testing.
	for _, part := range []string{ip, ua, site} {
		var l [8]byte
		for i := range 8 {
			l[i] = byte(len(part) >> (8 * i))
		}
		h.Write(l[:])
		h.Write([]byte(part))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:16])
}

// derive produces the salt for one day: from the configured secret where there is one, and
// from crypto/rand where there is not.
func (s *rotatingSalt) derive(day string) []byte {
	if len(s.secret) > 0 {
		h := hmac.New(sha256.New, s.secret)
		h.Write([]byte(day))
		return h.Sum(nil)
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is not survivable for this purpose: a predictable salt would
		// make every visitor id reversible by anyone who guessed it.
		panic("ingest: crypto/rand unavailable: " + err.Error())
	}
	return buf
}

func (s *rotatingSalt) today() []byte {
	day := s.now().UTC().Format("2006-01-02")

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.day != day || s.current == nil {
		s.day, s.current = day, s.derive(day)
	}
	return s.current
}

// clientIP prefers the edge's view of the caller.
//
// Only used as one input to a hash that is thrown away daily, never stored or logged, which is
// why trusting a header here is acceptable where it would not be for access control: the worst
// a forged value does is split one visitor into two.
func clientIP(r *http.Request) string {
	if v := r.Header.Get("CF-Connecting-IP"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			return strings.TrimSpace(v[:i])
		}
		return strings.TrimSpace(v)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
