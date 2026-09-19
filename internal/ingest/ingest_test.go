package ingest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
)

type memSink struct {
	mu   sync.Mutex
	recs [][]byte
	err  error
}

func (s *memSink) Enqueue(p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.recs = append(s.recs, append([]byte(nil), p...))
	return nil
}

func (s *memSink) events(t *testing.T) []*event.Event {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*event.Event
	for _, r := range s.recs {
		e, err := event.Decode(r)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		out = append(out, e)
	}
	return out
}

const chromeMac = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

func newTestIngester(sink Sink, outcomes *[]string, mu *sync.Mutex) *Ingester {
	return New(Options{
		Sink:     sink,
		Resolver: HostResolver{},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEvent: func(o string) {
			if outcomes == nil {
				return
			}
			mu.Lock()
			*outcomes = append(*outcomes, o)
			mu.Unlock()
		},
	})
}

func post(t *testing.T, i *Ingester, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/i", strings.NewReader(body))
	r.Header.Set("User-Agent", chromeMac)
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	i.Handle(w, r)
	return w
}

func TestHappyPath(t *testing.T) {
	sink := &memSink{}
	i := newTestIngester(sink, nil, nil)

	body := `{
	  "u":"https://shop.example/pricing?utm_source=google&utm_medium=cpc&utm_campaign=spring",
	  "r":"https://google.com/search",
	  "v":"visitor-1","s":"session-1","x":"checkout_test","n":"variant_b",
	  "b":[
	    {"e":"$pageview","t":1758300000000},
	    {"e":"cta_clicked","t":1758300001000,"l":"hero_cta","g":"pricing","p":{"plan":"pro"}}
	  ]}`

	w := post(t, i, body, map[string]string{"CF-IPCountry": "IN"})
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}

	evs := sink.events(t)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}

	e := evs[0]
	checks := map[string]struct{ got, want string }{
		"Site":        {e.Site, "shop.example"},
		"Name":        {e.Name, "$pageview"},
		"Path":        {e.Path, "/pricing"},
		"Visitor":     {e.Visitor, "visitor-1"},
		"Session":     {e.Session, "session-1"},
		"Referrer":    {e.ReferrerHost, "google.com"},
		"Channel":     {e.Channel, "paid"}, // utm_medium=cpc must beat the google.com referrer
		"UTMSource":   {e.UTMSource, "google"},
		"UTMCampaign": {e.UTMCampaign, "spring"},
		"Browser":     {e.Browser, "Chrome"},
		"OS":          {e.OS, "macOS"},
		"Device":      {e.Device, "desktop"},
		"Platform":    {e.Platform, "web"},
		"Country":     {e.Country, "IN"},
		"Experiment":  {e.Experiment, "checkout_test"},
		"Variant":     {e.Variant, "variant_b"},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}

	if evs[1].Label != "hero_cta" || evs[1].Page != "pricing" {
		t.Errorf("second event label/page = %q/%q, want hero_cta/pricing", evs[1].Label, evs[1].Page)
	}
	if evs[1].Props != `{"plan":"pro"}` {
		t.Errorf("Props = %q, want the JSON tail preserved", evs[1].Props)
	}
	// The client's timestamp is kept but never becomes the server's.
	if evs[0].TSClient != 1758300000000 {
		t.Errorf("TSClient = %d, want the value the client sent", evs[0].TSClient)
	}
	if evs[0].TSServer == evs[0].TSClient {
		t.Errorf("TSServer must be our clock, not the client's")
	}
}

func TestBotsAreDroppedWholeBatch(t *testing.T) {
	sink := &memSink{}
	var mu sync.Mutex
	var outcomes []string
	i := newTestIngester(sink, &outcomes, &mu)

	r := httptest.NewRequest(http.MethodPost, "/i",
		strings.NewReader(`{"u":"https://s.example/","b":[{"e":"$pageview"},{"e":"x"}]}`))
	r.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")
	w := httptest.NewRecorder()
	i.Handle(w, r)

	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 — a bot must not learn it was detected", w.Code)
	}
	if len(sink.recs) != 0 {
		t.Errorf("stored %d events from a bot, want 0", len(sink.recs))
	}
	for _, o := range outcomes {
		if o != "dropped_bot" {
			t.Errorf("outcome = %q, want dropped_bot", o)
		}
	}
}

func TestUnresolvedSiteIsDroppedQuietly(t *testing.T) {
	sink := &memSink{}
	var mu sync.Mutex
	var outcomes []string
	// An allow-list that does not contain the sending host.
	i := New(Options{
		Sink:     sink,
		Resolver: HostResolver{Allow: map[string]bool{"known.example": true}},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEvent:  func(o string) { mu.Lock(); outcomes = append(outcomes, o); mu.Unlock() },
	})

	w := post(t, i, `{"u":"https://stranger.example/","b":[{"e":"$pageview"}]}`, nil)

	// 204, not 404: the caller must not be able to probe which sites exist.
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
	if len(sink.recs) != 0 {
		t.Errorf("stored %d events for an unknown site, want 0", len(sink.recs))
	}
	if len(outcomes) != 1 || outcomes[0] != "dropped_unresolved" {
		t.Errorf("outcomes = %v, want [dropped_unresolved]", outcomes)
	}
}

// Malformed input gets a 4xx so an SDK author can debug, because the shape of the request
// reveals nothing about which sites exist. Only semantic discards are silent.
func TestMalformedRequestsAreRejected(t *testing.T) {
	sink := &memSink{}
	i := newTestIngester(sink, nil, nil)

	if w := post(t, i, `not json at all`, nil); w.Code != http.StatusBadRequest {
		t.Errorf("bad json: status = %d, want 400", w.Code)
	}

	big := make([]wireEvent, 600)
	for n := range big {
		big[n] = wireEvent{Name: "e"}
	}
	b, _ := json.Marshal(batchRequest{URL: "https://s.example/", Batch: big})
	if w := post(t, i, string(b), nil); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized batch: status = %d, want 413", w.Code)
	}
	if len(sink.recs) != 0 {
		t.Errorf("an oversized batch stored %d events; the bound must apply before any work", len(sink.recs))
	}
}

func TestBodyBoundIsEnforced(t *testing.T) {
	sink := &memSink{}
	i := New(Options{
		Sink:         sink,
		Resolver:     HostResolver{},
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes: 256,
	})

	huge := `{"u":"https://s.example/","b":[{"e":"` + strings.Repeat("x", 4096) + `"}]}`
	w := post(t, i, huge, nil)
	if w.Code == http.StatusNoContent {
		t.Errorf("an oversized body was accepted; the bound is the first defence on a public endpoint")
	}
}

func TestGzipBody(t *testing.T) {
	sink := &memSink{}
	i := newTestIngester(sink, nil, nil)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte(`{"u":"https://s.example/x","v":"v1","b":[{"e":"$pageview"}]}`))
	gz.Close()

	r := httptest.NewRequest(http.MethodPost, "/i", &buf)
	r.Header.Set("User-Agent", chromeMac)
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	i.Handle(w, r)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if got := len(sink.recs); got != 1 {
		t.Fatalf("stored %d events, want 1", got)
	}
}

// A SPA that navigates mid-batch reports the new path per event, but the campaign context
// belongs to the landing URL and must not follow it.
func TestPerEventURLOverridesPathOnly(t *testing.T) {
	sink := &memSink{}
	i := newTestIngester(sink, nil, nil)

	post(t, i, `{"u":"https://s.example/landing?utm_source=news","v":"v1","b":[
	  {"e":"$pageview"},
	  {"e":"$pageview","u":"/features"}
	]}`, nil)

	evs := sink.events(t)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}
	if evs[0].Path != "/landing" || evs[1].Path != "/features" {
		t.Errorf("paths = %q, %q; want /landing, /features", evs[0].Path, evs[1].Path)
	}
	if evs[1].UTMSource != "news" {
		t.Errorf("campaign context was lost on the second event: UTMSource = %q", evs[1].UTMSource)
	}
}

// Without a client identity we derive one, so a cookieless deployment still counts visitors.
// It must be stable within a day and unrecoverable after the salt rotates.
func TestDerivedVisitorIsStableWithinADayAndRotates(t *testing.T) {
	day := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	sink := &memSink{}
	i := New(Options{
		Sink:     sink,
		Resolver: HostResolver{},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:      func() time.Time { return day },
	})

	send := func() string {
		r := httptest.NewRequest(http.MethodPost, "/i",
			strings.NewReader(`{"u":"https://s.example/","b":[{"e":"$pageview"}]}`))
		r.Header.Set("User-Agent", chromeMac)
		r.Header.Set("CF-Connecting-IP", "203.0.113.7")
		i.Handle(httptest.NewRecorder(), r)
		evs := sink.events(t)
		return evs[len(evs)-1].Visitor
	}

	a, b := send(), send()
	if a == "" {
		t.Fatal("no visitor id was derived")
	}
	if a != b {
		t.Errorf("derived visitor changed within a day: %q then %q", a, b)
	}

	// Move to the next day: the salt rotates and the same visitor must hash differently.
	day = day.Add(24 * time.Hour)
	if c := send(); c == a {
		t.Errorf("derived visitor survived the daily salt rotation; it must not be linkable across days")
	}
}

func TestSinkFailureIsCountedNotPanicked(t *testing.T) {
	sink := &memSink{err: io.ErrClosedPipe}
	var mu sync.Mutex
	var outcomes []string
	i := newTestIngester(sink, &outcomes, &mu)

	w := post(t, i, `{"u":"https://s.example/","b":[{"e":"$pageview"}]}`, nil)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
	if len(outcomes) != 1 || outcomes[0] != "dropped_sink" {
		t.Errorf("outcomes = %v, want [dropped_sink]", outcomes)
	}
}

func TestEventWithoutNameIsSkipped(t *testing.T) {
	sink := &memSink{}
	var mu sync.Mutex
	var outcomes []string
	i := newTestIngester(sink, &outcomes, &mu)

	post(t, i, `{"u":"https://s.example/","b":[{"e":""},{"e":"$pageview"}]}`, nil)

	if len(sink.recs) != 1 {
		t.Errorf("stored %d events, want 1 — the nameless one must be skipped", len(sink.recs))
	}
	if len(outcomes) != 2 || outcomes[0] != "dropped_invalid" || outcomes[1] != "accepted" {
		t.Errorf("outcomes = %v, want [dropped_invalid accepted]", outcomes)
	}
}

// Events in one batch must have distinct, increasing server timestamps.
//
// One timestamp for the whole batch makes time a partial order, and every analysis that
// depends on event ORDER rather than on counts then becomes ambiguous: a funnel of
// view → add → buy reported in a single batch would have three identical timestamps, and
// whether it counted as a conversion would come down to which row a file happened to store
// first. This was found by the DuckDB oracle disagreeing with the engine on a funnel.
func TestBatchEventsGetDistinctIncreasingTimestamps(t *testing.T) {
	sink := &memSink{}
	i := newTestIngester(sink, nil, nil)

	post(t, i, `{"u":"https://s.example/","v":"v1","b":[
	  {"e":"view"},{"e":"add"},{"e":"buy"}
	]}`, nil)

	evs := sink.events(t)
	if len(evs) != 3 {
		t.Fatalf("got %d events, want 3", len(evs))
	}

	for n := 1; n < len(evs); n++ {
		if evs[n].TSServer <= evs[n-1].TSServer {
			t.Errorf("event %d has TSServer %d, not after event %d's %d — batch order must be total",
				n, evs[n].TSServer, n-1, evs[n-1].TSServer)
		}
	}

	// The distortion must stay small: one millisecond per event, not more.
	if spread := evs[2].TSServer - evs[0].TSServer; spread != 2 {
		t.Errorf("a 3-event batch spans %dms, want exactly 2", spread)
	}
}
