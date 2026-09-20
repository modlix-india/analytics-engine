package modlix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/ingest"
)

// fakeSecurity answers like the real service, and counts how often it was asked.
func fakeSecurity(t *testing.T, hosts map[string][2]string, delay time.Duration) (*Security, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		pair, ok := hosts[r.URL.Query().Get("host")]
		if !ok {
			// What security actually answers for an unknown host: a resolved tuple
			// whose appCode is the sentinel, not an empty body and not a 404.
			pair = [2]string{"SYSTEM", NoApp}
		}
		json.NewEncoder(w).Encode([]string{pair[0], pair[1], "LIVE"})
	}))
	t.Cleanup(srv.Close)

	return NewSecurity(srv.URL, 5*time.Second), &calls
}

func newResolver(t *testing.T, sec *Security) *Resolver {
	t.Helper()
	r := &Resolver{Security: sec, Shared: NewMemoryShared(), Budget: 2 * time.Second}
	r.Start(context.Background())
	return r
}

func TestUserAgentTagNeedsNoLookup(t *testing.T) {
	sec, calls := fakeSecurity(t, nil, 0)
	r := newResolver(t, sec)

	got, ok := r.Resolve(context.Background(), ingest.Signals{
		UserAgent: "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 ModlixApp/1.4.2 SYSTEM/monkbars",
		PageURL:   "https://apps.dev.modlix.com/monkbars/SYSTEM/page/home",
	})

	if !ok {
		t.Fatal("a tagged mobile request was discarded")
	}
	if got.Site != "monkbars_SYSTEM" {
		t.Errorf("site = %q, want monkbars_SYSTEM", got.Site)
	}
	if got.Platform != "mobile_app" || got.AppVersion != "1.4.2" {
		t.Errorf("platform/version = %q/%q, want mobile_app/1.4.2", got.Platform, got.AppVersion)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("the app named its own site and we still called security %d times", n)
	}
}

// Older generated apps predate the tag. Absence must fall through, not discard.
func TestUntaggedAppFallsThroughToTheURL(t *testing.T) {
	sec, _ := fakeSecurity(t, nil, 0)
	r := newResolver(t, sec)

	got, ok := r.Resolve(context.Background(), ingest.Signals{
		UserAgent: "Mozilla/5.0 (Linux; Android 11; wv) AppleWebKit/537.36",
		PageURL:   "https://apps.dev.modlix.com/monkbars/SYSTEM/page/home",
		Origin:    "https://apps.dev.modlix.com",
	})
	if !ok || got.Site != "monkbars_SYSTEM" {
		t.Fatalf("got %q, %v; want monkbars_SYSTEM resolved from the path", got.Site, ok)
	}
}

// The shared-host form, which is the default for any app without a custom domain.
func TestPathPrefixResolvesWithoutALookup(t *testing.T) {
	sec, calls := fakeSecurity(t, nil, 0)
	r := newResolver(t, sec)

	for _, u := range []string{
		"https://apps.dev.modlix.com/monkbars/SYSTEM/page/home",
		"https://apps.dev.modlix.com/monkbars/SYSTEM/manifest/x",
	} {
		got, ok := r.Resolve(context.Background(), ingest.Signals{
			PageURL: u, Origin: "https://apps.dev.modlix.com",
		})
		if !ok || got.Site != "monkbars_SYSTEM" {
			t.Errorf("%s -> %q, %v; want monkbars_SYSTEM", u, got.Site, ok)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("the path named the site and we still called security %d times", n)
	}
}

// The page URL is payload. Without a browser-set Origin agreeing with it, a script could
// file its events under any app it named.
func TestPathIsNotTrustedAgainstADifferentOrigin(t *testing.T) {
	sec, _ := fakeSecurity(t, map[string][2]string{
		"attacker.example": {"ATK", "attackerapp"},
	}, 0)
	r := newResolver(t, sec)

	got, ok := r.Resolve(context.Background(), ingest.Signals{
		PageURL: "https://apps.dev.modlix.com/victimapp/VICTIM/page/home",
		Origin:  "https://attacker.example",
	})

	if ok && got.Site == "victimapp_VICTIM" {
		t.Fatal("a page claimed another tenant's site and was believed")
	}
	// It still resolves — as the attacker's own site, from their own host. That is the
	// right outcome: they may write into their own numbers all they like.
	if !ok || got.Site != "attackerapp_ATK" {
		t.Fatalf("got %q, %v; want the origin's own site", got.Site, ok)
	}
}

func TestHostLookupIsCachedAndDeduplicated(t *testing.T) {
	sec, calls := fakeSecurity(t, map[string][2]string{
		"theorempro.in": {"THRM", "theorem"},
	}, 0)
	r := newResolver(t, sec)

	for i := 0; i < 5; i++ {
		got, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://theorempro.in"})
		if !ok || got.Site != "theorem_THRM" {
			t.Fatalf("got %q, %v; want theorem_THRM", got.Site, ok)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("security was called %d times for five events on one host, want 1", n)
	}
}

// An unknown host is the common case on a public endpoint. It must be cheap, and it must
// stay cheap: a scanner that drives one security call per request is a free amplifier.
func TestUnknownHostIsDiscardedAndNegativelyCached(t *testing.T) {
	sec, calls := fakeSecurity(t, nil, 0)
	r := newResolver(t, sec)

	for i := 0; i < 4; i++ {
		if _, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://scanner.example"}); ok {
			t.Fatal("an unrecognised host resolved to a site")
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("security was called %d times for four events on an unknown host, want 1", n)
	}
}

// The ingest path must never wait on a dependency. A slow security service costs events, not
// throughput — and the answer it eventually gives is still cached for the next one.
func TestASlowLookupDropsRatherThanStalls(t *testing.T) {
	sec, _ := fakeSecurity(t, map[string][2]string{
		"slow.example": {"SLW", "slowapp"},
	}, 300*time.Millisecond)

	r := &Resolver{Security: sec, Shared: NewMemoryShared(), Budget: 20 * time.Millisecond}
	r.Start(context.Background())

	start := time.Now()
	if _, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://slow.example"}); ok {
		t.Fatal("resolved despite the lookup not having finished")
	}
	if waited := time.Since(start); waited > 150*time.Millisecond {
		t.Fatalf("Resolve blocked for %v, well past its %v budget", waited, 20*time.Millisecond)
	}

	// The detached fetch finishes and fills the cache, so the next event does not pay again.
	time.Sleep(500 * time.Millisecond)
	got, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://slow.example"})
	if !ok || got.Site != "slowapp_SLW" {
		t.Fatalf("got %q, %v after the lookup completed; want slowapp_SLW", got.Site, ok)
	}
}

// Security being unreachable says nothing about the host. Caching that as "unknown" would
// keep a real customer's events in the bin long after the outage ended.
func TestALookupErrorIsNotCached(t *testing.T) {
	var calls atomic.Int64
	var fail atomic.Bool
	fail.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode([]string{"THRM", "theorem", "LIVE"})
	}))
	defer srv.Close()

	r := &Resolver{Security: NewSecurity(srv.URL, time.Second), Shared: NewMemoryShared(), Budget: time.Second}
	r.Start(context.Background())

	if _, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://theorempro.in"}); ok {
		t.Fatal("resolved while security was failing")
	}

	fail.Store(false)
	got, ok := r.Resolve(context.Background(), ingest.Signals{Origin: "https://theorempro.in"})
	if !ok || got.Site != "theorem_THRM" {
		t.Fatalf("got %q, %v once security recovered; want theorem_THRM", got.Site, ok)
	}
}

// One site, one spelling. Two would split every number a customer looks at.
func TestSiteKeyIsCanonical(t *testing.T) {
	cases := [][3]string{
		{"SYSTEM", "monkbars", "monkbars_SYSTEM"},
		{"system", "MonkBars", "monkbars_SYSTEM"},
		{"THRM", "theorem", "theorem_THRM"},
	}
	for _, c := range cases {
		if got := SiteKey(c[0], c[1]); got != c[2] {
			t.Errorf("SiteKey(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}

func TestNoHostIsDiscarded(t *testing.T) {
	sec, _ := fakeSecurity(t, nil, 0)
	r := newResolver(t, sec)

	if _, ok := r.Resolve(context.Background(), ingest.Signals{UserAgent: "curl/8.0"}); ok {
		t.Fatal("a request with no host at all resolved to a site")
	}
}
