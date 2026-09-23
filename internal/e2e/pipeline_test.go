// Package e2e drives one event through every layer in the order production does: an HTTP
// request to the ingest handler, the WAL, the compactor, Parquet, the rollups, and back out
// through the query handler.
//
// It exists because every other test in this repo builds its own input. Each layer was
// correct in isolation and the chain was not: the fixtures all spelled a page view
// "$pageview", the first hand-written curl said "pageview", ingest stored that verbatim, and
// every traffic widget — which filters on "$pageview" — answered zero rows with no error for
// data sitting correctly in the file. A test that starts at the wire is the only kind that
// can see that, because it is the only one that does not get to choose the spelling.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/compact"
	"github.com/modlix-india/analytics-engine/internal/ingest"
	"github.com/modlix-india/analytics-engine/internal/query"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

const (
	site   = "shop.example"
	origin = "https://" + site
	secret = "test-secret"
	ua     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stack is the whole engine, minus the process.
func stack(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()

	w, err := wal.Open(wal.Options{
		Dir: filepath.Join(dir, "wal"),
		// One event per segment, so the compactor has closed segments to work on without
		// the test waiting out a rotation interval.
		SegmentBytes: 1,
		SyncInterval: 5 * time.Millisecond,
		Log:          quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

	ing := ingest.New(ingest.Options{
		Sink: w, Resolver: ingest.HostResolver{}, Log: quiet(),
		MaxBatchEvents: 100, MaxBodyBytes: 1 << 20,
	})

	c := compact.New(compact.Options{
		WAL: w, DataDir: dir, NodeID: "e2e",
		Interval: 20 * time.Millisecond, Log: quiet(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	mux := http.NewServeMux()
	mux.HandleFunc("POST /i", ing.Handle)
	mux.Handle("POST /q", &query.Handler{
		Engine: &query.Engine{DataDir: dir, Log: quiet()},
		Auth:   query.SharedSecret{Secret: secret},
		Log:    quiet(),
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, dir
}

func send(t *testing.T, srv *httptest.Server, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/i", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", origin)
	req.Header.Set("User-Agent", ua)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("ingest answered %d, want 204", res.StatusCode)
	}
}

type result struct {
	Rows []struct {
		Label    string `json:"label"`
		Events   int64  `json:"events"`
		Visitors int64  `json:"visitors"`
	} `json:"rows"`
}

func ask(t *testing.T, srv *httptest.Server, body string) result {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/q", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+secret)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("query answered %d: %s", res.StatusCode, b)
	}

	var out result
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// waitForParquet blocks until the compactor has written something, so the test asserts on a
// query that had data to answer from rather than on a race it happened to lose.
func waitForParquet(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		filepath.WalkDir(filepath.Join(dir, "rollup"), func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && filepath.Ext(p) == ".parquet" {
				found = true
			}
			return nil
		})
		if found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the compactor wrote no rollup within 10s")
}

func window() (string, string) {
	now := time.Now().UTC()
	return now.Add(-2 * time.Hour).Format(time.RFC3339), now.Add(2 * time.Hour).Format(time.RFC3339)
}

// A page view sent the way a client writes it must be counted by the widget that counts page
// views. The spelling the caller used is the caller's business; being invisible is not an
// acceptable answer to it.
func TestPageviewFromTheWireReachesTheWidget(t *testing.T) {
	for _, name := range []string{"pageview", "$pageview", "page_view"} {
		t.Run(name, func(t *testing.T) {
			srv, dir := stack(t)
			send(t, srv, fmt.Sprintf(
				`{"u":"https://shop.example/pricing","r":"https://www.google.com/","b":[{"e":%q}]}`, name))
			waitForParquet(t, dir)

			from, to := window()
			got := ask(t, srv, fmt.Sprintf(
				`{"widget":"topPages","site":%q,"from":%q,"to":%q,"timezone":"UTC","limit":10}`,
				site, from, to))

			if len(got.Rows) != 1 {
				t.Fatalf("topPages returned %d rows, want 1: %+v", len(got.Rows), got.Rows)
			}
			if got.Rows[0].Label != "/pricing" || got.Rows[0].Events != 1 {
				t.Fatalf("got %+v, want /pricing with 1 event", got.Rows[0])
			}
		})
	}
}

// The other half of the same rule: a custom event name is the caller's vocabulary and must
// survive untouched. Folding page-view spellings must not become a licence to rewrite names.
func TestCustomEventNamesAreStoredVerbatim(t *testing.T) {
	srv, dir := stack(t)
	send(t, srv, `{"u":"https://shop.example/pricing","b":[{"e":"pageview"},{"e":"cta_clicked"}]}`)
	waitForParquet(t, dir)

	from, to := window()
	got := ask(t, srv, fmt.Sprintf(
		`{"widget":"topEvents","site":%q,"from":%q,"to":%q,"timezone":"UTC","limit":10}`,
		site, from, to))

	names := map[string]int64{}
	for _, r := range got.Rows {
		names[r.Label] = r.Events
	}
	if names["cta_clicked"] != 1 {
		t.Errorf("cta_clicked = %d, want 1 (names must not be rewritten): %+v", names["cta_clicked"], names)
	}
	if names["$pageview"] != 1 {
		t.Errorf("$pageview = %d, want 1 (pageview must be folded onto it): %+v", names["$pageview"], names)
	}
}
