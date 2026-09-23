package query

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/objstore"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// uploadAndWipe puts every Parquet file from dir into the store, then removes it locally,
// leaving a node that has the bucket and nothing else — exactly the state of a node added to
// a fleet after the data was written, or one whose local retention has swept.
func uploadAndWipe(t *testing.T, dir string, mem *objstore.Memory) {
	t.Helper()
	ctx := context.Background()

	for _, tier := range []string{"data", "rollup"} {
		root := filepath.Join(dir, tier)
		filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			f, err := os.Open(p)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := mem.Put(ctx, tier+"/"+filepath.ToSlash(rel), f, info.Size()); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
	}
}

// The property the whole object-storage tier exists for: a node can answer for history it
// never ingested and does not hold. Without it, adding a node would mean a backfill, and
// pruning local disk would mean losing the ability to query.
func TestQueryReadsPartitionsItNeverHadLocally(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	rows := threePageRows(base)

	dir := fixture(t, rows)
	mem := objstore.NewMemory()
	uploadAndWipe(t, dir, mem)

	// Nothing local remains.
	if _, err := os.Stat(filepath.Join(dir, "data")); !os.IsNotExist(err) {
		t.Fatal("local data survived the wipe; the test is not testing what it claims")
	}

	e := New(dir)
	e.Remote = mem

	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("query against object storage failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows from the bucket, want 2: %+v", len(res.Rows), res.Rows)
	}
	if res.Rows[0].Label != "/a" || res.Rows[0].Events != 3 {
		t.Errorf("top row = %+v, want /a with 3 events", res.Rows[0])
	}
}

// Fetched objects are cached, so a second query for the same cold partition does not pay for
// the download again.
func TestRemoteFetchesAreCached(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	dir := fixture(t, threePageRows(base))
	mem := objstore.NewMemory()
	uploadAndWipe(t, dir, mem)

	e := New(dir)
	e.Remote = mem
	req := Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(2 * time.Hour),
	}

	if _, err := e.Query(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	cache := filepath.Join(dir, "cache")
	var cached int
	filepath.Walk(cache, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(p) == ".parquet" {
			cached++
		}
		return nil
	})
	if cached == 0 {
		t.Fatal("nothing was cached, so every query would re-download cold partitions")
	}

	// Break the store: a second query must be served entirely from the cache.
	mem.FailPut = errors.New("unused")
	res, err := e.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("second query failed despite a warm cache: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("cached query returned %d rows, want 2", len(res.Rows))
	}
}

// Local and remote copies of the same partition must not be counted twice. The replicator
// uploads before local retention prunes, so both existing at once is the normal state, not an
// edge case.
func TestLocalAndRemoteCopiesAreNotDoubleCounted(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	dir := fixture(t, threePageRows(base))
	mem := objstore.NewMemory()
	ctx := context.Background()

	// Upload, but leave the local files in place.
	for _, tier := range []string{"data", "rollup"} {
		root := filepath.Join(dir, tier)
		filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			f, _ := os.Open(p)
			defer f.Close()
			mem.Put(ctx, tier+"/"+filepath.ToSlash(rel), f, info.Size())
			return nil
		})
	}

	e := New(dir)
	e.Remote = mem

	res, err := e.Query(ctx, Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0].Events != 3 {
		t.Errorf("/a has %d events, want 3 — a partition present both locally and remotely was counted twice",
			res.Rows[0].Events)
	}
}

// A bucket that is briefly unreachable should degrade to whatever is local, not fail the
// query. Partial data with a warning beats none.
func TestRemoteListFailureDegradesToLocal(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	dir := fixture(t, threePageRows(base))

	e := New(dir)
	e.Remote = failingStore{}
	e.Log = slog.New(slog.NewTextHandler(io.Discard, nil))

	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("an unreachable bucket failed the whole query: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("got %d rows from local data, want 2", len(res.Rows))
	}
}

type failingStore struct{}

func (failingStore) Put(context.Context, string, io.Reader, int64) error { return errors.New("down") }
func (failingStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("down")
}
func (failingStore) List(context.Context, string) ([]objstore.Object, error) {
	return nil, errors.New("down")
}
func (failingStore) Delete(context.Context, string) error { return errors.New("down") }

// threePageRows: /a three times, /b twice.
func threePageRows(base time.Time) []store.Row {
	return []store.Row{
		pv(base, "/a", "v1"), pv(base.Add(time.Minute), "/a", "v2"), pv(base.Add(2*time.Minute), "/a", "v3"),
		pv(base.Add(3*time.Minute), "/b", "v4"), pv(base.Add(4*time.Minute), "/b", "v5"),
	}
}
