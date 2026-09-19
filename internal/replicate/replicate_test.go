package replicate

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/objstore"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// writeParquet drops a placeholder file into a tier partition. The replicator never looks
// inside these, so their bytes are irrelevant and their PATHS are the whole point.
func writeParquet(t *testing.T, dir, tier, site, date, name string) string {
	t.Helper()
	p := filepath.Join(dir, tier, site, date, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("parquet-bytes"), 0o640); err != nil {
		t.Fatal(err)
	}
	return p
}

func newRep(t *testing.T, dir string, store objstore.Store, opts ...func(*Options)) *Replicator {
	t.Helper()
	o := Options{Store: store, DataDir: dir, NodeID: "n1", Log: quiet()}
	for _, f := range opts {
		f(&o)
	}
	return New(o)
}

func TestUploadsBothTiers(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()

	writeParquet(t, dir, "data", "s.example", "2026-09-19", "n1-000000000001.parquet")
	writeParquet(t, dir, "rollup", "s.example", "2026-09-19", "n1-000000000001.parquet")

	newRep(t, dir, mem).RunOnce(context.Background())

	want := []string{
		"data/s.example/2026-09-19/n1-000000000001.parquet",
		"rollup/s.example/2026-09-19/n1-000000000001.parquet",
	}
	if got := mem.Keys(); !slices.Equal(got, want) {
		t.Errorf("uploaded %v, want %v", got, want)
	}
}

// Parquet keys carry no node element in their PATH — only in the filename — which is what lets
// several nodes write into one shared tree with no coordination. If this changes, the "add a
// node and it just reads history" property goes with it.
func TestParquetKeysAreNodeIndependent(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	writeParquet(t, dir, "data", "s.example", "2026-09-19", "n7-000000000042.parquet")

	newRep(t, dir, mem).RunOnce(context.Background())

	for _, k := range mem.Keys() {
		if strings.HasPrefix(k, "data/n7/") || strings.HasPrefix(k, "data/n1/") {
			t.Errorf("key %q is namespaced by node; Parquet must be shared across nodes", k)
		}
	}
}

func TestUploadIsSkippedSecondTime(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	writeParquet(t, dir, "data", "s.example", "2026-09-19", "a.parquet")

	var uploads int
	r := newRep(t, dir, mem, func(o *Options) {
		o.OnUpload = func(string, int64) { uploads++ }
	})

	r.RunOnce(context.Background())
	r.RunOnce(context.Background())

	if uploads != 1 {
		t.Errorf("uploaded %d times, want 1 — an unchanged file must not be re-sent every cycle", uploads)
	}
}

// The active WAL segment is still being appended to; shipping it would send a prefix and
// immediately need to send it again.
func TestOnlyClosedWALSegmentsAreUploaded(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()

	w, err := wal.Open(wal.Options{
		Dir: filepath.Join(dir, "wal"), SegmentBytes: 64,
		SyncInterval: 5 * time.Millisecond, Log: quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := range 20 {
		if err := w.Append(context.Background(), []byte(strings.Repeat("x", 20)+string(rune('a'+i%26)))); err != nil {
			t.Fatal(err)
		}
	}

	closed, _ := w.ClosedSegments()
	if len(closed) == 0 {
		t.Fatal("test setup produced no closed segments")
	}

	newRep(t, dir, mem, func(o *Options) { o.WAL = w }).RunOnce(context.Background())

	keys := mem.Keys()
	if len(keys) != len(closed) {
		t.Errorf("uploaded %d segments, want %d (the closed ones only): %v", len(keys), len(closed), keys)
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "wal/n1/") {
			t.Errorf("WAL key %q should be namespaced by node: segments are node-private until compacted", k)
		}
	}
}

// A remote WAL copy exists to cover the window before compaction. Once the local segment has
// gone — which the compactor only does after Parquet is durable — the copy protects nothing.
func TestRemoteWALCopyIsDroppedOnceCompacted(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		t.Fatal(err)
	}

	seg := filepath.Join(walDir, "000000000001.wal")
	if err := os.WriteFile(seg, []byte("segment"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(context.Background(), "wal/n1/000000000001.wal", strings.NewReader("segment"), 7); err != nil {
		t.Fatal(err)
	}

	r := newRep(t, dir, mem)

	// Still present locally: the copy must stay.
	r.RunOnce(context.Background())
	if len(mem.Keys()) != 1 {
		t.Fatalf("the remote copy was dropped while the segment still existed locally: %v", mem.Keys())
	}

	// The compactor removes it, having written Parquet.
	if err := os.Remove(seg); err != nil {
		t.Fatal(err)
	}
	r.RunOnce(context.Background())

	if len(mem.Keys()) != 0 {
		t.Errorf("the remote copy survived compaction: %v", mem.Keys())
	}
}

// The safety property that matters most: a bucket that is unreachable must cost disk, never
// data. Nothing local may be deleted on the strength of an upload that did not happen.
func TestFailedUploadNeverPrunesLocalData(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	mem.FailPut = errors.New("bucket unreachable")

	old := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")
	p := writeParquet(t, dir, "data", "s.example", old, "a.parquet")

	newRep(t, dir, mem, func(o *Options) {
		o.LocalRetention = time.Hour // long past, so retention alone would delete it
	}).RunOnce(context.Background())

	if _, err := os.Stat(p); err != nil {
		t.Errorf("local Parquet was pruned after a FAILED upload: %v", err)
	}
}

func TestLocalPruneAfterSuccessfulUpload(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()

	old := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")
	recent := time.Now().UTC().Format("2006-01-02")
	oldFile := writeParquet(t, dir, "data", "s.example", old, "a.parquet")
	newFile := writeParquet(t, dir, "data", "s.example", recent, "b.parquet")

	newRep(t, dir, mem, func(o *Options) {
		o.LocalRetention = 7 * 24 * time.Hour
	}).RunOnce(context.Background())

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Errorf("old local Parquet survived the retention window")
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Errorf("recent local Parquet was pruned: %v", err)
	}
	// Pruning local must never remove the remote copy: that is the whole point of the tier.
	if len(mem.Keys()) != 2 {
		t.Errorf("remote objects = %v, want both still present", mem.Keys())
	}
}

// Remote retention dates from the KEY, not the object's modification time. A file rewritten by
// a re-run of compaction would otherwise look young and outlive its cohort, leaving a
// partition with holes in it.
func TestRemoteRetentionUsesThePartitionDateNotMtime(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	ctx := context.Background()

	old := time.Now().UTC().AddDate(0, 0, -400).Format("2006-01-02")
	recent := time.Now().UTC().Format("2006-01-02")

	// Both written to the bucket just now, so their mtimes are identical and only the key
	// distinguishes them.
	mem.Put(ctx, "data/s.example/"+old+"/a.parquet", strings.NewReader("x"), 1)
	mem.Put(ctx, "data/s.example/"+recent+"/b.parquet", strings.NewReader("x"), 1)

	newRep(t, dir, mem, func(o *Options) {
		o.RemoteRetention = 90 * 24 * time.Hour
	}).RunOnce(ctx)

	keys := mem.Keys()
	if len(keys) != 1 || !strings.Contains(keys[0], recent) {
		t.Errorf("remote keys after retention = %v, want only the recent partition", keys)
	}
}

// Zero retention means keep forever. Deleting a customer's history because a variable was
// unset is not a failure anyone recovers from.
func TestZeroRetentionKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	mem := objstore.NewMemory()
	ctx := context.Background()

	ancient := time.Now().UTC().AddDate(-5, 0, 0).Format("2006-01-02")
	local := writeParquet(t, dir, "data", "s.example", ancient, "a.parquet")
	mem.Put(ctx, "data/s.example/"+ancient+"/remote.parquet", strings.NewReader("x"), 1)

	newRep(t, dir, mem).RunOnce(ctx) // no retention configured at all

	if _, err := os.Stat(local); err != nil {
		t.Errorf("local file deleted with no retention configured: %v", err)
	}
	if len(mem.Keys()) != 2 {
		t.Errorf("remote objects = %v, want both kept", mem.Keys())
	}
}

func TestDateOf(t *testing.T) {
	for _, tc := range []struct {
		rel  string
		ok   bool
		want string
	}{
		{"s.example/2026-09-19/a.parquet", true, "2026-09-19"},
		{"a.b.c/2026-01-01/n1-000000000001.parquet", true, "2026-01-01"},
		{"nodate/a.parquet", false, ""},
		{"a.parquet", false, ""},
		{"s.example/not-a-date/a.parquet", false, ""},
	} {
		got, ok := dateOf(tc.rel)
		if ok != tc.ok {
			t.Errorf("dateOf(%q) ok = %v, want %v", tc.rel, ok, tc.ok)
			continue
		}
		if ok && got.Format("2006-01-02") != tc.want {
			t.Errorf("dateOf(%q) = %s, want %s", tc.rel, got.Format("2006-01-02"), tc.want)
		}
	}
}
