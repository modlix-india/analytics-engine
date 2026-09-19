package compact

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
	"github.com/modlix-india/analytics-engine/internal/store"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// setup writes events into a WAL, forces rotation so they sit in a CLOSED segment, and returns
// a compactor over it.
func setup(t *testing.T, evs []*event.Event) (*Compactor, string, *wal.WAL) {
	t.Helper()
	dir := t.TempDir()

	w, err := wal.Open(wal.Options{
		Dir: filepath.Join(dir, "wal"),
		// Tiny, so every write rotates and nothing is left in the active segment.
		SegmentBytes: 1,
		SyncInterval: 5 * time.Millisecond,
		Log:          quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range evs {
		if err := w.Append(context.Background(), event.Encode(nil, e)); err != nil {
			t.Fatal(err)
		}
	}

	c := New(Options{
		WAL: w, DataDir: dir, NodeID: "n1",
		Interval: time.Hour, Log: quiet(),
	})
	return c, dir, w
}

func parquetFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(filepath.Join(dir, "data"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Ext(p) == ".parquet" {
			out = append(out, p)
		}
		return nil
	})
	sort.Strings(out)
	return out
}

func allRows(t *testing.T, dir string) []store.Row {
	t.Helper()
	var out []store.Row
	for _, f := range parquetFiles(t, dir) {
		rows, err := store.ReadFile(f)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", f, err)
		}
		out = append(out, rows...)
	}
	return out
}

func ev(site, name string, ts int64) *event.Event {
	return &event.Event{
		TSServer: ts, TSClient: ts - 100,
		Site: site, Name: name, Path: "/p", Visitor: "v1", Session: "s1",
		Channel: "direct", Browser: "Chrome", OS: "macOS", Device: "desktop",
		Platform: "web", Country: "IN", Props: `{"a":1}`,
	}
}

func TestCompactRoundTrip(t *testing.T) {
	ts := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC).UnixMilli()
	c, dir, w := setup(t, []*event.Event{
		ev("shop.example", "$pageview", ts),
		ev("shop.example", "cta_clicked", ts+1000),
	})
	defer w.Close()

	c.runOnce()

	rows := allRows(t, dir)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	// Sorted by server time within the file, which is what makes row-group statistics prune.
	if rows[0].TSServer > rows[1].TSServer {
		t.Errorf("rows are not sorted by ts_server")
	}
	if rows[0].Site != "shop.example" || rows[0].Name != "$pageview" {
		t.Errorf("row 0 = %+v", rows[0])
	}
	if rows[0].Props != `{"a":1}` {
		t.Errorf("Props = %q, want the JSON tail preserved through Parquet", rows[0].Props)
	}
	if rows[0].Country != "IN" || rows[0].Browser != "Chrome" {
		t.Errorf("enrichment did not survive: %+v", rows[0])
	}
}

// Every query filters on site and a time range, so the layout has to separate them.
func TestPartitioningBySiteAndDate(t *testing.T) {
	d1 := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC).UnixMilli()
	d2 := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC).UnixMilli()

	c, dir, w := setup(t, []*event.Event{
		ev("a.example", "$pageview", d1),
		ev("a.example", "$pageview", d2),
		ev("b.example", "$pageview", d1),
	})
	defer w.Close()

	c.runOnce()

	files := parquetFiles(t, dir)
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3 (two sites, and one of them across two dates):\n%v", len(files), files)
	}

	var got []string
	for _, f := range files {
		rel, _ := filepath.Rel(filepath.Join(dir, "data"), f)
		got = append(got, filepath.Dir(rel))
	}
	sort.Strings(got)
	want := []string{"a.example/2026-09-19", "a.example/2026-09-20", "b.example/2026-09-19"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("partition %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// The crash-safety property: output names are derived from the input segment, so a compaction
// that is interrupted after writing Parquet but before deleting the segment re-runs to exactly
// the same result. Without this, that window silently duplicates every event in the segment.
func TestCompactionIsIdempotent(t *testing.T) {
	ts := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC).UnixMilli()
	evs := []*event.Event{
		ev("shop.example", "$pageview", ts),
		ev("shop.example", "$pageview", ts+1),
		ev("shop.example", "$pageview", ts+2),
	}

	// First pass: compact, but put the segments back, simulating a crash before the delete.
	c, dir, w := setup(t, evs)
	defer w.Close()

	segs, _ := w.ClosedSegments()
	backup := map[string][]byte{}
	for _, s := range segs {
		b, _ := os.ReadFile(s)
		backup[s] = b
	}

	c.runOnce()
	first := allRows(t, dir)
	firstFiles := parquetFiles(t, dir)

	// The crash: segments are back, as if the delete never happened.
	for path, b := range backup {
		if err := os.WriteFile(path, b, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	c.runOnce()
	second := allRows(t, dir)
	secondFiles := parquetFiles(t, dir)

	if len(second) != len(first) {
		t.Errorf("re-running compaction changed the row count: %d then %d — events were duplicated",
			len(first), len(second))
	}
	if len(secondFiles) != len(firstFiles) {
		t.Errorf("re-running compaction changed the file count: %d then %d", len(firstFiles), len(secondFiles))
	}
}

func TestSegmentsAreRemovedOnlyAfterWriting(t *testing.T) {
	ts := time.Now().UnixMilli()
	c, dir, w := setup(t, []*event.Event{ev("s.example", "$pageview", ts)})
	defer w.Close()

	before, _ := w.ClosedSegments()
	if len(before) == 0 {
		t.Fatal("test setup produced no closed segments")
	}

	c.runOnce()

	after, _ := w.ClosedSegments()
	if len(after) != 0 {
		t.Errorf("%d segments survived compaction, want 0", len(after))
	}
	if len(parquetFiles(t, dir)) == 0 {
		t.Error("segments were removed but no Parquet was written")
	}
}

// The active segment is still being appended to. Compacting it would race the writer and
// delete data that was never converted.
func TestActiveSegmentIsNeverCompacted(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(wal.Options{
		Dir:          filepath.Join(dir, "wal"),
		SegmentBytes: 1 << 20, // large, so nothing rotates
		SyncInterval: 5 * time.Millisecond,
		Log:          quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(context.Background(), event.Encode(nil, ev("s.example", "$pageview", time.Now().UnixMilli()))); err != nil {
		t.Fatal(err)
	}

	c := New(Options{WAL: w, DataDir: dir, NodeID: "n1", Interval: time.Hour, Log: quiet()})
	c.runOnce()

	if n := len(parquetFiles(t, dir)); n != 0 {
		t.Errorf("compacted %d files from the active segment; it must be left alone", n)
	}
}

// A site comes from a resolver, which is an embedder's code. Treating it as a trusted path
// element is how "../" escapes the data directory.
func TestSiteCannotEscapeDataDir(t *testing.T) {
	ts := time.Now().UnixMilli()
	c, dir, w := setup(t, []*event.Event{ev("../../etc/passwd", "$pageview", ts)})
	defer w.Close()

	c.runOnce()

	// Containment is the property, and it must be checked on the resolved path. Note that a
	// sanitised name may legitimately BEGIN with dots — "../../etc/passwd" becomes
	// ".._.._etc_passwd", a perfectly ordinary directory name containing no traversal,
	// because every separator became an underscore. Testing for a ".." prefix would flag
	// that as an escape when it is exactly the sanitisation working.
	data := filepath.Clean(filepath.Join(dir, "data"))
	for _, f := range parquetFiles(t, dir) {
		if !strings.HasPrefix(filepath.Clean(f), data+string(os.PathSeparator)) {
			t.Errorf("file escaped the data directory: %s", f)
		}
		if strings.Contains(f, string(os.PathSeparator)+".."+string(os.PathSeparator)) {
			t.Errorf("path contains a traversal element: %s", f)
		}
	}
	// And it must still have been stored, under a sanitised name.
	if len(allRows(t, dir)) != 1 {
		t.Error("the event was dropped; it should be stored under a safe path, not discarded")
	}
}

func TestSafeSegment(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"shop.example", "shop.example"},
		{"SHOP.example", "shop.example"},
		{"app_SYSTEM", "app_system"},
		{"../etc", ".._etc"},
		{"..", "_dots"},
		{".", "_dots"},
		{"", "_empty"},
		{"a/b", "a_b"},
		{"a b", "a_b"},
	} {
		if got := store.SafeSegment(tc.in); got != tc.want {
			t.Errorf("SafeSegment(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
