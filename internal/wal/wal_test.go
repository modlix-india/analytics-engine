package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testWAL(t *testing.T, dir string, sync time.Duration) *WAL {
	t.Helper()
	w, err := Open(Options{
		Dir:          dir,
		SegmentBytes: 1 << 20,
		SegmentAge:   time.Hour,
		SyncInterval: sync,
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	return w
}

func readAll(t *testing.T, dir string) [][]byte {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	var out [][]byte
	for _, s := range segs {
		r, err := OpenReader(s)
		if err != nil {
			t.Fatalf("OpenReader: %v", err)
		}
		for {
			rec, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Next() on %s: %v", s, err)
			}
			out = append(out, rec)
		}
		r.Close()
	}
	return out
}

func TestAppendRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, 10*time.Millisecond)

	want := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for _, p := range want {
		if err := w.Append(context.Background(), p); err != nil {
			t.Fatalf("Append(%q) = %v", p, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	got := readAll(t, dir)
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != string(want[i]) {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// The contract that everything else depends on: a nil return from Append means the bytes are
// on disk, not merely accepted. If this regresses, ingest starts acknowledging events it can
// lose, and nothing else in the system would notice.
func TestAppendReturnsOnlyAfterDurable(t *testing.T) {
	dir := t.TempDir()
	// Long interval so a passing test cannot be an accident of timing.
	w := testWAL(t, dir, 500*time.Millisecond)
	defer w.Close()

	start := time.Now()
	if err := w.Append(context.Background(), []byte("durable")); err != nil {
		t.Fatalf("Append() = %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 100*time.Millisecond {
		t.Errorf("Append returned after %v; it cannot have waited for a 500ms group commit", elapsed)
	}
	// Visible on disk to an independent reader the instant Append returned.
	if got := len(readAll(t, dir)); got != 1 {
		t.Errorf("found %d records on disk after Append returned, want 1", got)
	}
}

func TestUnsyncedReportsLossWindow(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, time.Hour) // never fires during the test
	defer w.Close()

	go w.Append(context.Background(), []byte("a"))
	go w.Append(context.Background(), []byte("b"))

	deadline := time.After(2 * time.Second)
	for w.Unsynced() != 2 {
		select {
		case <-deadline:
			t.Fatalf("Unsynced() = %d, want 2", w.Unsynced())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// A crash between write and fsync leaves a partial record at the end of the last segment.
// That is expected, and recovery must keep everything before it.
func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, 5*time.Millisecond)
	for i := range 5 {
		if err := w.Append(context.Background(), []byte(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs, _ := listSegments(dir)
	last := segs[len(segs)-1]
	st, _ := os.Stat(last)

	// Chop the final record mid-payload, exactly as a power cut would.
	if err := os.Truncate(last, st.Size()-3); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(Options{Dir: dir, SyncInterval: 5 * time.Millisecond, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open() after torn tail = %v; it must recover, not refuse", err)
	}
	defer w2.Close()

	got := readAll(t, dir)
	if len(got) != 4 {
		t.Fatalf("recovered %d records, want the 4 that were intact", len(got))
	}
	// And the log must be usable again, not merely readable.
	if err := w2.Append(context.Background(), []byte("after-recovery")); err != nil {
		t.Fatalf("Append() after recovery = %v", err)
	}
}

// The same damage in a segment that is not the last cannot be a crash: everything after it
// was written later and fsynced since. Starting anyway would serve data with a hole in it.
func TestRecoveryRefusesCorruptionBeforeLastSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{
		Dir:          dir,
		SegmentBytes: 64, // rotate aggressively so the test gets several segments
		SyncInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := w.Append(context.Background(), []byte(fmt.Sprintf("record-number-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs, _ := listSegments(dir)
	if len(segs) < 3 {
		t.Fatalf("need at least 3 segments to test this, got %d", len(segs))
	}

	// Flip a bit in the payload of the first segment.
	victim := segs[0]
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	b[headerSize] ^= 0xFF
	if err := os.WriteFile(victim, b, 0o640); err != nil {
		t.Fatal(err)
	}

	_, err = Open(Options{Dir: dir, SyncInterval: 5 * time.Millisecond, SegmentBytes: 64})
	if err == nil {
		t.Fatal("Open() accepted corruption in a non-final segment; it must refuse to start")
	}
	if !strings.Contains(err.Error(), "refusing to start") {
		t.Errorf("error should say why it refused, got: %v", err)
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 128, SyncInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if err := w.Append(context.Background(), []byte(fmt.Sprintf("payload-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs, _ := listSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("expected rotation, got %d segment(s)", len(segs))
	}
	if got := len(readAll(t, dir)); got != 40 {
		t.Errorf("read %d records across segments, want 40", got)
	}
	// Lexical order must equal chronological order, or the compactor reads them out of order.
	sorted := make([]string, len(segs))
	copy(sorted, segs)
	for i := 1; i < len(sorted); i++ {
		if filepath.Base(sorted[i-1]) >= filepath.Base(sorted[i]) {
			t.Errorf("segment names not lexically ordered: %s then %s", sorted[i-1], sorted[i])
		}
	}
}

func TestConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, 5*time.Millisecond)

	const writers, each = 16, 50
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if err := w.Append(context.Background(), []byte(fmt.Sprintf("g%02d-i%03d", g, i))); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	w.Close()

	if got := len(readAll(t, dir)); got != writers*each {
		t.Errorf("read %d records, want %d", got, writers*each)
	}
}

// Truncating at every possible offset must never panic and must always yield a readable
// prefix. This is the property that matters at boot: whatever state a crash left, Open has to
// cope. A loop over all offsets is cheap and covers the boundaries a hand-written case misses
// — header split across a read, length present but payload absent, CRC half written.
func TestRecoveryAtEveryTruncationOffset(t *testing.T) {
	src := t.TempDir()
	w := testWAL(t, src, 5*time.Millisecond)
	for i := range 8 {
		if err := w.Append(context.Background(), []byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs, _ := listSegments(src)
	full, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}

	for cut := 0; cut <= len(full); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, segmentName(1)), full[:cut], 0o640); err != nil {
			t.Fatal(err)
		}

		w, err := Open(Options{Dir: dir, SyncInterval: time.Hour, SegmentBytes: 1 << 20})
		if err != nil {
			t.Fatalf("cut=%d: Open() = %v; a truncated tail must always recover", cut, err)
		}
		// Every surviving record must still read back cleanly.
		for _, rec := range readAll(t, dir) {
			if len(rec) == 0 {
				t.Fatalf("cut=%d: recovered a zero-length record", cut)
			}
		}
		w.Close()
	}
}

func FuzzReaderNeverPanics(f *testing.F) {
	// Seed with a well-formed record, so the fuzzer starts from something meaningful and
	// mutates outward rather than exploring random noise forever.
	good, _ := appendRecord(nil, []byte("seed"))
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, segmentName(1))
		if err := os.WriteFile(path, data, 0o640); err != nil {
			t.Skip()
		}
		// The contract is only that it terminates without panicking and without trying to
		// allocate the world; any error is a legitimate answer for arbitrary bytes.
		r, err := OpenReader(path)
		if err != nil {
			return
		}
		defer r.Close()
		for {
			if _, err := r.Next(); err != nil {
				return
			}
		}
	})
}

// Enqueue must not wait for the group commit — that is its entire reason to exist — and the
// records must still be durable afterwards. Both halves matter: the first is the performance
// claim, the second is that the shortcut costs no data.
func TestEnqueueDoesNotWaitButStillPersists(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, 200*time.Millisecond)

	start := time.Now()
	for i := range 100 {
		if err := w.Enqueue([]byte(fmt.Sprintf("e-%03d", i))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	elapsed := time.Since(start)

	// 100 enqueues cannot have waited on even one 200ms commit.
	if elapsed > 50*time.Millisecond {
		t.Errorf("100 Enqueues took %v; Enqueue must not wait for the group commit", elapsed)
	}
	// Nothing is on disk yet: the sync has not fired.
	if got := len(readAll(t, dir)); got != 0 {
		t.Errorf("found %d records on disk before the first sync, want 0", got)
	}

	// Close performs the final commit, so nothing enqueued is lost on a clean shutdown.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(readAll(t, dir)); got != 100 {
		t.Errorf("recovered %d records after Close, want all 100", got)
	}
}

// Enqueue and Append share one batch, so they must be safe to interleave from many goroutines.
func TestEnqueueAndAppendInterleave(t *testing.T) {
	dir := t.TempDir()
	w := testWAL(t, dir, 5*time.Millisecond)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				var err error
				if (g+i)%2 == 0 {
					err = w.Enqueue([]byte(fmt.Sprintf("q%02d-%03d", g, i)))
				} else {
					err = w.Append(context.Background(), []byte(fmt.Sprintf("a%02d-%03d", g, i)))
				}
				if err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	w.Close()

	if got := len(readAll(t, dir)); got != 8*50 {
		t.Errorf("read %d records, want 400", got)
	}
}

// A site that goes quiet must not strand its last events in an unrotated segment. Rotation is
// otherwise only considered after a write, so without an idle check those events stay
// uncompacted — and therefore invisible to every query — for as long as the quiet lasts.
func TestIdleSegmentStillRotates(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{
		Dir:          dir,
		SegmentBytes: 1 << 20,               // far from full
		SegmentAge:   50 * time.Millisecond, // but it ages out quickly
		SyncInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(context.Background(), []byte("last-event-before-going-quiet")); err != nil {
		t.Fatal(err)
	}

	// Now send nothing at all, as an idle site would.
	deadline := time.After(3 * time.Second)
	for {
		closed, err := w.ClosedSegments()
		if err != nil {
			t.Fatal(err)
		}
		if len(closed) > 0 {
			return // rotated while idle, which is the point
		}
		select {
		case <-deadline:
			t.Fatal("an idle segment holding data never rotated; its events would never be compacted")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// The other half: an idle node with an EMPTY segment must not churn out new files every tick.
func TestIdleEmptySegmentDoesNotRotate(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{
		Dir:          dir,
		SegmentBytes: 1 << 20,
		SegmentAge:   20 * time.Millisecond,
		SyncInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	time.Sleep(300 * time.Millisecond) // many intervals, no data

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Errorf("an idle empty WAL produced %d segments, want 1", len(segs))
	}
}
