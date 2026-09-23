package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const segmentExt = ".wal"

type Options struct {
	Dir string

	// SegmentBytes and SegmentAge bound a segment, whichever is reached first.
	//
	// The age bound is not cosmetic: a segment is only compacted once closed, so without it a
	// quiet site's events would sit unqueryable for as long as it took to reach the size
	// bound — days, on a site with little traffic.
	SegmentBytes int64
	SegmentAge   time.Duration

	// SyncInterval is the group-commit window. See the durability note in README.
	SyncInterval time.Duration

	// OnSync is called after every completed group commit with how long the fsync took and
	// how many records it covered. The metrics package owns the interpretation; this package
	// owns only the measurement.
	OnSync func(d time.Duration, records int)

	Log *slog.Logger
}

// WAL is an append-only log with group commit.
//
// Two mutexes, deliberately:
//
//   - mu guards the in-memory batch being accumulated.
//   - writeMu serialises access to the file itself.
//
// Splitting them is what keeps an fsync off the append path. The syncer swaps the pending
// buffer under mu, releases it, and only then writes and fsyncs under writeMu — so appends
// continue into a fresh buffer for the whole duration of a slow disk write, instead of
// blocking on it. With one mutex this would still be correct, and a 5ms fsync would stall
// every appender for 5ms.
type WAL struct {
	opt Options
	log *slog.Logger

	mu       sync.Mutex
	pending  []byte
	nPending int
	cur      *batch

	writeMu     sync.Mutex
	f           *os.File
	segBytes    int64
	segOpenedAt time.Time
	segSeq      uint64

	closeOnce sync.Once
	closed    chan struct{}
	done      chan struct{}

	errMu   sync.Mutex
	lastErr error
}

// batch is one group commit. Appenders take the current batch, then wait on done; the syncer
// swaps in a fresh one, performs the fsync, records the outcome, and releases everyone at once.
type batch struct {
	done chan struct{}
	err  error
}

func newBatch() *batch { return &batch{done: make(chan struct{})} }

// Open recovers the directory and opens it for appending.
//
// Recovery is the reason this function is longer than it looks like it should be. A crash
// leaves a partly-written record at the end of the last segment, which is expected and is
// truncated. The same damage anywhere else means the file was altered after it was written,
// which is not expected, and Open refuses rather than starting a process that would serve
// silently incomplete data.
func Open(o Options) (*WAL, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.SyncInterval <= 0 {
		return nil, fmt.Errorf("wal: SyncInterval must be positive")
	}
	if err := os.MkdirAll(o.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	segs, err := listSegments(o.Dir)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		opt:     o,
		log:     o.Log,
		pending: make([]byte, 0, 1<<20),
		cur:     newBatch(),
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
	}

	for i, seg := range segs {
		last := i == len(segs)-1
		n, off, err := scanSegment(seg)

		switch {
		case err == nil:
			// Intact.
		case errors.Is(err, ErrTorn) && last:
			w.log.Warn("wal: truncating torn tail of last segment",
				"segment", filepath.Base(seg), "offset", off, "records_kept", n)
			if err := os.Truncate(seg, off); err != nil {
				return nil, fmt.Errorf("wal: truncate %s: %w", seg, err)
			}
		case errors.Is(err, ErrTorn):
			// Damage before the final segment cannot be a crash: everything after it was
			// written later and fsynced since.
			return nil, fmt.Errorf("wal: corruption in %s at offset %d, and it is not the last segment: refusing to start", filepath.Base(seg), off)
		default:
			return nil, err
		}
	}

	if err := w.openSegment(nextSeq(segs)); err != nil {
		return nil, err
	}

	go w.syncLoop()

	return w, nil
}

// Append records a payload and returns once it is durable.
//
// It returns only after the fsync covering it has completed, so a caller that has seen a nil
// error may acknowledge to its own client. That is the entire contract, and it is why ingest
// can be a thin handler: the durability decision lives here, once.
func (w *WAL) Append(ctx context.Context, payload []byte) error {
	select {
	case <-w.closed:
		return errors.New("wal: closed")
	default:
	}

	w.mu.Lock()
	next, err := appendRecord(w.pending, payload)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.pending = next
	w.nPending++
	b := w.cur
	w.mu.Unlock()

	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		// The record stays in the batch and will still be written. Reporting the context
		// error here means the caller does not know whether it landed — which is honest,
		// and why ingest must treat a cancelled append as "unknown", never as "dropped".
		return ctx.Err()
	}
}

// Unsynced reports how many acknowledged-in-memory records have not yet been fsynced: the
// current loss window, in records.
func (w *WAL) Unsynced() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nPending
}

// Err reports the last sync failure, for readiness. A node whose disk has stopped accepting
// writes is alive but must not be sent traffic.
func (w *WAL) Err() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.lastErr
}

func (w *WAL) syncLoop() {
	defer close(w.done)

	t := time.NewTicker(w.opt.SyncInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			w.syncOnce()
		case <-w.closed:
			// Final commit, so a clean shutdown loses nothing.
			w.syncOnce()
			return
		}
	}
}

func (w *WAL) syncOnce() {
	w.mu.Lock()
	if w.nPending == 0 {
		// Nothing to do, and nobody waiting: an empty batch must not cost an fsync. On an
		// idle node this is the common case, every interval, forever.
		w.mu.Unlock()
		// A segment holding data can still have aged out. Rotation is otherwise only
		// considered after a write, so on a site that goes quiet the last events would stay
		// in an unrotated — and therefore uncompacted, and therefore unqueryable — segment
		// for as long as the quiet lasted. The age bound exists precisely for that case, so
		// it has to be enforced when nothing is arriving.
		w.rotateIfStale()
		return
	}
	data, n, b := w.pending, w.nPending, w.cur
	w.pending = make([]byte, 0, max(1<<20, len(data)))
	w.nPending = 0
	w.cur = newBatch()
	w.mu.Unlock()

	start := time.Now()
	err := w.writeAndSync(data)
	elapsed := time.Since(start)

	w.errMu.Lock()
	w.lastErr = err
	w.errMu.Unlock()

	if err != nil {
		w.log.Error("wal: sync failed", "err", err, "records", n)
	}
	if w.opt.OnSync != nil {
		w.opt.OnSync(elapsed, n)
	}

	// Set the error before releasing waiters, or an appender can observe the zero value.
	b.err = err
	close(b.done)
}

// rotateIfStale closes an aged segment that has data, even with nothing pending.
//
// Never rotates an empty segment: an idle node would otherwise create a fresh file every
// interval forever, and the compactor would spend its time on nothing.
func (w *WAL) rotateIfStale() {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.f == nil || w.segBytes == 0 || w.opt.SegmentAge <= 0 {
		return
	}
	if time.Since(w.segOpenedAt) < w.opt.SegmentAge {
		return
	}
	if err := w.rotateLocked(); err != nil {
		w.log.Error("wal: rotating a stale segment failed", "err", err)
	}
}

func (w *WAL) writeAndSync(data []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.f == nil {
		return errors.New("wal: no open segment")
	}
	if _, err := w.f.Write(data); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.segBytes += int64(len(data))

	// Rotation after the sync, never before: a segment is only closed once everything in it
	// is durable, which is what lets the compactor treat a closed segment as final.
	if w.shouldRotate() {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) shouldRotate() bool {
	if w.opt.SegmentBytes > 0 && w.segBytes >= w.opt.SegmentBytes {
		return true
	}
	return w.opt.SegmentAge > 0 && time.Since(w.segOpenedAt) >= w.opt.SegmentAge
}

func (w *WAL) rotateLocked() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	return w.openSegmentLocked(w.segSeq + 1)
}

func (w *WAL) openSegment(seq uint64) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return w.openSegmentLocked(seq)
}

func (w *WAL) openSegmentLocked(seq uint64) error {
	path := filepath.Join(w.opt.Dir, segmentName(seq))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	w.f = f
	w.segBytes = st.Size()
	w.segOpenedAt = time.Now()
	w.segSeq = seq

	// fsync the directory so the new file's name survives a crash. Without it the data can be
	// durable while the entry pointing at it is not, which is the failure that looks like the
	// events simply never existed.
	if err := syncDir(w.opt.Dir); err != nil {
		return err
	}

	w.log.Info("wal: segment open", "segment", segmentName(seq), "bytes", w.segBytes)
	return nil
}

// Close stops the syncer, commits what is pending, and closes the current segment.
func (w *WAL) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	<-w.done

	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func segmentName(seq uint64) string {
	// Zero-padded so lexical order is chronological order, which is what makes listing
	// segments with filepath.Glob and sorting them correct rather than nearly correct.
	return fmt.Sprintf("%012d%s", seq, segmentExt)
}

func listSegments(dir string) ([]string, error) {
	segs, err := filepath.Glob(filepath.Join(dir, "*"+segmentExt))
	if err != nil {
		return nil, err
	}
	sort.Strings(segs)
	return segs, nil
}

func nextSeq(segs []string) uint64 {
	if len(segs) == 0 {
		return 1
	}
	base := strings.TrimSuffix(filepath.Base(segs[len(segs)-1]), segmentExt)
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 1
	}
	// Append to the newest segment rather than starting a fresh one, so a restart does not
	// litter the directory with tiny files.
	return n
}

// scanSegment reads a segment to its end, returning the record count and the offset at which
// the intact prefix ends.
func scanSegment(path string) (records int, endOffset int64, err error) {
	r, err := OpenReader(path)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()

	for {
		_, err := r.Next()
		if errors.Is(err, io.EOF) {
			return records, r.Offset(), nil
		}
		if err != nil {
			return records, r.Offset(), err
		}
		records++
	}
}

// Enqueue records a payload and returns as soon as it is in the pending batch, without waiting
// for the fsync that will cover it.
//
// The distinction from Append is narrower than it looks, and it is worth being exact about
// because it is easy to reach for the wrong one:
//
//   - Both have the SAME durability window. The syncer fsyncs on its interval either way, so
//     a hard kill loses the same events under both.
//   - They differ only in whether the CALLER LEARNS that the write is durable.
//
// So waiting is worth it only when the caller would do something different on failure. A
// browser beacon would not: nothing retries a pageview, and the response is discarded. Making
// it wait costs a full sync interval of latency and caps one connection at
// 1/SyncInterval events per second, while buying no actual safety.
//
// Ingest therefore uses Enqueue. Append remains for callers that genuinely act on the answer.
func (w *WAL) Enqueue(payload []byte) error {
	select {
	case <-w.closed:
		return errors.New("wal: closed")
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	next, err := appendRecord(w.pending, payload)
	if err != nil {
		return err
	}
	w.pending = next
	w.nPending++
	return nil
}

// ClosedSegments returns the segments that are complete and will never be appended to again,
// oldest first.
//
// A segment is closed once rotation has moved past it, and rotation only happens after an
// fsync — so everything in a closed segment is durable. That is the property the compactor
// relies on: it may read one of these, write Parquet, and delete it, without coordinating with
// the writer at all.
//
// The active segment is excluded even when it is full, because "full" and "rotated" are not
// the same instant.
func (w *WAL) ClosedSegments() ([]string, error) {
	segs, err := listSegments(w.opt.Dir)
	if err != nil {
		return nil, err
	}

	w.writeMu.Lock()
	active := w.segSeq
	w.writeMu.Unlock()

	out := make([]string, 0, len(segs))
	for _, s := range segs {
		seq, err := SegmentSeq(s)
		if err != nil || seq >= active {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// SegmentSeq extracts a segment's sequence number from its path.
//
// The compactor derives its output filenames from this, which is what makes compaction
// idempotent: re-running after a crash writes the same names and overwrites, rather than
// producing a second copy of every event.
func SegmentSeq(path string) (uint64, error) {
	base := strings.TrimSuffix(filepath.Base(path), segmentExt)
	return strconv.ParseUint(base, 10, 64)
}
