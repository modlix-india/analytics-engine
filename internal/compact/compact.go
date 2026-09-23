// Package compact folds closed WAL segments into Parquet.
//
// It is the only writer of the Parquet tier and the only deleter of WAL segments, which keeps
// the ownership rule simple: ingest appends, compaction converts, nothing else touches either.
//
// # Crash safety
//
// Compaction is idempotent by construction. Output filenames are derived from the input
// segment's sequence number, so re-running a segment writes exactly the same files and
// overwrites them. That turns the interrupted-midway case — the one that normally produces
// duplicate events, silently, forever — into a no-op:
//
//  1. read segment N
//  2. write its Parquet files (each atomically, via temp-and-rename)
//  3. delete segment N
//
// A crash between 2 and 3 re-does 2 on restart and reaches the same result. A crash inside 2
// leaves some files written and some not; the re-run completes them. At no point can a reader
// see a partial file, because of the rename in store.WriteFile.
package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
	"github.com/modlix-india/analytics-engine/internal/rollup"
	"github.com/modlix-india/analytics-engine/internal/store"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

type Options struct {
	WAL      *wal.WAL
	DataDir  string
	NodeID   string
	Interval time.Duration
	Log      *slog.Logger

	// MaxBufferedRows bounds how many rows are held in memory before a partition is flushed
	// to its own part file.
	//
	// Without it, one segment full of a single site's traffic would be materialised entirely
	// as Go structs, which is several times the segment's own size. The cost of flushing
	// early is more, smaller files; the daily merge pass exists to undo that.
	MaxBufferedRows int

	// OnLag reports the age of the oldest event still only in the WAL — which is query
	// freshness, and the number to look at when a dashboard seems behind.
	OnLag func(time.Duration)
}

type Compactor struct {
	opt Options
	log *slog.Logger
}

func New(o Options) *Compactor {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.MaxBufferedRows <= 0 {
		o.MaxBufferedRows = 200_000
	}
	if o.Interval <= 0 {
		o.Interval = 5 * time.Minute
	}
	return &Compactor{opt: o, log: o.Log}
}

// Run compacts on an interval until the context is cancelled, then compacts once more.
//
// The final pass matters on a clean shutdown: the WAL's own Close has already committed
// everything pending, so without this the last segments would sit unqueryable until the
// process next starts.
func (c *Compactor) Run(ctx context.Context) {
	t := time.NewTicker(c.opt.Interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			c.runOnce()
		case <-ctx.Done():
			c.runOnce()
			return
		}
	}
}

func (c *Compactor) runOnce() {
	segs, err := c.opt.WAL.ClosedSegments()
	if err != nil {
		c.log.Error("compact: list segments", "err", err)
		return
	}
	if len(segs) == 0 {
		c.reportLag(nil)
		return
	}

	start := time.Now()
	var events, files int

	for _, seg := range segs {
		e, f, err := c.compactSegment(seg)
		if err != nil {
			// Stop at the first failure rather than skipping ahead. Segments are compacted
			// oldest first, and carrying on past a broken one would leave a permanent hole
			// that later passes keep stepping over.
			c.log.Error("compact: segment failed, stopping this pass",
				"segment", filepath.Base(seg), "err", err)
			break
		}
		events += e
		files += f
	}

	if events > 0 {
		c.log.Info("compact: done",
			"segments", len(segs), "events", events, "files", files,
			"took", time.Since(start).String())
	}
	c.reportLag(nil)
}

// compactSegment converts one segment and then removes it.
func (c *Compactor) compactSegment(seg string) (events, files int, err error) {
	seq, err := wal.SegmentSeq(seg)
	if err != nil {
		return 0, 0, fmt.Errorf("segment name %q: %w", filepath.Base(seg), err)
	}

	r, err := wal.OpenReader(seg)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()

	// Buffered per partition, because one segment interleaves every site this node serves and
	// Parquet wants them separated.
	buf := map[partition][]store.Row{}
	part := map[partition]int{}
	buffered := 0

	flush := func(p partition) error {
		rows := buf[p]
		if len(rows) == 0 {
			return nil
		}

		// Raw first, rollup second. If the process dies between the two, the next run
		// re-does both from the same segment and overwrites both — the filenames derive
		// from the segment sequence, so neither tier can end up with a duplicate.
		if err := store.WriteFile(c.pathFor("data", p, seq, part[p]), rows); err != nil {
			return err
		}
		if err := rollup.WriteFile(c.pathFor("rollup", p, seq, part[p]), rollup.Build(rows)); err != nil {
			return err
		}

		part[p]++
		files++
		buffered -= len(rows)
		buf[p] = nil
		return nil
	}

	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A torn record here means this segment was damaged after being closed, since
			// rotation only happens after an fsync. Everything before it is still good and
			// is written; the rest is lost and said so, loudly.
			c.log.Error("compact: truncated segment, keeping the intact prefix",
				"segment", filepath.Base(seg), "offset", r.Offset(), "err", err)
			break
		}

		ev, err := event.Decode(rec)
		if err != nil {
			// One unreadable record must not cost the whole segment.
			c.log.Warn("compact: skipping undecodable record", "segment", filepath.Base(seg), "err", err)
			continue
		}

		row := store.RowOf(ev)
		site, date := store.PartitionOf(&row)
		p := partition{site: site, date: date}

		buf[p] = append(buf[p], row)
		buffered++
		events++

		if buffered >= c.opt.MaxBufferedRows {
			// Flush the largest partition rather than all of them: it reclaims the most
			// memory for one file, and leaves small partitions to accumulate into
			// reasonably sized files instead of a scatter of tiny ones.
			if err := flush(largest(buf)); err != nil {
				return events, files, err
			}
		}
	}

	for p := range buf {
		if err := flush(p); err != nil {
			return events, files, err
		}
	}

	// Only now is the segment expendable. Every file above was renamed into place and its
	// directory fsynced, so the data exists in the Parquet tier before it stops existing here.
	if err := os.Remove(seg); err != nil {
		return events, files, fmt.Errorf("remove compacted segment: %w", err)
	}

	return events, files, nil
}

type partition struct{ site, date string }

// pathFor builds {tier}/{site}/{date}/{node}-{walseq}[-{part}].parquet.
//
// The two tiers are parallel trees rather than files side by side, so a query reads only the
// one it needs and never lists the other. The part suffix appears only when a segment had to
// be flushed more than once for one partition, keeping the common case's name simpler.
func (c *Compactor) pathFor(tier string, p partition, seq uint64, part int) string {
	name := fmt.Sprintf("%s-%012d.parquet", store.SafeSegment(c.opt.NodeID), seq)
	if part > 0 {
		name = fmt.Sprintf("%s-%012d-%d.parquet", store.SafeSegment(c.opt.NodeID), seq, part)
	}
	return filepath.Join(c.opt.DataDir, tier,
		store.SafeSegment(p.site), store.SafeSegment(p.date), name)
}

func largest(buf map[partition][]store.Row) partition {
	var best partition
	n := -1
	for p, rows := range buf {
		if len(rows) > n {
			best, n = p, len(rows)
		}
	}
	return best
}

// reportLag publishes how stale the Parquet tier is.
//
// Measured from the oldest closed-but-uncompacted segment's modification time, which is when
// it stopped being written to. It rises on its own if compaction stalls, so it alerts without
// needing a separate liveness check on this goroutine.
func (c *Compactor) reportLag(_ any) {
	if c.opt.OnLag == nil {
		return
	}
	segs, err := c.opt.WAL.ClosedSegments()
	if err != nil || len(segs) == 0 {
		c.opt.OnLag(0)
		return
	}
	st, err := os.Stat(segs[0])
	if err != nil {
		c.opt.OnLag(0)
		return
	}
	c.opt.OnLag(time.Since(st.ModTime()))
}
