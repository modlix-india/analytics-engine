package query

import (
	"fmt"
	"hash/maphash"
	"path/filepath"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
)

// The per-visitor analyses — funnel, retention, stickiness, lifecycle — are the only ones that
// cannot be answered from rollups, because they depend on the ORDER and SPACING of one
// visitor's events rather than on totals. A rollup has thrown that away by construction.
//
// They therefore read raw Parquet, and the two costs that implies are handled here:
//
//   - Width. A narrow projection struct reads three columns instead of twenty-five; parquet-go
//     matches by name and never decompresses the rest. A funnel over a month touches a small
//     fraction of the bytes the raw tier holds.
//   - Depth. Holding every visitor's timeline in memory does not scale, so the scan is
//     hash-partitioned on visitor: each pass keeps one slice of visitors and re-reads the
//     files. All four accumulators are additive across partitions, since a visitor belongs to
//     exactly one, so partial results simply sum.

// seqRow is the projection. Adding a field here costs a column read on every one of these
// queries, so it is deliberately the minimum the four analyses need.
type seqRow struct {
	TSServer int64  `parquet:"ts_server,timestamp,delta"`
	Name     string `parquet:"event,dict,zstd"`
	Visitor  string `parquet:"visitor,zstd"`
}

type seqEvent struct {
	TS   int64
	Name string
}

// visitorPartitions is how many slices the scan is split into.
//
// The trade is memory against IO: more partitions hold fewer visitors at once but re-read the
// files once each. Sixteen keeps a month of a large site within a few hundred MB while costing
// sixteen passes over a projection that is already small.
const visitorPartitions = 16

var visitorSeed = maphash.MakeSeed()

func partitionOf(visitor string, parts int) int {
	return int(maphash.String(visitorSeed, visitor) % uint64(parts))
}

// scanVisitors calls fn once per visitor in the given partition, with that visitor's events in
// time order.
//
// fn must not retain the slice: it is reused between visitors, because allocating one per
// visitor is the difference between a scan that fits in memory and one that does not.
func (e *Engine) scanVisitors(site string, s span, parts, part int, fn func(visitor string, evs []seqEvent)) error {
	byVisitor := map[string][]seqEvent{}

	for _, date := range utcDates(s) {
		files, err := e.filesIn("data", site, date)
		if err != nil {
			return err
		}
		for _, f := range files {
			rows, err := parquet.ReadFile[seqRow](f)
			if err != nil {
				return fmt.Errorf("read raw %s: %w", filepath.Base(f), err)
			}
			for i := range rows {
				r := &rows[i]
				if r.Visitor == "" {
					continue
				}
				ts := time.UnixMilli(r.TSServer)
				if ts.Before(s.Start) || !ts.Before(s.End) {
					continue
				}
				if partitionOf(r.Visitor, parts) != part {
					continue
				}
				byVisitor[r.Visitor] = append(byVisitor[r.Visitor], seqEvent{TS: r.TSServer, Name: r.Name})
			}
		}
	}

	for v, evs := range byVisitor {
		// Files are already sorted by time, but a visitor's events can come from several
		// files — different segments, different nodes — so the merge is not free.
		sort.Slice(evs, func(i, j int) bool { return evs[i].TS < evs[j].TS })
		fn(v, evs)
	}
	return nil
}

// eachVisitor runs fn over every visitor, one partition at a time.
func (e *Engine) eachVisitor(site string, s span, fn func(visitor string, evs []seqEvent)) error {
	for p := range visitorPartitions {
		if err := e.scanVisitors(site, s, visitorPartitions, p, fn); err != nil {
			return err
		}
	}
	return nil
}

// funnelDepth reports how many consecutive steps a visitor completed.
//
// Strict ordering: each step must occur after the previous one, and the whole sequence within
// the conversion window measured from the FIRST step. That last detail is the one most often
// got wrong — a window measured per-step would let a visitor take a month over five steps and
// still be counted as converting within a day.
//
// The first occurrence of step one is the anchor. Trying every possible anchor would count
// more conversions and is what some tools do; anchoring on the first is the stricter and more
// defensible reading, and it is stated here because the two differ on real data.
func funnelDepth(evs []seqEvent, steps []string, window time.Duration) int {
	if len(steps) == 0 {
		return 0
	}

	i := 0
	for ; i < len(evs); i++ {
		if evs[i].Name == steps[0] {
			break
		}
	}
	if i == len(evs) {
		return 0
	}

	anchor := evs[i].TS
	deadline := anchor + window.Milliseconds()
	last := anchor
	depth := 1

	for step := 1; step < len(steps); step++ {
		found := false
		for j := i + 1; j < len(evs); j++ {
			if window > 0 && evs[j].TS > deadline {
				return depth
			}
			// Strictly AFTER the previous step in time, not merely later in the file.
			// Ordering by position would make the answer depend on which row a file
			// happened to store first, and would disagree with any SQL expressing the same
			// definition. Ingest gives each event in a batch its own millisecond so this
			// remains satisfiable for steps reported together.
			if evs[j].TS <= last {
				continue
			}
			if evs[j].Name == steps[step] {
				i, last, found = j, evs[j].TS, true
				break
			}
		}
		if !found {
			return depth
		}
		depth++
	}
	return depth
}

// periodKey buckets a timestamp into the local day or week it belongs to.
//
// Uses a real time.Location throughout, so a day is whatever length that zone says it is.
func periodKey(ts int64, loc *time.Location, weekly bool) string {
	t := time.UnixMilli(ts).In(loc)
	if !weekly {
		return t.Format("2006-01-02")
	}
	// ISO week, so a week boundary is Monday everywhere rather than depending on locale.
	y, w := t.ISOWeek()
	return fmt.Sprintf("%04d-W%02d", y, w)
}

// periodsBetween lists every period label a range touches, so an empty period is reported as
// zero rather than silently missing — a gap in a retention grid is information.
//
// It advances to the next CALENDAR boundary rather than adding a fixed 24 hours. That
// distinction is the whole function: stepping by 24h from an arbitrary start time visits one
// instant per day, and a range that begins at 16:00 and ends 24 hours later then yields a
// single period, silently dropping every event in the second calendar day. The unit tests
// missed this because they used ranges already aligned to midnight; an end-to-end query with
// a "last 24 hours" range did not.
//
// It also means a week is a week and a DST day is 23 or 25 hours, both of which fall out of
// asking the calendar instead of doing arithmetic.
func periodsBetween(s span, loc *time.Location, weekly bool) []string {
	var out []string
	seen := map[string]bool{}

	for t := s.Start.In(loc); t.Before(s.End); t = nextPeriodStart(t, loc, weekly) {
		k := periodKey(t.UnixMilli(), loc, weekly)
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// nextPeriodStart returns the first instant of the period after the one containing t.
func nextPeriodStart(t time.Time, loc *time.Location, weekly bool) time.Time {
	t = t.In(loc)
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)

	if !weekly {
		return midnight.AddDate(0, 0, 1)
	}
	// ISO weeks start on Monday, so advance day by day to the next one. At most seven steps,
	// and it stays correct across month and year boundaries without any arithmetic of its own.
	for d := midnight.AddDate(0, 0, 1); ; d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Monday {
			return d
		}
	}
}
