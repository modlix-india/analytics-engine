// Package query answers the fixed widget set from the rollup tier.
//
// The client never supplies a query — only a widget name, a site, a range and a few
// parameters. That is the whole security model for reads: with no client-supplied expression
// there is nothing to sanitise, and no amount of cleverness in a request can widen its scope
// beyond the site the caller was authorised for.
package query

import (
	"fmt"
	"time"

	"github.com/modlix-india/analytics-engine/internal/rollup"
)

// span is a half-open absolute interval, [Start, End).
type span struct {
	Start, End time.Time
}

// hourPlan splits an interval into whole hourly rollup buckets plus the partial hours at each
// end that a rollup cannot answer.
//
// This is the timezone fix in one function. A rollup bucket covers a whole UTC hour, so:
//
//   - A whole-hour offset (UTC+5, UTC-8) always lands on hour boundaries. Every bucket is
//     full, Partials is empty, and the answer is exact from rollups alone.
//   - A half-hour offset — India's +5:30, Nepal's +5:45, parts of Australia — lands mid-hour.
//     The two boundary hours are then half inside the range and half outside, and no rollup
//     can separate them. Those hours are recomputed from raw Parquet and everything between
//     still comes from rollups.
//
// The correction is two hours of raw data no matter how long the range, which is why this
// stays cheap for a thirty-day query.
type hourPlan struct {
	FullHours []int64 // rollup bucket starts, unix millis UTC
	Partials  []span  // sub-hour remainders needing a raw scan
}

func planHours(s span) hourPlan {
	var p hourPlan
	if !s.End.After(s.Start) {
		return p
	}

	// The first whole bucket starts at or after Start; the last ends at or before End.
	firstFull := ceilHour(s.Start)
	lastFull := s.End.UTC().Truncate(time.Hour)

	// The whole range sits inside a single hour, so no bucket is fully covered.
	if !lastFull.After(firstFull) {
		return hourPlan{Partials: []span{{s.Start, s.End}}}
	}

	if s.Start.Before(firstFull) {
		p.Partials = append(p.Partials, span{s.Start, firstFull})
	}
	if lastFull.Before(s.End) {
		p.Partials = append(p.Partials, span{lastFull, s.End})
	}

	for h := firstFull; h.Before(lastFull); h = h.Add(time.Hour) {
		p.FullHours = append(p.FullHours, h.UnixMilli())
	}
	return p
}

func ceilHour(t time.Time) time.Time {
	tr := t.UTC().Truncate(time.Hour)
	if tr.Before(t) {
		return tr.Add(time.Hour)
	}
	return tr
}

// daySpans divides an interval into the site's local calendar days.
//
// Uses a real time.Location and the actual boundaries rather than a fixed offset, because a
// day is 23 or 25 hours long in any zone that observes DST. Adding 24 hours would be wrong
// twice a year — and India, which does not observe it, is exactly where that bug would survive
// every local test and then appear for a customer elsewhere.
func daySpans(s span, loc *time.Location) []span {
	var out []span

	cur := s.Start.In(loc)
	cur = time.Date(cur.Year(), cur.Month(), cur.Day(), 0, 0, 0, 0, loc)

	for cur.Before(s.End) {
		// Midnight of the following calendar day, which is 23, 24 or 25 hours away.
		next := time.Date(cur.Year(), cur.Month(), cur.Day()+1, 0, 0, 0, 0, loc)

		start, end := cur, next
		if start.Before(s.Start) {
			start = s.Start
		}
		if end.After(s.End) {
			end = s.End
		}
		if end.After(start) {
			out = append(out, span{start, end})
		}
		cur = next
	}
	return out
}

// utcDates lists the UTC date directories an interval touches.
//
// A local day maps onto two UTC dates for any non-zero offset, which is why the partition is
// only ever a pruning hint: the reader opens both directories and filters on exact timestamps.
func utcDates(s span) []string {
	var out []string
	d := s.Start.UTC().Truncate(24 * time.Hour)
	for !d.After(s.End.UTC()) {
		out = append(out, d.Format("2006-01-02"))
		d = d.Add(24 * time.Hour)
	}
	return out
}

func loadLocation(name string) (*time.Location, error) {
	if name == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("unknown timezone %q: %w", name, err)
	}
	return loc, nil
}

// inHours reports whether a rollup row falls in the given bucket set.
func inHours(r *rollup.Row, hours map[int64]bool) bool { return hours[r.Hour] }
