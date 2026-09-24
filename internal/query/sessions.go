package query

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/modlix-india/analytics-engine/internal/event"
)

// The visit analyses: what one person did between arriving and leaving.
//
// These are the questions the daily-rotating visitor identity does NOT cost us. Retention,
// stickiness and lifecycle all ask what somebody did on several different days, and the salt
// rotation makes that unanswerable; everything here happens inside one visit, which is at most
// a few minutes long and can never straddle the rotation in a way that matters.
//
// The machinery is the per-visitor sort-merge from sequence.go, partitioned on the session
// rather than the visitor. A session belongs to exactly one partition, so every accumulator
// here is additive across partitions in the same way.

// sessIdle is how long a gap ends a visit.
//
// It is not a choice this file gets to make: the beacon mints a new session id after this much
// inactivity (IDLE_MS in analytics.js), so a session's events cannot span a longer gap. It is
// restated here because the two must agree, and because it is what makes the boundary handling
// below exact rather than approximate.
const sessIdle = 30 * time.Minute

// visitBuckets are the lengths a visit is reported in.
//
// A histogram rather than an average, and rather than a median, for a reason that is about the
// scan and not about presentation: buckets ADD across visitor partitions, so sixteen passes
// sum into one answer. A median does not, and computing one would mean holding every visit's
// duration in memory at once, which is the thing the partitioning exists to avoid.
var visitBuckets = []struct {
	label string
	upto  time.Duration
}{
	{"under 10s", 10 * time.Second},
	{"10s to 30s", 30 * time.Second},
	{"30s to 1m", time.Minute},
	{"1m to 3m", 3 * time.Minute},
	{"3m to 10m", 10 * time.Minute},
	{"over 10m", 1<<62 - 1},
}

// sessRow is the projection these four read.
//
// Wider than seqRow by three columns, and kept separate rather than widening that one: the
// funnel and retention read a month at a time and would pay for path and page on every row
// without ever looking at them.
type sessRow struct {
	TSServer int64  `parquet:"ts_server,timestamp,delta"`
	Name     string `parquet:"event,dict,zstd"`
	Session  string `parquet:"session,zstd"`
	Path     string `parquet:"path,dict,zstd"`
	Page     string `parquet:"page,dict,zstd"`
}

type sessEvent struct {
	TS   int64
	Name string
	Page string // the page's identity, already resolved from page-or-path
}

// visit is one session's events, in time order, plus what the boundary scan learned about it.
type visit struct {
	events []sessEvent

	// startsInRange is false for a visit that was already under way when the range began. Its
	// entry page happened before the window and counting it here would attribute an arrival to
	// the wrong day.
	startsInRange bool

	// complete is false for a visit that was still going when the range ended. Its last page
	// is not an exit, its page count is short and its length is truncated — so it is counted
	// as an arrival and nothing else.
	complete bool
}

// pageOf resolves the identity a visit's step is filed under.
//
// The page NAME wins over the address, the same way the heatmap decides, and for the same
// reason: routing serves two different definitions at one URL, so keying on the address alone
// merges two pages into one row. The address stands in where no name was reported, which is
// what a row recorded before the application knew where it was looks like.
func pageOf(r *sessRow) string {
	if r.Page != "" {
		return r.Page
	}
	return r.Path
}

// scanVisits calls fn once per visit in the given partition.
//
// # The boundary, which is the whole difficulty
//
// A visit that is still running when the range ends has not left anywhere, and a visit that
// began before the range started did not arrive during it. Scanning exactly [from, to) gets
// both wrong in the same direction — it invents an exit for every visit in progress, which
// makes the busiest page look like the biggest leak, convincingly and with no error anywhere.
//
// So the scan is widened by sessIdle at each end, and the extra events are used only to
// classify, never to count. The widening is exactly enough rather than a guess: the beacon
// cannot reuse a session id across a longer gap, so a session with no event in [to, to+idle)
// provably ended inside the range.
func (e *Engine) scanVisits(ctx context.Context, site string, s span, parts, part int, fn func(v *visit)) error {
	scan := span{s.Start.Add(-sessIdle), s.End.Add(sessIdle)}

	bySession := map[string][]sessEvent{}
	for _, date := range utcDates(scan) {
		files, err := e.filesIn(ctx, "data", site, date)
		if err != nil {
			return err
		}
		for _, f := range files {
			rows, err := parquet.ReadFile[sessRow](f)
			if err != nil {
				return fmt.Errorf("read raw %s: %w", filepath.Base(f), err)
			}
			for i := range rows {
				r := &rows[i]
				if r.Session == "" {
					continue
				}
				ts := time.UnixMilli(r.TSServer)
				if ts.Before(scan.Start) || !ts.Before(scan.End) {
					continue
				}
				if partitionOf(r.Session, parts) != part {
					continue
				}
				bySession[r.Session] = append(bySession[r.Session],
					sessEvent{TS: r.TSServer, Name: r.Name, Page: pageOf(r)})
			}
		}
	}

	for _, evs := range bySession {
		sort.Slice(evs, func(i, j int) bool { return evs[i].TS < evs[j].TS })

		first := time.UnixMilli(evs[0].TS)
		last := time.UnixMilli(evs[len(evs)-1].TS)

		v := visit{
			events:        evs,
			startsInRange: !first.Before(s.Start) && first.Before(s.End),
			complete:      last.Before(s.End),
		}
		// Only the events inside the range are handed on. The lead and tail exist to classify
		// the visit, and letting them through would count a page view twice on two adjacent
		// days — the correction becoming a second, quieter error than the one it fixed.
		trimmed := evs[:0]
		for _, ev := range evs {
			t := time.UnixMilli(ev.TS)
			if !t.Before(s.Start) && t.Before(s.End) {
				trimmed = append(trimmed, ev)
			}
		}
		if len(trimmed) == 0 {
			continue
		}
		v.events = trimmed
		fn(&v)
	}
	return nil
}

// eachVisit runs fn over every visit, one partition at a time.
func (e *Engine) eachVisit(ctx context.Context, site string, s span, fn func(v *visit)) error {
	for p := range visitorPartitions {
		if err := e.scanVisits(ctx, site, s, visitorPartitions, p, fn); err != nil {
			return err
		}
	}
	return nil
}

// pageViews keeps only the page views, which is what an entry or an exit is about. A visit
// whose last event is a click has still left from the page that click was on.
func (v *visit) pageViews() []sessEvent {
	out := make([]sessEvent, 0, len(v.events))
	for _, ev := range v.events {
		if ev.Name == event.NamePageview {
			out = append(out, ev)
		}
	}
	return out
}

// entryPages ranks the pages visits begin on.
//
// Counted for every visit that started inside the range, complete or not: where somebody
// arrived is known the moment they arrive, so waiting to see how the visit ends would drop
// today's arrivals from today's answer.
func (e *Engine) entryPages(ctx context.Context, req Request) (*Result, error) {
	counts := map[string]int64{}
	var total int64

	err := e.eachVisit(ctx, req.Site, span{req.From, req.To}, func(v *visit) {
		if !v.startsInRange {
			return
		}
		views := v.pageViews()
		if len(views) == 0 {
			return
		}
		total++
		counts[views[0].Page]++
	})
	if err != nil {
		return nil, err
	}
	return rankedVisits(req, counts, total), nil
}

// exitPages ranks the pages visits end on, and reports the total so a share can be computed
// against the same denominator every tile uses.
//
// Only COMPLETE visits count. One still in progress has not left anywhere, and the page it
// happens to be sitting on is not an exit.
func (e *Engine) exitPages(ctx context.Context, req Request) (*Result, error) {
	counts := map[string]int64{}
	var total int64

	err := e.eachVisit(ctx, req.Site, span{req.From, req.To}, func(v *visit) {
		if !v.startsInRange || !v.complete {
			return
		}
		views := v.pageViews()
		if len(views) == 0 {
			return
		}
		total++
		counts[views[len(views)-1].Page]++
	})
	if err != nil {
		return nil, err
	}
	return rankedVisits(req, counts, total), nil
}

// pagesPerVisit is the distribution, not the average.
//
// An average here is the classic misleading number: a site where most people read one page and
// a few read twenty reports "3.1 pages per visit", which describes nobody. The distribution
// says the thing an owner can act on, which is how many people saw only the page they landed
// on.
func (e *Engine) pagesPerVisit(ctx context.Context, req Request) (*Result, error) {
	const maxPages = 10

	counts := map[int]int64{}
	var total int64

	err := e.eachVisit(ctx, req.Site, span{req.From, req.To}, func(v *visit) {
		if !v.startsInRange || !v.complete {
			return
		}
		seen := len(v.pageViews())
		if seen == 0 {
			return
		}
		total++
		if seen > maxPages {
			seen = maxPages
		}
		counts[seen]++
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site, Denominator: total}
	for n := 1; n <= maxPages; n++ {
		if counts[n] == 0 {
			continue
		}
		label := strconv.Itoa(n)
		if n == maxPages {
			label = strconv.Itoa(maxPages) + "+"
		}
		res.Rows = append(res.Rows, Row{Label: label, Events: counts[n]})
	}
	return res, nil
}

// visitLength buckets how long a visit lasted, first event to last.
//
// Wall clock, not attention: a tab left open behind another one is counted as a long visit,
// and a visit of one page view has no length at all rather than a length of zero. Both are
// stated in the label the dashboard uses, because this is the measure most often quoted as if
// it meant engagement — which is a different number, and one nothing captures yet.
func (e *Engine) visitLength(ctx context.Context, req Request) (*Result, error) {
	counts := make([]int64, len(visitBuckets))
	var total int64

	err := e.eachVisit(ctx, req.Site, span{req.From, req.To}, func(v *visit) {
		if !v.startsInRange || !v.complete || len(v.events) < 2 {
			return
		}
		total++
		d := time.Duration(v.events[len(v.events)-1].TS-v.events[0].TS) * time.Millisecond
		for i, b := range visitBuckets {
			if d < b.upto {
				counts[i]++
				break
			}
		}
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site, Denominator: total}
	for i, b := range visitBuckets {
		if counts[i] == 0 {
			continue
		}
		res.Rows = append(res.Rows, Row{Label: b.label, Events: counts[i]})
	}
	return res, nil
}

// rankedVisits turns a page-keyed tally into the usual top-N answer.
//
// Sorted descending with the key breaking ties, exactly as MergeByKey does, so two widgets
// looking at the same data never disagree about the order of two equal rows.
func rankedVisits(req Request, counts map[string]int64, total int64) *Result {
	res := &Result{Widget: req.Widget, Site: req.Site, Denominator: total}
	for k, n := range counts {
		res.Rows = append(res.Rows, Row{Label: k, Events: n})
	}
	sort.Slice(res.Rows, func(i, j int) bool {
		if res.Rows[i].Events != res.Rows[j].Events {
			return res.Rows[i].Events > res.Rows[j].Events
		}
		return res.Rows[i].Label < res.Rows[j].Label
	})
	if req.Limit > 0 && len(res.Rows) > req.Limit {
		res.Rows = res.Rows[:req.Limit]
	}
	return res
}
