package query

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/modlix-india/analytics-engine/internal/ingest"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// scrollThresholds are the depths reported, as percentages of the page.
//
// Read at query time from the raw maximum each view reached, not counted at capture time.
// That is what makes this list a presentation decision: changing it changes what yesterday's
// data says, which is the opposite of what happens when thresholds are baked in at ingest.
var scrollThresholds = []int32{25, 50, 75, 90, 100}

// Scroll is the part of a scroll answer that is not a row.
type Scroll struct {
	// Views is how many scroll reports were counted. It is the denominator of every row, and
	// it is NOT the page's view count: a view that ended without the browser firing pagehide
	// — a crashed tab, a killed app — reports nothing.
	Views int64 `json:"views"`

	// AveragePct and MedianPct are the maximum depth reached, averaged and at the midpoint.
	//
	// Both, because they disagree in a way that is itself the finding. A long page where most
	// people read the first screen and a few read all of it has a respectable average and a
	// low median, and the median is the one describing what happens.
	AveragePct float64 `json:"averagePct"`
	MedianPct  int32   `json:"medianPct"`

	// FoldPct is how much of the page is visible without scrolling: the median window height
	// over the median document height. It is what turns "40% of people got past halfway" into
	// something actionable — if the fold is at 30%, the call to action below it is the reason.
	FoldPct int32 `json:"foldPct"`
}

// scrollDepth reports how far down one page people got.
//
// Raw, and bounded the way the heatmap is: one site, one page, one range, asked for by a
// person looking at that page rather than by a dashboard refreshing twelve tiles. That bound
// is also why a median is affordable here and not in the visit widgets — the values for one
// page fit in memory, where a site-wide scan is partitioned precisely because they would not.
func (e *Engine) scrollDepth(ctx context.Context, req Request) (*Result, error) {
	if req.Path == "" && req.Page == "" {
		return nil, fmt.Errorf("query: a scroll report needs a page or a path")
	}

	rows, err := e.rawRows(ctx, req.Site, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	band := bandOf(req.Viewport)

	var depths []int32
	var folds []int32
	var sum int64

	for i := range rows {
		r := &rows[i]
		if r.Name != ingest.NameScroll || !matchesPage(r, req) {
			continue
		}
		ts := time.UnixMilli(r.TSServer)
		if ts.Before(req.From) || !ts.Before(req.To) {
			continue
		}
		// A report with no document height measured nothing. Recorded before the columns
		// existed, or sent by a client that could not read the layout.
		if r.DocH <= 0 {
			continue
		}
		if req.Variant != "" && r.Variant != req.Variant {
			continue
		}
		// Banded by layout, like the heatmap: a phone and a desktop are different pages, and
		// how far down one of them somebody got says nothing about the other.
		if band != 0 && bandOf(r.Viewport) != band {
			continue
		}

		depths = append(depths, r.ScrollPct)
		sum += int64(r.ScrollPct)
		if r.ViewportH > 0 {
			folds = append(folds, int32(int64(r.ViewportH)*100/int64(r.DocH)))
		}
	}

	res := &Result{Widget: req.Widget, Site: req.Site, Denominator: int64(len(depths))}
	sc := &Scroll{Views: int64(len(depths))}
	if len(depths) == 0 {
		res.Scroll = sc
		return res, nil
	}

	sc.AveragePct = float64(sum) / float64(len(depths))
	sc.MedianPct = medianOf(depths)
	sc.FoldPct = medianOf(folds)

	// One row per threshold: how many of those views got at least that far.
	for _, t := range scrollThresholds {
		var n int64
		for _, d := range depths {
			if d >= t {
				n++
			}
		}
		res.Rows = append(res.Rows, Row{Label: strconv.Itoa(int(t)) + "%", Events: n})
	}

	res.Scroll = sc
	return res, nil
}

// medianOf sorts a copy and takes the middle. The lower of the two middles on an even count,
// deliberately: it is a value somebody actually reached, where an average of two is not.
func medianOf(v []int32) int32 {
	if len(v) == 0 {
		return 0
	}
	s := make([]int32, len(v))
	copy(s, v)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[(len(s)-1)/2]
}

// scrollPages lists the pages with scroll reports at all, so a dashboard can offer something
// to look at without the reader having to know a path by heart.
//
// The same shape and the same page-beats-address rule as heatmapPages, for the same reason:
// the two are read side by side on one screen, and a page that appeared in one list and not
// the other would look like missing data rather than like a different question.
func (e *Engine) scrollPages(ctx context.Context, req Request) (*Result, error) {
	rows, err := e.rawRows(ctx, req.Site, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	counts := map[heatPageKey]int64{}
	for i := range rows {
		r := &rows[i]
		if r.Name != ingest.NameScroll || r.DocH <= 0 {
			continue
		}
		ts := time.UnixMilli(r.TSServer)
		if ts.Before(req.From) || !ts.Before(req.To) {
			continue
		}
		counts[heatPageKey{page: r.Page, path: r.Path}]++
	}

	merged := map[heatPageKey]int64{}
	for k, n := range counts {
		if k.page != "" {
			k.path = firstPathFor(counts, k.page)
		}
		merged[k] += n
	}

	res := &Result{Widget: req.Widget, Site: req.Site}
	for k, n := range merged {
		label := k.page
		if label == "" {
			label = k.path
		}
		res.Rows = append(res.Rows, Row{Label: label, Page: k.page, Path: k.path, Events: n})
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
	return res, nil
}

var _ = store.Row{}
