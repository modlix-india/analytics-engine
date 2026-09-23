package query

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modlix-india/analytics-engine/internal/ingest"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// HeatmapCell is one square of the grid, and how many clicks landed in it.
//
// X is in ten-thousandths of the viewport width and Y in pixels from the top of the document —
// the same units the click was recorded in, so the caller scales them to whatever width it is
// drawing at rather than trusting a pixel we chose.
type HeatmapCell struct {
	X     int32 `json:"x"`
	Y     int32 `json:"y"`
	Count int64 `json:"count"`
}

// Heatmap is the answer for one page.
type Heatmap struct {
	Cells []HeatmapCell `json:"cells"`

	// Max is the busiest cell's count. The renderer needs it to scale its colour ramp, and
	// computing it here means every client scales the same way rather than each inventing a
	// normalisation and disagreeing about what "hot" looks like.
	Max int64 `json:"max"`

	// Clicks is every click in range, cells or not — so a page with clicks that all landed
	// outside the rendered area is distinguishable from a page with none.
	Clicks int64 `json:"clicks"`

	// CellX and CellY are the grid's resolution, in the same units as the cells.
	CellX int32 `json:"cellX"`
	CellY int32 `json:"cellY"`

	// Variants that appear in this page's clicks, whether or not one was asked for. A
	// dashboard cannot offer a per-variant view without knowing which exist, and asking the
	// experiment definition would tell it which were CONFIGURED rather than which were seen.
	Variants []string `json:"variants,omitempty"`

	// Viewports actually observed, bucketed the way the query buckets them. A map drawn at a
	// width nobody browsed at is a picture of nothing.
	Viewports []int32 `json:"viewports,omitempty"`
}

const (
	// A 1% × 20px grid. Horizontal in proportion because that is how the coordinate is
	// stored; vertical in pixels because a page is as tall as it is.
	//
	// Fine enough that two buttons side by side are separate blobs, coarse enough that a
	// thousand clicks make a shape rather than a hundred lonely dots.
	heatCellX int32 = 100
	heatCellY int32 = 20

	// Clicks are only comparable within a band of similar widths: the same layout. These are
	// the usual breakpoints — phone, tablet, desktop — and a click is filed under the band
	// its viewport fell in.
	//
	// Every band is a real, non-zero number, and the widest is NOT zero. It was, for about
	// ten minutes, and "desktop" and "no band asked for" were then the same value: asking for
	// the desktop map returned every click at every width, which looks like a working
	// heatmap. Zero means "no band asked for" and nothing else.
	bandPhone   int32 = 600
	bandTablet  int32 = 1024
	bandDesktop int32 = 9999
)

// bandOf files a viewport width under the layout it was almost certainly showing.
func bandOf(vw int32) int32 {
	switch {
	case vw <= 0:
		return 0 // unmeasured, which is not a band
	case vw < bandPhone:
		return bandPhone
	case vw < bandTablet:
		return bandTablet
	default:
		return bandDesktop
	}
}

// matchesPage decides whether a row belongs to the page being asked about.
//
// **The page name wins over the address, when there is one.** An address is not a page:
// page routing serves two different definitions at one URL, so keying a heatmap on the
// path piles both arms of an A/B test into one grid over two different layouts, and the
// result is a picture of neither. The `page` column has always been recorded and until now
// nothing read it.
//
// It is also the right way round for the opposite case: one page served at several
// addresses — a campaign landing URL and the page it stands for — is one layout, and its
// clicks belong together.
//
// The path is still honoured on its own, because rows recorded before the application
// reported the page it actually served carry no usable name, and those are the only thing
// a heatmap of yesterday has.
func matchesPage(r *store.Row, req Request) bool {
	if req.Page != "" {
		return r.Page == req.Page
	}
	return r.Path == req.Path
}

// heatmap aggregates click positions for one page into a grid.
//
// Raw, not rollups: a rollup has thrown the coordinates away, and rolling clicks up by cell at
// compaction would fix the grid resolution forever at whatever we guessed today. The scan is
// bounded — one site, one path, one date range — and a heatmap is asked for one page at a time
// by a person looking at it, not by a dashboard refreshing twelve tiles.
func (e *Engine) heatmap(ctx context.Context, req Request) (*Result, error) {
	if req.Path == "" && req.Page == "" {
		return nil, fmt.Errorf("query: a heatmap needs a page or a path")
	}

	rows, err := e.rawRows(ctx, req.Site, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	band := bandOf(req.Viewport)

	cells := map[[2]int32]int64{}
	variants := map[string]bool{}
	viewports := map[int32]bool{}
	var clicks int64

	for i := range rows {
		r := &rows[i]
		if r.Name != ingest.NameClick || !matchesPage(r, req) {
			continue
		}
		ts := time.UnixMilli(r.TSServer)
		if ts.Before(req.From) || !ts.Before(req.To) {
			continue
		}

		// Recorded before the columns existed, or sent by a client that did not measure.
		if r.Viewport <= 0 {
			continue
		}

		variants[r.Variant] = true
		viewports[bandOf(r.Viewport)] = true

		if req.Variant != "" && r.Variant != req.Variant {
			continue
		}
		if band != 0 && bandOf(r.Viewport) != band {
			continue
		}
		clicks++

		key := [2]int32{(r.ClickX / heatCellX) * heatCellX, (r.ClickY / heatCellY) * heatCellY}
		cells[key]++
	}

	out := &Heatmap{CellX: heatCellX, CellY: heatCellY, Clicks: clicks}
	for k, n := range cells {
		out.Cells = append(out.Cells, HeatmapCell{X: k[0], Y: k[1], Count: n})
		if n > out.Max {
			out.Max = n
		}
	}
	// Sorted so a response is byte-identical for the same data — a cached answer and a fresh
	// one have to look the same, and a map's order is not an order.
	sort.Slice(out.Cells, func(i, j int) bool {
		if out.Cells[i].Y != out.Cells[j].Y {
			return out.Cells[i].Y < out.Cells[j].Y
		}
		return out.Cells[i].X < out.Cells[j].X
	})

	for v := range variants {
		if v != "" {
			out.Variants = append(out.Variants, v)
		}
	}
	sort.Strings(out.Variants)
	for v := range viewports {
		out.Viewports = append(out.Viewports, v)
	}
	sort.Slice(out.Viewports, func(i, j int) bool { return out.Viewports[i] < out.Viewports[j] })

	return &Result{Widget: req.Widget, Site: req.Site, Heatmap: out}, nil
}

// heatPageKey is one entry in the heatmap page list: which page, at which address.
type heatPageKey struct{ page, path string }

// heatmapPages lists what has clicks, so a dashboard can offer something to look at without
// the reader having to know a path by heart.
//
// One row per (page, path) pair rather than per path. Two arms of an A/B test share an
// address, so a list keyed on the address cannot even OFFER them separately — it names one
// entry and draws both arms over it. The pair is also what the caller needs back: the page
// says which clicks, and the path says where the page lives.
func (e *Engine) heatmapPages(ctx context.Context, req Request) (*Result, error) {
	rows, err := e.rawRows(ctx, req.Site, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	counts := map[heatPageKey]int64{}
	for i := range rows {
		r := &rows[i]
		if r.Name != ingest.NameClick || r.Viewport <= 0 {
			continue
		}
		ts := time.UnixMilli(r.TSServer)
		if ts.Before(req.From) || !ts.Before(req.To) {
			continue
		}
		counts[heatPageKey{page: r.Page, path: r.Path}]++
	}

	// A page seen at several addresses is one entry: the clicks are one layout, and
	// splitting them would hand the reader two half-maps of the same page. Only rows with
	// no page name at all stay keyed by address, which is what an old row is.
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

// firstPathFor picks one address to show for a page seen at several, deterministically: the
// answer has to be byte-identical between a cached response and a fresh one, and a map has no
// order. Lowest string wins, which is arbitrary and stable, and it is only ever used as the
// address to open — the clicks are already keyed by the page.
func firstPathFor(counts map[heatPageKey]int64, page string) string {
	best := ""
	for k := range counts {
		if k.page != page {
			continue
		}
		if best == "" || k.path < best {
			best = k.path
		}
	}
	return best
}

var _ = store.Row{}
