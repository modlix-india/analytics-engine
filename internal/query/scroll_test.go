package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// scrolled builds one scroll report, the way the beacon sends it when a view ends.
func scrolled(ts time.Time, path string, pct, viewH, docH, width int32) store.Row {
	return store.Row{
		TSServer: ts.UnixMilli(), Site: "s.example", Name: "$scroll",
		Path: path, Visitor: "v", Session: "s",
		ScrollPct: pct, ViewportH: viewH, DocH: docH, Viewport: width,
	}
}

func TestScrollDepthThresholdsAndMiddle(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	// Five views of one page: 10, 30, 60, 95, 100 percent.
	var rows []store.Row
	for i, pct := range []int32{10, 30, 60, 95, 100} {
		rows = append(rows, scrolled(base.Add(time.Duration(i)*time.Minute), "/long", pct, 900, 4000, 1440))
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetScrollDepth, Site: "s.example", Path: "/long",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]int64{"25%": 4, "50%": 3, "75%": 2, "90%": 2, "100%": 1}
	for _, r := range res.Rows {
		if want[r.Label] != r.Events {
			t.Errorf("%s reached by %d, want %d", r.Label, r.Events, want[r.Label])
		}
	}
	if len(res.Rows) != len(want) {
		t.Errorf("got %d thresholds, want %d", len(res.Rows), len(want))
	}

	if res.Scroll == nil {
		t.Fatal("no scroll summary")
	}
	if res.Scroll.Views != 5 || res.Denominator != 5 {
		t.Errorf("views = %d / denominator = %d, want 5", res.Scroll.Views, res.Denominator)
	}
	// 10+30+60+95+100 = 295 over 5.
	if res.Scroll.AveragePct != 59 {
		t.Errorf("average = %v, want 59", res.Scroll.AveragePct)
	}
	if res.Scroll.MedianPct != 60 {
		t.Errorf("median = %d, want 60", res.Scroll.MedianPct)
	}
	// 900 of 4000 is visible without scrolling.
	if res.Scroll.FoldPct != 22 {
		t.Errorf("fold = %d%%, want 22", res.Scroll.FoldPct)
	}
}

// The average and the median disagreeing IS the finding, so both are reported and a test says
// what the disagreement looks like: most people read the first screen, a few read everything.
func TestScrollAverageAndMedianCanDisagree(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 8 {
		rows = append(rows, scrolled(base.Add(time.Duration(i)*time.Minute), "/p", 15, 900, 6000, 1440))
	}
	for i := range 2 {
		rows = append(rows, scrolled(base.Add(time.Duration(10+i)*time.Minute), "/p", 100, 900, 6000, 1440))
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetScrollDepth, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Scroll.MedianPct != 15 {
		t.Errorf("median = %d, want 15 — the middle reader saw the first screen", res.Scroll.MedianPct)
	}
	if res.Scroll.AveragePct != 32 {
		t.Errorf("average = %v, want 32 — two thorough readers lift it well above the middle",
			res.Scroll.AveragePct)
	}
}

// A phone and a desktop are different pages, so their depths are not comparable and the band
// filter has to keep them apart — the same rule the heatmap applies to clicks.
func TestScrollBandsByViewportWidth(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		scrolled(base, "/p", 100, 800, 3000, 390),                    // phone, read fully
		scrolled(base.Add(time.Minute), "/p", 20, 900, 9000, 1440),   // desktop, barely started
		scrolled(base.Add(2*time.Minute), "/p", 30, 900, 9000, 1440), // desktop
	}

	e := New(fixture(t, rows))
	ask := func(viewport int32) *Result {
		res, err := e.Query(context.Background(), Request{
			Widget: WidgetScrollDepth, Site: "s.example", Path: "/p", Viewport: viewport,
			From: base.Add(-time.Hour), To: base.Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	if got := ask(390); got.Scroll.Views != 1 || got.Scroll.MedianPct != 100 {
		t.Errorf("phone band: %d views, median %d; want 1 and 100", got.Scroll.Views, got.Scroll.MedianPct)
	}
	if got := ask(1440); got.Scroll.Views != 2 || got.Scroll.MedianPct != 20 {
		t.Errorf("desktop band: %d views, median %d; want 2 and 20", got.Scroll.Views, got.Scroll.MedianPct)
	}
	if got := ask(0); got.Scroll.Views != 3 {
		t.Errorf("no band asked for: %d views, want all 3", got.Scroll.Views)
	}
}

// A page NAME beats the address here too, or an A/B test's two arms pile into one answer over
// two different layouts — exactly what the heatmap was keyed on the page to avoid.
func TestScrollPrefersThePageNameOverTheAddress(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	a := scrolled(base, "/", 90, 900, 3000, 1440)
	a.Page = "landingA"
	b := scrolled(base.Add(time.Minute), "/", 10, 900, 3000, 1440)
	b.Page = "landingB"

	res, err := New(fixture(t, []store.Row{a, b})).Query(context.Background(), Request{
		Widget: WidgetScrollDepth, Site: "s.example", Page: "landingA",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Scroll.Views != 1 || res.Scroll.MedianPct != 90 {
		t.Errorf("got %d views at median %d; want only landingA's one report at 90",
			res.Scroll.Views, res.Scroll.MedianPct)
	}
}

// No data is an empty answer with a zero count, not an error and not a set of zero rows that
// a chart would draw as five empty bars.
func TestScrollWithNoReportsSaysSo(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	res, err := New(fixture(t, []store.Row{pv(base, "/p", "v")})).Query(context.Background(), Request{
		Widget: WidgetScrollDepth, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 0 {
		t.Errorf("got %d rows for a page nobody reported scrolling", len(res.Rows))
	}
	if res.Scroll == nil || res.Scroll.Views != 0 {
		t.Errorf("scroll = %+v, want a summary saying zero views", res.Scroll)
	}
}

func TestScrollPagesRanksByReports(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 3 {
		rows = append(rows, scrolled(base.Add(time.Duration(i)*time.Minute), "/busy", 50, 900, 3000, 1440))
	}
	rows = append(rows, scrolled(base.Add(9*time.Minute), "/quiet", 50, 900, 3000, 1440))

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetScrollPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 2 || res.Rows[0].Label != "/busy" || res.Rows[0].Events != 3 {
		t.Errorf("got %+v, want /busy first with 3", res.Rows)
	}
}
