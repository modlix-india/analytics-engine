package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

func click(ts time.Time, path string, x, y, vw int32, variant string) store.Row {
	return store.Row{
		TSServer: ts.UnixMilli(), Site: "s.example", Name: "$click",
		Path: path, Visitor: "v1", Session: "sess",
		ClickX: x, ClickY: y, Viewport: vw, Variant: variant,
	}
}

// clickOn is a click that also carries the page the application actually served, which is
// what tells two A/B arms at one address apart.
func clickOn(ts time.Time, path, page string, x, y, vw int32) store.Row {
	r := click(ts, path, x, y, vw, "")
	r.Page = page
	return r
}

func heatmapOf(t *testing.T, dir string, req Request) *Heatmap {
	t.Helper()
	req.Widget = WidgetHeatmap
	req.Site = "s.example"
	res, err := New(dir).Query(context.Background(), req)
	if err != nil {
		t.Fatalf("Query() = %v", err)
	}
	if res.Heatmap == nil {
		t.Fatal("no heatmap in the result")
	}
	return res.Heatmap
}

func TestHeatmapBucketsClicksIntoCells(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		// Three clicks inside one cell: x within 100 ten-thousandths of each other,
		// y within 20px. They must come back as one cell of three, not three cells.
		click(at, "/pricing", 5010, 401, 1440, ""),
		click(at, "/pricing", 5090, 419, 1440, ""),
		click(at, "/pricing", 5000, 400, 1440, ""),
		// A fourth, plainly elsewhere.
		click(at, "/pricing", 2000, 900, 1440, ""),
		// Another page entirely, which must not appear.
		click(at, "/features", 5000, 400, 1440, ""),
	})

	hm := heatmapOf(t, dir, Request{
		Path: "/pricing", From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})

	if hm.Clicks != 4 {
		t.Errorf("counted %d clicks on /pricing, want 4", hm.Clicks)
	}
	if len(hm.Cells) != 2 {
		t.Fatalf("got %d cells, want 2: %+v", len(hm.Cells), hm.Cells)
	}
	if hm.Max != 3 {
		t.Errorf("Max = %d, want 3 — the renderer scales its colours by this", hm.Max)
	}

	// Cells are sorted, so the busy one is the lower y.
	if hm.Cells[0].Y != 400 || hm.Cells[0].X != 5000 || hm.Cells[0].Count != 3 {
		t.Errorf("first cell = %+v, want x=5000 y=400 count=3", hm.Cells[0])
	}
}

// A page with an experiment running has one map per arm, and the arms must not be averaged
// together — the whole reason to look is that they differ.
func TestHeatmapSeparatesExperimentArms(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		click(at, "/pricing", 5000, 400, 1440, "a"),
		click(at, "/pricing", 5000, 400, 1440, "a"),
		click(at, "/pricing", 1000, 400, 1440, "b"),
	})
	window := Request{Path: "/pricing", From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0)}

	all := heatmapOf(t, dir, window)
	if all.Clicks != 3 {
		t.Errorf("every arm together = %d clicks, want 3", all.Clicks)
	}
	if len(all.Variants) != 2 || all.Variants[0] != "a" || all.Variants[1] != "b" {
		t.Errorf("Variants = %v, want [a b] — a dashboard cannot offer arms it is not told about",
			all.Variants)
	}

	a := window
	a.Variant = "a"
	only := heatmapOf(t, dir, a)
	if only.Clicks != 2 {
		t.Errorf("arm a = %d clicks, want 2", only.Clicks)
	}
	if len(only.Cells) != 1 || only.Cells[0].X != 5000 {
		t.Errorf("arm a cells = %+v, want only the one at x=5000", only.Cells)
	}
}

// Clicks are only comparable within a band of similar widths, because a band is a layout.
// Averaging a phone and a desktop produces a picture of neither.
func TestHeatmapBandsByViewport(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		click(at, "/pricing", 5000, 400, 390, ""),  // phone
		click(at, "/pricing", 5000, 400, 820, ""),  // tablet
		click(at, "/pricing", 5000, 400, 1440, ""), // desktop
		click(at, "/pricing", 5000, 400, 1920, ""), // desktop
	})
	window := Request{Path: "/pricing", From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0)}

	if got := heatmapOf(t, dir, window); got.Clicks != 4 {
		t.Errorf("no band asked for = %d clicks, want all 4", got.Clicks)
	}

	phone := window
	phone.Viewport = 390
	if got := heatmapOf(t, dir, phone); got.Clicks != 1 {
		t.Errorf("phone band = %d clicks, want 1", got.Clicks)
	}

	desktop := window
	desktop.Viewport = 1440
	got := heatmapOf(t, dir, desktop)
	if got.Clicks != 2 {
		t.Errorf("desktop band = %d clicks, want 2 (1440 and 1920 are the same layout)", got.Clicks)
	}
	if len(got.Viewports) != 3 {
		t.Errorf("Viewports = %v, want all three bands — a map drawn at a width nobody used is "+
			"a picture of nothing, so the caller has to be told which exist", got.Viewports)
	}
}

// Rows written before the click columns existed carry a zero viewport. They are not clicks at
// the top-left corner; they are clicks nobody measured, and drawing them would put a blob on
// every page that has ever been redesigned.
func TestHeatmapIgnoresUnmeasuredClicks(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		click(at, "/pricing", 0, 0, 0, ""),
		click(at, "/pricing", 5000, 400, 1440, ""),
	})

	hm := heatmapOf(t, dir, Request{
		Path: "/pricing", From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})
	if hm.Clicks != 1 || len(hm.Cells) != 1 {
		t.Errorf("got %d clicks in %d cells, want 1 in 1", hm.Clicks, len(hm.Cells))
	}
}

func TestHeatmapNeedsAPath(t *testing.T) {
	dir := fixture(t, []store.Row{click(utc(2026, 9, 20, 10, 0), "/p", 1, 1, 1440, "")})
	_, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetHeatmap, Site: "s.example",
		From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})
	if err == nil {
		t.Fatal("a heatmap with no path was accepted; it would have scanned every page at once")
	}
}

// The bug this whole change exists for. Page routing serves two different pages at one
// address, so a heatmap keyed on the address draws both of them over one of their two
// layouts — and it looks like a working heatmap, which is what makes it expensive.
func TestHeatmapSeparatesTwoPagesAtOneAddress(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	window := Request{From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0)}
	dir := fixture(t, []store.Row{
		clickOn(at, "/", "home", 1000, 100, 1440),
		clickOn(at, "/", "home", 1000, 100, 1440),
		clickOn(at, "/", "homeTwo", 8000, 900, 1440),
	})

	byPage := window
	byPage.Page = "home"
	if got := heatmapOf(t, dir, byPage); got.Clicks != 2 {
		t.Errorf("home has %d clicks, want 2 — the arms are being averaged together", got.Clicks)
	}

	other := window
	other.Page = "homeTwo"
	if got := heatmapOf(t, dir, other); got.Clicks != 1 {
		t.Errorf("homeTwo has %d clicks, want 1", got.Clicks)
	}

	// And the address on its own still answers for everything at it, which is what a row
	// recorded before the application reported its page name looks like.
	byPath := window
	byPath.Path = "/"
	if got := heatmapOf(t, dir, byPath); got.Clicks != 3 {
		t.Errorf("the address has %d clicks, want all 3", got.Clicks)
	}
}

// One page reached at two addresses — a campaign landing URL and the page it stands for —
// is one layout, so its clicks belong in one map rather than two half-maps.
func TestHeatmapGathersOnePageAcrossAddresses(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		clickOn(at, "/", "home", 1000, 100, 1440),
		clickOn(at, "/spring-sale", "home", 1000, 100, 1440),
	})

	hm := heatmapOf(t, dir, Request{
		Page: "home", From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})
	if hm.Clicks != 2 {
		t.Errorf("counted %d clicks for home, want 2 across both addresses", hm.Clicks)
	}
}

// The picker has to be able to OFFER the arms separately, or there is no way to ask for one.
func TestHeatmapPagesListsPagesNotAddresses(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		clickOn(at, "/", "home", 1, 1, 1440),
		clickOn(at, "/", "home", 2, 2, 1440),
		clickOn(at, "/", "homeTwo", 3, 3, 1440),
		// No page name at all: an old row, which can only be offered by its address.
		click(at, "/legacy", 1, 1, 1440, ""),
	})

	res, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetHeatmapPages, Site: "s.example",
		From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (home, homeTwo, /legacy): %+v", len(res.Rows), res.Rows)
	}
	if res.Rows[0].Label != "home" || res.Rows[0].Page != "home" || res.Rows[0].Events != 2 {
		t.Errorf("first row = %+v, want home/home with 2", res.Rows[0])
	}
	// The address travels with the page, because the caller has to open it somewhere.
	if res.Rows[0].Path != "/" {
		t.Errorf("home has path %q, want / — the caller needs an address to frame", res.Rows[0].Path)
	}
	var legacy *Row
	for i := range res.Rows {
		if res.Rows[i].Label == "/legacy" {
			legacy = &res.Rows[i]
		}
	}
	if legacy == nil || legacy.Page != "" {
		t.Errorf("a row with no page name must still be offered by its address: %+v", res.Rows)
	}
}

// The page list is how somebody finds a page to look at without knowing one by heart.
func TestHeatmapPagesRanksByClicks(t *testing.T) {
	at := utc(2026, 9, 20, 10, 0)
	dir := fixture(t, []store.Row{
		click(at, "/pricing", 1, 1, 1440, ""),
		click(at, "/pricing", 2, 2, 1440, ""),
		click(at, "/features", 1, 1, 1440, ""),
		pv(at, "/nothing-clicked", "v9"),
	})

	res, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetHeatmapPages, Site: "s.example",
		From: utc(2026, 9, 20, 0, 0), To: utc(2026, 9, 21, 0, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d pages, want 2 — a page with views but no clicks has no heatmap: %+v",
			len(res.Rows), res.Rows)
	}
	if res.Rows[0].Label != "/pricing" || res.Rows[0].Events != 2 {
		t.Errorf("first row = %+v, want /pricing with 2", res.Rows[0])
	}
}
