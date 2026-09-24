package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// The preceding period is the same NUMBER OF DAYS, not the same number of hours.
//
// The distinction only shows up across a DST change, which is why it is tested in a zone that
// observes one rather than in the zone this platform mostly runs in. Subtracting a duration
// would land the comparison an hour off midnight, and every day in the chart would then be
// compared against a window straddling two of the days it should be measured against — while
// still producing a chart that looked entirely reasonable.
func TestPreviousSpanCountsCalendarDays(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skip("tzdata unavailable")
	}

	// Europe/Berlin leaves summer time on 25 October 2026, so this week is 169 hours long.
	from := time.Date(2026, 10, 19, 0, 0, 0, 0, berlin)
	to := time.Date(2026, 10, 26, 0, 0, 0, 0, berlin)

	prev := previousSpan(span{from, to}, berlin)

	if !prev.End.Equal(from) {
		t.Errorf("the comparison period must end where the current one begins: %v vs %v", prev.End, from)
	}
	want := time.Date(2026, 10, 12, 0, 0, 0, 0, berlin)
	if !prev.Start.Equal(want) {
		t.Errorf("previous start = %v, want %v — seven calendar days, not 7*24 hours", prev.Start, want)
	}
	if h := prev.End.Sub(prev.Start).Hours(); h != 168 {
		t.Logf("the comparison week is %.0f hours to the current one's %.0f; both are seven days",
			h, to.Sub(from).Hours())
	}
}

// A range that is not day-aligned falls back to elapsed time, because there is no calendar
// boundary to count.
func TestPreviousSpanOfAPartialDayUsesElapsedTime(t *testing.T) {
	from := time.Date(2026, 9, 19, 9, 30, 0, 0, time.UTC)
	to := from.Add(4 * time.Hour)

	prev := previousSpan(span{from, to}, time.UTC)

	if !prev.End.Equal(from) || !prev.Start.Equal(from.Add(-4*time.Hour)) {
		t.Errorf("previous = [%v, %v), want the four hours immediately before", prev.Start, prev.End)
	}
}

func TestCompareAlignsBreakdownRowsByLabel(t *testing.T) {
	// Two days. Yesterday /home was busy and /gone existed; today /home is quieter, /new
	// exists and /gone does not.
	d1 := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 10 {
		rows = append(rows, pv(d1.Add(time.Duration(i)*time.Minute), "/home", "a"))
	}
	for i := range 4 {
		rows = append(rows, pv(d1.Add(time.Duration(i)*time.Minute), "/gone", "b"))
	}
	for i := range 6 {
		rows = append(rows, pv(d2.Add(time.Duration(i)*time.Minute), "/home", "c"))
	}
	for i := range 3 {
		rows = append(rows, pv(d2.Add(time.Duration(i)*time.Minute), "/new", "d"))
	}

	today := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: today, To: today.AddDate(0, 0, 1),
		Compare: ComparePrevious, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Previous) != len(res.Rows) {
		t.Fatalf("%d current rows against %d previous; they must align index for index",
			len(res.Rows), len(res.Previous))
	}
	for i := range res.Rows {
		if res.Rows[i].Label != res.Previous[i].Label {
			t.Errorf("row %d: %q against %q — the two series are not aligned",
				i, res.Rows[i].Label, res.Previous[i].Label)
		}
	}

	byLabel := map[string][2]int64{}
	for i, r := range res.Rows {
		byLabel[r.Label] = [2]int64{r.Events, res.Previous[i].Events}
	}
	if got := byLabel["/home"]; got != [2]int64{6, 10} {
		t.Errorf("/home = %v, want 6 now against 10 before", got)
	}
	if got := byLabel["/new"]; got != [2]int64{3, 0} {
		t.Errorf("/new = %v, want 3 now against 0 before — a page that is new reads as all-new", got)
	}
	if _, listed := byLabel["/gone"]; listed {
		t.Error("/gone is listed; a key with no traffic this period is not part of a top-N of this period")
	}

	// It is still counted in the total, which is the whole reason the total exists.
	if res.PreviousTotal == nil || res.Total == nil {
		t.Fatal("a comparison without both totals cannot produce a headline delta")
	}
	if res.PreviousTotal.Events != 14 {
		t.Errorf("previous total = %d events, want 14 — every key, not only the ones listed",
			res.PreviousTotal.Events)
	}
	if res.Total.Events != 9 {
		t.Errorf("current total = %d events, want 9", res.Total.Events)
	}
}

// The total must count the keys the limit cut off. A headline computed from the top N is
// wrong on exactly the sites with a long tail, and more wrong the bigger they get.
func TestTotalCountsBeyondTheLimit(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	// Twenty pages, one view each, asked for with a limit of three.
	for i := range 20 {
		rows = append(rows, pv(base.Add(time.Duration(i)*time.Minute),
			"/p"+string(rune('a'+i)), "v"+string(rune('a'+i))))
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want the 3 that were asked for", len(res.Rows))
	}
	if res.Total == nil || res.Total.Events != 20 {
		t.Errorf("total = %+v, want 20 events across every page", res.Total)
	}
}

// A time series aligns by position: the two periods share no dates, so aligning by label would
// align nothing at all and every comparison would read as zero.
func TestCompareAlignsATimeSeriesByPosition(t *testing.T) {
	d1 := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 5 {
		rows = append(rows, pv(d1.Add(time.Duration(i)*time.Minute), "/home", "a"))
	}
	for i := range 2 {
		rows = append(rows, pv(d2.Add(time.Duration(i)*time.Minute), "/home", "b"))
	}

	today := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetPageviewsOverTime, Site: "s.example",
		From: today, To: today.AddDate(0, 0, 1), Compare: ComparePrevious,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 1 || len(res.Previous) != 1 {
		t.Fatalf("got %d current and %d previous points, want 1 each", len(res.Rows), len(res.Previous))
	}
	if res.Rows[0].Events != 2 || res.Previous[0].Events != 5 {
		t.Errorf("got %d today against %d yesterday, want 2 against 5",
			res.Rows[0].Events, res.Previous[0].Events)
	}
	if res.Previous[0].Label != "" {
		t.Errorf("previous label = %q; carrying the old date would put two dates on one column",
			res.Previous[0].Label)
	}
}

func TestCompareIsRefusedWhereItWouldCostARawScan(t *testing.T) {
	e := New(t.TempDir())
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)

	for _, w := range []string{WidgetFunnel, WidgetRetention, WidgetExitPages, WidgetHeatmapPages} {
		_, err := e.Query(context.Background(), Request{
			Widget: w, Site: "s", Steps: []string{"a", "b"},
			From: now, To: now.AddDate(0, 0, 1), Compare: ComparePrevious,
		})
		if err == nil {
			t.Errorf("%s accepted a comparison; it reads raw events and a second pass is not free", w)
		}
	}

	// And a misspelling is refused rather than silently ignored, because a comparison that
	// quietly did not happen looks identical to a period with no data.
	if _, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s", From: now, To: now.AddDate(0, 0, 1), Compare: "prev",
	}); err == nil {
		t.Error("an unknown compare value was accepted")
	}
}

// A conversion rate needs a denominator that came from the same query as the numerator.
//
// Two queries and a division in the page is how two tiles end up disagreeing about how many
// visitors there were — one asked over a slightly different range, or with a limit that cut
// the tail off the total.
func TestEventWidgetsCarryTheSiteAudience(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	// Ten page views from three visitors.
	for i := range 10 {
		rows = append(rows, pv(base.Add(time.Duration(i)*time.Minute), "/p",
			[]string{"a", "b", "c"}[i%3]))
	}
	// Two conversions.
	for i := range 2 {
		r := pv(base.Add(time.Duration(20+i)*time.Minute), "/p", "a")
		r.Name = "form_submit"
		rows = append(rows, r)
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetEventTimeline, Site: "s.example", Event: "form_submit",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Audience == nil {
		t.Fatal("no audience, so a rate cannot be computed from this answer alone")
	}
	if res.Audience.Views != 10 {
		t.Errorf("audience views = %d, want 10 page views", res.Audience.Views)
	}
	if res.Audience.Visitors != 3 {
		t.Errorf("audience visitors = %d, want 3", res.Audience.Visitors)
	}
	// The conversions themselves are the rows, and they are NOT in the audience: the
	// denominator is the site's traffic, not everything that happened on it.
	var conversions int64
	for _, r := range res.Rows {
		conversions += r.Events
	}
	if conversions != 2 {
		t.Errorf("conversions = %d, want 2", conversions)
	}
}

// And nothing else pays for it: a breakdown of pages against each other needs no site-wide
// base, and fetching one would be a second rollup read on every tile of a dashboard.
func TestBreakdownsDoNotFetchAnAudience(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	res, err := New(fixture(t, []store.Row{pv(base, "/p", "a")})).Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Audience != nil {
		t.Errorf("topPages fetched an audience it does not use: %+v", res.Audience)
	}
}
