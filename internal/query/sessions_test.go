package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// view builds one page view of a named session, so a test reads as a sequence of visits
// rather than as a table of rows.
func view(ts time.Time, session, path string) store.Row {
	return store.Row{
		TSServer: ts.UnixMilli(), Site: "s.example", Name: "$pageview",
		Path: path, Visitor: "v-" + session, Session: session,
	}
}

func rowsByLabel(res *Result) map[string]int64 {
	out := map[string]int64{}
	for _, r := range res.Rows {
		out[r.Label] = r.Events
	}
	return out
}

func TestEntryAndExitPages(t *testing.T) {
	base := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

	rows := []store.Row{
		// one: /home then /pricing, and leaves from /pricing.
		view(base, "one", "/home"),
		view(base.Add(time.Minute), "one", "/pricing"),
		// two: same shape, so /pricing is the exit twice and /home the entry twice.
		view(base.Add(2*time.Minute), "two", "/home"),
		view(base.Add(3*time.Minute), "two", "/pricing"),
		// three: arrives on /pricing directly and leaves from there.
		view(base.Add(4*time.Minute), "three", "/pricing"),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetEntryPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := rowsByLabel(res); got["/home"] != 2 || got["/pricing"] != 1 {
		t.Errorf("entry pages = %v, want /home 2 and /pricing 1", got)
	}
	if res.Denominator != 3 {
		t.Errorf("entry denominator = %d, want 3 visits", res.Denominator)
	}

	res, err = New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetExitPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := rowsByLabel(res); got["/pricing"] != 3 || got["/home"] != 0 {
		t.Errorf("exit pages = %v, want /pricing 3 and no /home", got)
	}
	if res.Denominator != 3 {
		t.Errorf("exit denominator = %d, want 3 visits", res.Denominator)
	}
}

// The range edge is where this gets it wrong if it is going to.
//
// A visit still in progress at the end of the range has not left anywhere, and one already
// under way at the start did not arrive during it. Counting either would make whatever page
// people happen to be sitting on at midnight look like the site's biggest leak.
func TestVisitsStraddlingTheRangeAreNotCountedAsArrivalsOrExits(t *testing.T) {
	// The range is one hour, 10:00 to 11:00.
	from := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	rows := []store.Row{
		// inside: begins and ends within the range. The only visit that may count for both.
		view(from.Add(10*time.Minute), "inside", "/home"),
		view(from.Add(12*time.Minute), "inside", "/pricing"),

		// straddles the END: began inside, still going after `to`. An arrival, not an exit.
		view(to.Add(-5*time.Minute), "late", "/home"),
		view(to.Add(5*time.Minute), "late", "/contact"),

		// straddles the START: began before `from`, still going inside. Neither.
		view(from.Add(-5*time.Minute), "early", "/blog"),
		view(from.Add(5*time.Minute), "early", "/about"),
	}

	dir := fixture(t, rows)

	entries, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetEntryPages, Site: "s.example", From: from, To: to,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := rowsByLabel(entries)
	if got["/home"] != 2 {
		t.Errorf("entries: /home = %d, want 2 (the visit that began inside counts even though it ran on)", got["/home"])
	}
	if got["/blog"] != 0 || got["/about"] != 0 {
		t.Errorf("entries = %v, want nothing from the visit that began before the range", got)
	}

	exits, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetExitPages, Site: "s.example", From: from, To: to,
	})
	if err != nil {
		t.Fatal(err)
	}
	got = rowsByLabel(exits)
	if got["/pricing"] != 1 {
		t.Errorf("exits: /pricing = %d, want 1", got["/pricing"])
	}
	if got["/home"] != 0 {
		t.Errorf("exits: /home = %d, want 0 — that visit was still going at the end of the range", got["/home"])
	}
	if exits.Denominator != 1 {
		t.Errorf("exit denominator = %d, want 1 completed visit", exits.Denominator)
	}
}

// A page NAME beats the address, because routing serves two definitions at one URL and an
// exit-page table keyed on the address reports one row for two different pages.
func TestVisitPagesPreferTheNameOverTheAddress(t *testing.T) {
	base := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

	named := view(base, "one", "/")
	named.Page = "landingB"
	unnamed := view(base.Add(time.Minute), "two", "/")

	res, err := New(fixture(t, []store.Row{named, unnamed})).Query(context.Background(), Request{
		Widget: WidgetEntryPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	got := rowsByLabel(res)
	if got["landingB"] != 1 {
		t.Errorf("the named page was not reported under its name: %v", got)
	}
	if got["/"] != 1 {
		t.Errorf("the unnamed row should fall back to its address: %v", got)
	}
}

func TestPagesPerVisitAndVisitLength(t *testing.T) {
	base := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

	rows := []store.Row{
		// one page, no length at all: a visit of one event has no duration to report.
		view(base, "single", "/home"),

		// two pages, 20 seconds apart.
		view(base, "short", "/home"),
		view(base.Add(20*time.Second), "short", "/pricing"),

		// three pages over five minutes.
		view(base, "long", "/home"),
		view(base.Add(time.Minute), "long", "/pricing"),
		view(base.Add(5*time.Minute), "long", "/contact"),
	}

	dir := fixture(t, rows)

	pages, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetPagesPerVisit, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := rowsByLabel(pages)
	if got["1"] != 1 || got["2"] != 1 || got["3"] != 1 {
		t.Errorf("pages per visit = %v, want one visit each at 1, 2 and 3", got)
	}
	if pages.Denominator != 3 {
		t.Errorf("pages-per-visit denominator = %d, want 3", pages.Denominator)
	}

	length, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetVisitLength, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	got = rowsByLabel(length)
	if got["10s to 30s"] != 1 {
		t.Errorf("visit length = %v, want the 20-second visit in the 10s to 30s bucket", got)
	}
	if got["3m to 10m"] != 1 {
		t.Errorf("visit length = %v, want the 5-minute visit in the 3m to 10m bucket", got)
	}
	// The single-page visit is absent by design: one event is a moment, not a duration, and
	// reporting it as zero seconds would put a spike at the bottom of every site's histogram.
	if length.Denominator != 2 {
		t.Errorf("visit-length denominator = %d, want 2 — a one-event visit has no length", length.Denominator)
	}
}

// Partitioning must not change the answer. The same property the per-visitor analyses rely on,
// checked here because the key changed: sessions hash into partitions, not visitors.
func TestVisitResultIsIndependentOfPartitioning(t *testing.T) {
	base := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 200 {
		s := "sess-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		rows = append(rows,
			view(base.Add(time.Duration(i)*time.Second), s, "/home"),
			view(base.Add(time.Duration(i)*time.Second+time.Second), s, "/p"+string(rune('a'+i%7))))
	}

	e := New(fixture(t, rows))
	s := span{base.Add(-time.Hour), base.Add(time.Hour)}

	whole := map[string]int64{}
	if err := e.scanVisits(context.Background(), "s.example", s, 1, 0, func(v *visit) {
		views := v.pageViews()
		whole[views[len(views)-1].Page]++
	}); err != nil {
		t.Fatal(err)
	}

	split := map[string]int64{}
	if err := e.eachVisit(context.Background(), "s.example", s, func(v *visit) {
		views := v.pageViews()
		split[views[len(views)-1].Page]++
	}); err != nil {
		t.Fatal(err)
	}

	if len(whole) == 0 {
		t.Fatal("the unpartitioned scan found nothing, so this compares two empty maps")
	}
	for k, v := range whole {
		if split[k] != v {
			t.Errorf("%s: one partition says %d, sixteen say %d", k, v, split[k])
		}
	}
}
