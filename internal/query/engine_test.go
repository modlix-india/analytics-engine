package query

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/rollup"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// fixture writes raw and rollup tiers for the given events, exactly as compaction would.
func fixture(t *testing.T, rows []store.Row) string {
	t.Helper()
	dir := t.TempDir()

	byDate := map[string][]store.Row{}
	for _, r := range rows {
		d := time.UnixMilli(r.TSServer).UTC().Format("2006-01-02")
		byDate[d] = append(byDate[d], r)
	}

	for date, rs := range byDate {
		site := store.SafeSegment(rs[0].Site)
		raw := filepath.Join(dir, "data", site, date, "n1-000000000001.parquet")
		if err := store.WriteFile(raw, rs); err != nil {
			t.Fatal(err)
		}
		roll := filepath.Join(dir, "rollup", site, date, "n1-000000000001.parquet")
		if err := rollup.WriteFile(roll, rollup.Build(rs)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func pv(tsUTC time.Time, path, visitor string) store.Row {
	return store.Row{
		TSServer: tsUTC.UnixMilli(), Site: "s.example", Name: "$pageview",
		Path: path, Visitor: visitor, Session: "sess-" + visitor,
		ReferrerHost: "google.com", Channel: "organic", Country: "IN",
	}
}

func utc(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

// The test the whole hourly-rollup design exists for.
//
// An IST day runs 18:30 UTC to 18:30 UTC. These two events sit in the SAME UTC hour bucket
// (18:00–19:00) but on OPPOSITE sides of an IST day boundary. No rollup keyed to an hour — let
// alone to a day — can separate them, so getting this right proves the boundary correction
// from raw data actually works.
func TestISTDayBoundaryInsideASingleHour(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	before := utc(2026, 9, 19, 18, 15) // 23:45 IST on the 19th
	after := utc(2026, 9, 19, 18, 45)  // 00:15 IST on the 20th

	dir := fixture(t, []store.Row{
		pv(before, "/a", "v1"),
		pv(after, "/b", "v2"),
	})
	e := New(dir)

	// The IST day of 2026-09-19.
	istDayStart := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: istDayStart, To: istDayStart.AddDate(0, 0, 1),
		Timezone: "Asia/Kolkata",
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.RawHoursScanned != 2 {
		t.Errorf("RawHoursScanned = %d, want 2 — a half-hour offset needs both boundary hours", res.RawHoursScanned)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want exactly 1 (/a is in this IST day, /b is in the next):\n%+v", len(res.Rows), res.Rows)
	}
	if res.Rows[0].Label != "/a" {
		t.Errorf("row = %q, want /a — /b at 18:45 UTC belongs to the NEXT IST day", res.Rows[0].Label)
	}

	// And the following IST day must contain /b and nothing else.
	next := istDayStart.AddDate(0, 0, 1)
	res2, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: next, To: next.AddDate(0, 0, 1), Timezone: "Asia/Kolkata",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Rows) != 1 || res2.Rows[0].Label != "/b" {
		t.Errorf("next IST day = %+v, want exactly /b", res2.Rows)
	}
}

// The same data read as a UTC day must give a different — and also correct — answer. If the
// two agree, the timezone is being ignored.
func TestUTCAndISTDisagreeCorrectly(t *testing.T) {
	if _, err := time.LoadLocation("Asia/Kolkata"); err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	dir := fixture(t, []store.Row{
		pv(utc(2026, 9, 19, 18, 15), "/a", "v1"),
		pv(utc(2026, 9, 19, 18, 45), "/b", "v2"),
	})
	e := New(dir)

	utcDay := utc(2026, 9, 19, 0, 0)
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: utcDay, To: utcDay.Add(24 * time.Hour), Timezone: "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 2 {
		t.Errorf("the UTC day should contain both events, got %d rows: %+v", len(res.Rows), res.Rows)
	}
	if res.RawHoursScanned != 0 {
		t.Errorf("RawHoursScanned = %d for UTC; a whole-hour offset needs no correction", res.RawHoursScanned)
	}
}

func TestTopPagesRanksAndLimits(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	var rows []store.Row
	// /a three times, /b twice, /c once.
	for i, spec := range []struct {
		path string
		n    int
	}{{"/a", 3}, {"/b", 2}, {"/c", 1}} {
		for j := range spec.n {
			rows = append(rows, pv(base.Add(time.Duration(i*10+j)*time.Minute), spec.path,
				fmt.Sprintf("v%d-%d", i, j)))
		}
	}

	e := New(fixture(t, rows))
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(2 * time.Hour), Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 2 {
		t.Fatalf("Limit=2 returned %d rows", len(res.Rows))
	}
	if res.Rows[0].Label != "/a" || res.Rows[0].Events != 3 {
		t.Errorf("top row = %+v, want /a with 3 events", res.Rows[0])
	}
	if res.Rows[1].Label != "/b" || res.Rows[1].Events != 2 {
		t.Errorf("second row = %+v, want /b with 2 events", res.Rows[1])
	}
}

func TestPageviewsOverTimeBucketsByLocalDay(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	// One event on each of three IST days, placed near the boundary so a UTC-keyed
	// implementation would put them on the wrong days.
	rows := []store.Row{
		pv(utc(2026, 9, 18, 19, 0), "/a", "v1"), // IST 19th, 00:30
		pv(utc(2026, 9, 19, 19, 0), "/a", "v2"), // IST 20th, 00:30
		pv(utc(2026, 9, 20, 19, 0), "/a", "v3"), // IST 21st, 00:30
	}
	e := New(fixture(t, rows))

	start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetPageviewsOverTime, Site: "s.example",
		From: start, To: start.AddDate(0, 0, 3), Timezone: "Asia/Kolkata",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("got %d day buckets, want 3: %+v", len(res.Rows), res.Rows)
	}
	want := []string{"2026-09-19", "2026-09-20", "2026-09-21"}
	for i, w := range want {
		if res.Rows[i].Label != w {
			t.Errorf("bucket %d = %q, want %q", i, res.Rows[i].Label, w)
		}
		if res.Rows[i].Events != 1 {
			t.Errorf("bucket %s has %d events, want 1", res.Rows[i].Label, res.Rows[i].Events)
		}
	}
}

func TestTopReferrers(t *testing.T) {
	base := utc(2026, 9, 19, 10, 0)
	rows := []store.Row{pv(base, "/a", "v1"), pv(base.Add(time.Minute), "/b", "v2")}
	rows[1].ReferrerHost = "facebook.com"

	e := New(fixture(t, rows))
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetTopReferrers, Site: "s.example",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d referrers, want 2: %+v", len(res.Rows), res.Rows)
	}
}

func TestUnknownWidgetAndMissingSite(t *testing.T) {
	e := New(t.TempDir())
	now := time.Now()

	if _, err := e.Query(context.Background(), Request{Widget: "dropTable", Site: "s", From: now, To: now.Add(time.Hour)}); err == nil {
		t.Error("an unknown widget was accepted; the fixed set is the read security model")
	}
	if _, err := e.Query(context.Background(), Request{Widget: WidgetTopPages, From: now, To: now.Add(time.Hour)}); err == nil {
		t.Error("a query without a site was accepted")
	}
}

// A site with no data is an ordinary answer, not an error: it is what every range edge looks
// like, and what a brand new site looks like for its first hour.
func TestEmptySiteReturnsNoRows(t *testing.T) {
	e := New(t.TempDir())
	now := time.Now()

	res, err := e.Query(context.Background(), Request{Widget: WidgetTopPages, Site: "nobody.example", From: now.Add(-time.Hour), To: now})
	if err != nil {
		t.Fatalf("querying an empty site errored: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("got %d rows for a site with no data", len(res.Rows))
	}
}

// A request that names no zone must still mean something definite, and that something is the
// engine's configured default rather than the machine's. Modlix sets it to Asia/Kolkata; the
// engine's own default stays UTC so a standalone deployment cannot inherit an opinion from
// whichever host it happens to run on.
func TestDefaultTimezoneAppliesWhenTheRequestNamesNone(t *testing.T) {
	if _, err := time.LoadLocation("Asia/Kolkata"); err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	// 23:45 IST on the 19th, which is still the 19th in UTC — so the two zones put this
	// event on different calendar days and the answer reveals which one was used.
	dir := fixture(t, []store.Row{pv(utc(2026, 9, 19, 18, 15), "/a", "v1")})

	from := utc(2026, 9, 19, 0, 0)
	to := utc(2026, 9, 21, 0, 0)

	e := New(dir)
	e.DefaultTimezone = "Asia/Kolkata"
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetPageviewsOverTime, Site: "s.example", From: from, To: to,
	})
	if err != nil {
		t.Fatal(err)
	}

	var day string
	for _, r := range res.Rows {
		if r.Events > 0 {
			day = r.Label
		}
	}
	if day != "2026-09-19" {
		t.Errorf("the event landed on %q with the IST default; want 2026-09-19", day)
	}

	// An explicit zone on the request still wins over the default.
	res2, err := e.Query(context.Background(), Request{
		Widget: WidgetPageviewsOverTime, Site: "s.example", From: from, To: to,
		Timezone: "Pacific/Kiritimati", // UTC+14, so 18:15 UTC is already the 20th
	})
	if err != nil {
		t.Fatal(err)
	}
	day = ""
	for _, r := range res2.Rows {
		if r.Events > 0 {
			day = r.Label
		}
	}
	if day != "2026-09-20" {
		t.Errorf("an explicit zone put the event on %q; want 2026-09-20, so the request must override the default", day)
	}
}

// The daily-visitor flag has to key on the UTC rotation, not on the reporting day, and it has
// to be exclusive at the upper end: a request for exactly one UTC day is not two days.
func TestVisitorsDailyFlagsARangeThatCrossesTheSaltRotation(t *testing.T) {
	utc := func(y int, m time.Month, d, h int) time.Time {
		return time.Date(y, m, d, h, 0, 0, 0, time.UTC)
	}

	cases := []struct {
		name     string
		from, to time.Time
		want     bool
	}{
		{"one whole UTC day", utc(2026, 9, 19, 0), utc(2026, 9, 20, 0), false},
		{"part of one day", utc(2026, 9, 19, 6), utc(2026, 9, 19, 18), false},
		{"one hour past midnight", utc(2026, 9, 19, 23), utc(2026, 9, 20, 1), true},
		{"a week", utc(2026, 9, 13, 0), utc(2026, 9, 20, 0), true},
		// An IST day runs 18:30 UTC to 18:30 UTC, so a single local day crosses a rotation.
		// This is the case a reporting-zone comparison would have called safe.
		{"one IST day", utc(2026, 9, 18, 18).Add(30 * time.Minute), utc(2026, 9, 19, 18).Add(30 * time.Minute), true},
	}

	for _, c := range cases {
		if got := spansSaltRotation(c.from, c.to); got != c.want {
			t.Errorf("%s: spansSaltRotation = %v, want %v", c.name, got, c.want)
		}
	}
}
