package query

import (
	"fmt"
	"testing"
	"time"
)

// Every breakdown widget, checked against DuckDB.
//
// They share one code path over different dimensions, so a table is the honest way to test
// them: the risk is not that one is individually wrong but that the dimension mapping is,
// and a table catches a transposed pair where four separate hand-written tests would not.
func TestOracleAllBreakdowns(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)
	eng := New(dir)

	for _, tc := range []struct{ widget, column string }{
		{WidgetTopPages, "path"},
		{WidgetTopReferrers, "referrer_host"},
		{WidgetChannelBreakdown, "channel"},
		{WidgetDeviceBreakdown, "device"},
		{WidgetBrowserBreakdown, "browser"},
		{WidgetOSBreakdown, "os"},
		{WidgetGeoBreakdown, "country"},
		{WidgetPlatformBreakdown, "platform"},
	} {
		t.Run(tc.widget, func(t *testing.T) {
			got, err := eng.Query(Request{
				Widget: tc.widget, Site: "s.example", From: from, To: to, Limit: 20,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Empty values are not rolled up — an unknown country is not a country — so the
			// oracle must exclude them too, or the two disagree for a reason that is not a bug.
			want := duckQuery(t, fmt.Sprintf(`
				SELECT %s AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
				FROM read_parquet('%s')
				WHERE event = '$pageview' AND %s <> ''
				  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
				GROUP BY 1 ORDER BY 2 DESC, 1 ASC LIMIT 20`,
				tc.column, rawGlob(dir), tc.column, ts(from), ts(to)))

			compare(t, tc.widget, got.Rows, want, 6.0)
		})
	}
}

// topEvents ranks event NAMES rather than the values of a dimension, so it exercises a
// different merge key from every other widget.
func TestOracleTopEvents(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(Request{
		Widget: WidgetTopEvents, Site: "s.example", From: from, To: to, Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT event AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY 1 ORDER BY 2 DESC, 1 ASC LIMIT 20`, rawGlob(dir), ts(from), ts(to)))

	compare(t, "topEvents", got.Rows, want, 6.0)
}

func TestOracleEventTimeline(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(Request{
		Widget: WidgetEventTimeline, Site: "s.example", Event: "cta_clicked",
		From: from, To: to, Timezone: "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT strftime(ts_server, '%%Y-%%m-%%d') AS label,
		       count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = 'cta_clicked'
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY 1 ORDER BY 1 ASC`, rawGlob(dir), ts(from), ts(to)))

	var nonEmpty []Row
	for _, r := range got.Rows {
		if r.Events > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}
	compare(t, "eventTimeline", nonEmpty, want, 6.0)
}

func TestOracleBreakdownByProperty(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(Request{
		Widget: WidgetBreakdownByProperty, Site: "s.example", Property: "browser",
		From: from, To: to, Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT browser AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = '$pageview' AND browser <> ''
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY 1 ORDER BY 2 DESC, 1 ASC LIMIT 20`, rawGlob(dir), ts(from), ts(to)))

	compare(t, "breakdownByProperty(browser)", got.Rows, want, 6.0)
}

// The allow-list is what stops "fixed widget API" drifting back into "arbitrary query". A
// caller must not be able to name a column the engine never intended to expose.
func TestBreakdownByPropertyRefusesUnknownDimensions(t *testing.T) {
	e := New(t.TempDir())
	now := time.Now()

	for _, prop := range []string{"visitor", "session", "props", "site", "", "../etc"} {
		_, err := e.Query(Request{
			Widget: WidgetBreakdownByProperty, Site: "s.example", Property: prop,
			From: now.Add(-time.Hour), To: now,
		})
		if err == nil {
			t.Errorf("breakdownByProperty accepted %q; only the allow-listed dimensions are valid", prop)
		}
	}

	// And an allow-listed one is accepted.
	if _, err := e.Query(Request{
		Widget: WidgetBreakdownByProperty, Site: "s.example", Property: "country",
		From: now.Add(-time.Hour), To: now,
	}); err != nil {
		t.Errorf("breakdownByProperty rejected an allow-listed dimension: %v", err)
	}
}
