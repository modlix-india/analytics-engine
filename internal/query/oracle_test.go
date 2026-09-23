package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// The DuckDB oracle.
//
// Every widget answer is produced twice: once by this engine, reading pre-aggregated rollups
// through the hourly-bucket and boundary-correction machinery, and once by DuckDB reading the
// RAW Parquet with ordinary SQL. They must agree.
//
// That is worth far more than a hand-written expected value. The rollup pipeline has several
// places to be subtly wrong — bucketing, sketch merging, timezone boundaries, partial-hour
// correction — and a test asserting "topPages returns /a first" would pass through most of
// them. An independent implementation computing ground truth from the same bytes will not.
//
// DuckDB is invoked as a SUBPROCESS, deliberately. The Go binding needs cgo, which would
// forfeit the static-binary-on-distroless property the Dockerfile is explicit about wanting.
// This way DuckDB never enters go.mod, never enters the binary, and the test simply skips
// where the CLI is absent.

func duckdbAvailable(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("duckdb")
	if err != nil {
		t.Skip("duckdb CLI not installed; install it to run the oracle tests (brew install duckdb)")
	}
	return path
}

type oracleRow struct {
	Label    string  `json:"label"`
	Events   float64 `json:"events"`
	Visitors float64 `json:"visitors"`
}

func duckQuery(t *testing.T, sql string) []oracleRow {
	t.Helper()
	bin := duckdbAvailable(t)

	// SET TimeZone='UTC' is not optional and was not obvious. Our Parquet timestamps are
	// written with isAdjustedToUTC, so DuckDB reads them as instants and renders them in the
	// SESSION timezone — which on a machine in India is +5:30. A naive TIMESTAMP literal then
	// selects a window shifted by five and a half hours, and the oracle disagrees with the
	// engine for a reason that has nothing to do with either being wrong.
	//
	// That is exactly how this failed first: 61% of the expected events, and 8.5/14 hours of
	// overlap is 0.607. Pinning the session removes the ambiguity rather than compensating
	// for it.
	out, err := exec.Command(bin, "-json", "-c", "SET TimeZone='UTC'; "+sql).CombinedOutput()
	if err != nil {
		t.Fatalf("duckdb failed: %v\nsql:\n%s\noutput:\n%s", err, sql, out)
	}

	var rows []oracleRow
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("parsing duckdb output: %v\noutput:\n%s", err, out)
	}
	return rows
}

// oracleData generates events with enough variety that an aggregation bug shows up: many
// visitors, a long tail of paths, several referrers, spread across a day boundary in a
// half-hour timezone.
func oracleData(n int) []store.Row {
	rng := rand.New(rand.NewSource(42))

	// Straddles 18:30 UTC, which is midnight IST — the boundary the design exists for.
	start := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

	paths := []string{"/", "/pricing", "/features", "/docs", "/blog/a", "/blog/b", "/blog/c"}
	refs := []string{"google.com", "facebook.com", "t.co", "bing.com", ""}
	events := []string{"$pageview", "$pageview", "$pageview", "cta_clicked", "form_submitted"}

	// Every dimension a widget can break down by must actually VARY here. An earlier version
	// left device, browser, os and platform unset, so four of the breakdown oracle tests
	// compared an empty result against an empty result and passed without testing anything.
	// That is the failure mode of a table-driven test against generated data, and it is
	// silent — hence the non-vacuous assertion in the tests themselves.
	channels := []string{"direct", "organic", "social", "referral", "paid", ""}
	devices := []string{"desktop", "mobile", "tablet"}
	browsers := []string{"Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"}
	oses := []string{"Windows", "macOS", "Android", "iOS", "Linux"}
	platforms := []string{"web", "web", "web", "mobile_app"}
	versions := []string{"", "", "5.0.0", "5.1.0", "4.9.2"}
	countries := []string{"IN", "US", "GB", "DE", "SG", ""}

	rows := make([]store.Row, 0, n)
	for i := range n {
		v := fmt.Sprintf("v-%04d", rng.Intn(n/3+1))
		rows = append(rows, store.Row{
			// Spread over 14 hours so the range crosses the IST midnight at 18:30 UTC.
			TSServer:     start.Add(time.Duration(float64(i) / float64(n) * float64(14*time.Hour))).UnixMilli(),
			Site:         "s.example",
			Name:         events[rng.Intn(len(events))],
			Path:         paths[rng.Intn(len(paths))],
			Visitor:      v,
			Session:      "sess-" + v,
			ReferrerHost: refs[rng.Intn(len(refs))],
			Channel:      channels[rng.Intn(len(channels))],
			Device:       devices[rng.Intn(len(devices))],
			Browser:      browsers[rng.Intn(len(browsers))],
			OS:           oses[rng.Intn(len(oses))],
			Platform:     platforms[rng.Intn(len(platforms))],
			AppVersion:   versions[rng.Intn(len(versions))],
			Country:      countries[rng.Intn(len(countries))],
		})
	}
	return rows
}

// rawGlob is what DuckDB reads: every raw Parquet file, ignoring the rollup tier entirely.
func rawGlob(dir string) string {
	return filepath.ToSlash(filepath.Join(dir, "data", "*", "*", "*.parquet"))
}

func ts(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

// compare asserts event counts match exactly and visitor counts match within the sketch's
// error bound. The split matters: counts are exact in both implementations, so any difference
// is a real bug, while uniques come from HyperLogLog and a small difference is expected.
func compare(t *testing.T, widget string, got []Row, want []oracleRow, visitorTolerancePct float64) {
	t.Helper()

	// An empty result compared against an empty result is not evidence of anything, and it is
	// how four of these tests once passed while testing nothing at all. If the oracle found no
	// data, the fixture is wrong and the test must say so rather than agreeing cheerfully.
	if len(want) == 0 {
		t.Fatalf("%s: the DuckDB oracle returned no rows, so this comparison proves nothing — "+
			"the generated fixture does not populate the column being grouped by", widget)
	}

	if len(got) != len(want) {
		t.Errorf("%s: %d rows from the engine, %d from DuckDB\nengine: %+v\nduckdb: %+v",
			widget, len(got), len(want), got, want)
		return
	}

	for i := range got {
		if got[i].Label != want[i].Label {
			t.Errorf("%s row %d: label %q from engine, %q from DuckDB", widget, i, got[i].Label, want[i].Label)
			continue
		}
		if float64(got[i].Events) != want[i].Events {
			t.Errorf("%s row %q: %d events from engine, %.0f from DuckDB — counts must match exactly",
				widget, got[i].Label, got[i].Events, want[i].Events)
		}

		if want[i].Visitors > 0 {
			diff := math.Abs(float64(got[i].Visitors)-want[i].Visitors) / want[i].Visitors * 100
			if diff > visitorTolerancePct {
				t.Errorf("%s row %q: %d visitors from engine, %.0f actual — %.1f%% off, beyond the %.1f%% the sketch allows",
					widget, got[i].Label, got[i].Visitors, want[i].Visitors, diff, visitorTolerancePct)
			}
		}
	}
}

func TestOracleTopPages(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)

	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example", From: from, To: to, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT path AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = '$pageview'
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY path
		ORDER BY events DESC, label ASC
		LIMIT 10`, rawGlob(dir), ts(from), ts(to)))

	compare(t, "topPages", got.Rows, want, 6.0)
}

func TestOracleTopReferrers(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)

	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetTopReferrers, Site: "s.example", From: from, To: to, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Empty referrers are not a referrer and the engine does not roll them up, so the oracle
	// must exclude them too — otherwise the two disagree for a reason that is not a bug.
	want := duckQuery(t, fmt.Sprintf(`
		SELECT referrer_host AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = '$pageview' AND referrer_host <> ''
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY referrer_host
		ORDER BY events DESC, label ASC
		LIMIT 10`, rawGlob(dir), ts(from), ts(to)))

	compare(t, "topReferrers", got.Rows, want, 6.0)
}

// The one that matters most. The engine answers an IST day from hourly UTC buckets plus two
// corrected boundary hours; DuckDB answers it by converting every raw timestamp to IST and
// grouping. Two entirely different routes to the same number.
func TestOraclePageviewsOverTimeInIST(t *testing.T) {
	duckdbAvailable(t)
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	rows := oracleData(20_000)
	dir := fixture(t, rows)

	// Two IST days, so the 18:30 UTC boundary falls inside the range.
	from := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
	to := from.AddDate(0, 0, 2)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetPageviewsOverTime, Site: "s.example",
		From: from, To: to, Timezone: "Asia/Kolkata",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT strftime(ts_server AT TIME ZONE 'Asia/Kolkata', '%%Y-%%m-%%d') AS label,
		       count(*) AS events,
		       count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = '$pageview'
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY 1
		ORDER BY 1 ASC`, rawGlob(dir), ts(from), ts(to)))

	// The engine emits a row per day in range including empty ones; DuckDB only emits days
	// that have data. Drop the engine's empties so the two line up.
	var nonEmpty []Row
	for _, r := range got.Rows {
		if r.Events > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	if got.RawHoursScanned == 0 {
		t.Error("an IST range reported no boundary-hour corrections; the half-hour offset must need them")
	}
	compare(t, "pageviewsOverTime(IST)", nonEmpty, want, 6.0)
}

// A custom event, to prove the event filter is not hardcoded to pageviews anywhere.
func TestOracleCustomEventBreakdown(t *testing.T) {
	duckdbAvailable(t)

	rows := oracleData(20_000)
	dir := fixture(t, rows)

	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(14 * time.Hour)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetTopPages, Site: "s.example", Event: "cta_clicked",
		From: from, To: to, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckQuery(t, fmt.Sprintf(`
		SELECT path AS label, count(*) AS events, count(DISTINCT visitor) AS visitors
		FROM read_parquet('%s')
		WHERE event = 'cta_clicked'
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		GROUP BY path
		ORDER BY events DESC, label ASC
		LIMIT 10`, rawGlob(dir), ts(from), ts(to)))

	compare(t, "topPages(cta_clicked)", got.Rows, want, 8.0)
}
