package rollup

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

func at(h int, m int) int64 {
	return time.Date(2026, 9, 19, h, m, 0, 0, time.UTC).UnixMilli()
}

func row(ts int64, name, path, visitor, session string) store.Row {
	return store.Row{
		TSServer: ts, Site: "s.example", Name: name, Path: path,
		Visitor: visitor, Session: session, Channel: "direct", Country: "IN",
	}
}

func find(rows []Row, event, dim, key string) *Row {
	for i := range rows {
		if rows[i].Event == event && rows[i].Dim == dim && rows[i].Key == key {
			return &rows[i]
		}
	}
	return nil
}

func TestBuildCounts(t *testing.T) {
	rolled := Build([]store.Row{
		row(at(10, 0), "$pageview", "/a", "v1", "s1"),
		row(at(10, 30), "$pageview", "/a", "v2", "s2"),
		row(at(10, 45), "$pageview", "/b", "v1", "s1"),
		row(at(11, 5), "$pageview", "/a", "v3", "s3"),
	})

	// Three pageviews in hour 10, one in hour 11 — bucketed by hour, not lumped into a day.
	h10 := at(10, 0)
	h11 := at(11, 0)

	var got10, got11 int64
	for _, r := range rolled {
		if r.Dim == DimNone && r.Event == "$pageview" {
			switch r.Hour {
			case h10:
				got10 = r.Events
			case h11:
				got11 = r.Events
			}
		}
	}
	if got10 != 3 || got11 != 1 {
		t.Errorf("hourly totals = %d, %d; want 3, 1", got10, got11)
	}

	// Path breakdown within hour 10.
	var pathA int64
	for _, r := range rolled {
		if r.Hour == h10 && r.Dim == DimPath && r.Key == "/a" {
			pathA = r.Events
		}
	}
	if pathA != 2 {
		t.Errorf("events for /a in hour 10 = %d, want 2", pathA)
	}
}

// Empty dimension values must be skipped, not rolled up under a blank key: a count of events
// whose country is unknown is not a country breakdown, and mixing them makes both wrong.
func TestEmptyDimensionValuesAreSkipped(t *testing.T) {
	r := row(at(10, 0), "$pageview", "/a", "v1", "s1")
	r.Country = ""
	r.Browser = ""

	rolled := Build([]store.Row{r})

	if got := find(rolled, "$pageview", DimCountry, ""); got != nil {
		t.Errorf("an empty country produced a rollup row with a blank key: %+v", got)
	}
	if got := find(rolled, "$pageview", DimPath, "/a"); got == nil {
		t.Error("the non-empty path dimension was not rolled up")
	}
}

// The reason sketches exist rather than counts. A visitor active in three hours appears in
// three hourly buckets; summing their unique counts would say three visitors. Merging the
// sketches says one, which is the true answer.
func TestUniquesMergeRatherThanSum(t *testing.T) {
	rolled := Build([]store.Row{
		row(at(10, 0), "$pageview", "/a", "v1", "s1"),
		row(at(11, 0), "$pageview", "/a", "v1", "s1"),
		row(at(12, 0), "$pageview", "/a", "v1", "s1"),
	})

	// Three separate hourly buckets, each with one visitor.
	var buckets int
	var summed int64
	for _, r := range rolled {
		if r.Dim == DimNone && r.Event == "$pageview" {
			buckets++
			summed += r.Events
		}
	}
	if buckets != 3 {
		t.Fatalf("got %d hourly buckets, want 3", buckets)
	}
	if summed != 3 {
		t.Errorf("event count across buckets = %d, want 3 — counts DO sum", summed)
	}

	// But merging the sketches must yield one visitor, not three.
	var headline []Row
	for _, r := range rolled {
		if r.Dim == DimNone && r.Event == "$pageview" {
			headline = append(headline, r)
		}
	}
	merged, err := MergeByKey(headline)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 1 {
		t.Fatalf("got %d merged results, want 1", len(merged))
	}
	if merged[0].Visitors != 1 {
		t.Errorf("merged uniques = %d, want 1 — summing hourly uniques would have said 3", merged[0].Visitors)
	}
	if merged[0].Events != 3 {
		t.Errorf("merged events = %d, want 3", merged[0].Events)
	}
}

// Accuracy of the estimate at each precision, since the two tiers trade differently.
func TestSketchAccuracy(t *testing.T) {
	const visitors = 20_000

	var rows []store.Row
	for i := range visitors {
		// Two events each, so events and uniques differ and a bug swapping them is visible.
		v := fmt.Sprintf("visitor-%06d", i)
		rows = append(rows, row(at(10, 0), "$pageview", "/a", v, "sess-"+v))
		rows = append(rows, row(at(10, 1), "$pageview", "/a", v, "sess-"+v))
	}

	rolled := Build(rows)

	headline := find(rolled, "$pageview", DimNone, "")
	breakdown := find(rolled, "$pageview", DimPath, "/a")
	if headline == nil || breakdown == nil {
		t.Fatal("expected both a headline and a path row")
	}

	check := func(name string, r *Row, tolerance float64) {
		merged, err := MergeByKey([]Row{*r})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		est := float64(merged[0].Visitors)
		errPct := math.Abs(est-visitors) / visitors * 100
		t.Logf("%-9s estimate %6.0f vs %d actual — %.2f%% error", name, est, visitors, errPct)
		if errPct > tolerance {
			t.Errorf("%s error %.2f%% exceeds the %.1f%% expected at this precision", name, errPct, tolerance)
		}
		if merged[0].Events != int64(visitors*2) {
			t.Errorf("%s events = %d, want %d — event counts are exact, not estimated",
				name, merged[0].Events, visitors*2)
		}
	}

	// p=14 on the headline number somebody will compare against another tool.
	check("headline", headline, 2.0)
	// p=11 on a per-path breakdown, where an eighth of the size is worth a looser estimate.
	check("breakdown", breakdown, 6.0)
}

// Partials from several compactions, or several nodes, must combine to the same answer as one
// pass over everything. This is what lets nodes write into a shared bucket without agreeing on
// anything.
func TestPartialsCombineLikeASinglePass(t *testing.T) {
	all := []store.Row{
		row(at(10, 0), "$pageview", "/a", "v1", "s1"),
		row(at(10, 5), "$pageview", "/a", "v2", "s2"),
		row(at(10, 9), "$pageview", "/a", "v1", "s1"),
		row(at(10, 20), "$pageview", "/a", "v3", "s3"),
	}

	single, err := MergeByKey(onlyPath(Build(all)))
	if err != nil {
		t.Fatal(err)
	}

	// Same events, compacted in two separate passes, as two nodes or two segments would.
	partA := Build(all[:2])
	partB := Build(all[2:])
	combined, err := MergeByKey(append(onlyPath(partA), onlyPath(partB)...))
	if err != nil {
		t.Fatal(err)
	}

	if len(single) != 1 || len(combined) != 1 {
		t.Fatalf("got %d and %d results, want 1 each", len(single), len(combined))
	}
	if single[0].Events != combined[0].Events {
		t.Errorf("events: one pass %d, two partials %d", single[0].Events, combined[0].Events)
	}
	if single[0].Visitors != combined[0].Visitors {
		t.Errorf("uniques: one pass %d, two partials %d — sketch merging is not associative here",
			single[0].Visitors, combined[0].Visitors)
	}
}

func onlyPath(rows []Row) []Row {
	var out []Row
	for _, r := range rows {
		if r.Dim == DimPath {
			out = append(out, r)
		}
	}
	return out
}

func TestHourOfTruncatesToUTCHour(t *testing.T) {
	got := HourOf(at(10, 59))
	want := at(10, 0)
	if got != want {
		t.Errorf("HourOf(10:59) = %v, want %v", time.UnixMilli(got).UTC(), time.UnixMilli(want).UTC())
	}
}

// Both halves of an A/B assignment are rolled up.
//
// `variant` alone was a dimension from the start; `experiment` was stored on the event and
// never aggregated, so a site running two tests had both their arms in one variant breakdown
// with no way to tell which test a row belonged to. Modlix works around the remaining half of
// that -- there is no filter on a query, so it cannot ask for one experiment's arms -- by
// sending the rule key as part of the variant value. This dimension is what makes the
// experiment itself answerable.
func TestExperimentAndVariantAreBothRolledUp(t *testing.T) {
	r := row(at(10, 0), "$pageview", "/", "v1", "s1")
	r.Experiment = "1EVfJvnNJb5Tw9F21yU0rT"
	r.Variant = "1EVfJvnNJb5Tw9F21yU0rT:homeTwo"

	rolled := Build([]store.Row{r})

	if got := find(rolled, "$pageview", DimExperiment, "1EVfJvnNJb5Tw9F21yU0rT"); got == nil {
		t.Error("the experiment dimension was not rolled up")
	}
	if got := find(rolled, "$pageview", DimVariant, "1EVfJvnNJb5Tw9F21yU0rT:homeTwo"); got == nil {
		t.Error("the variant dimension was not rolled up")
	}
}

// An event outside any test must not create a row under a blank experiment, which would make
// "views in an experiment" and "views overall" the same number.
func TestAnEventInNoExperimentIsNotRolledUpUnderABlankKey(t *testing.T) {
	rolled := Build([]store.Row{row(at(10, 0), "$pageview", "/", "v1", "s1")})

	if got := find(rolled, "$pageview", DimExperiment, ""); got != nil {
		t.Errorf("an event with no experiment produced a rollup row: %+v", got)
	}
}
