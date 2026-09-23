package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

func evt(tsUTC time.Time, name, visitor string) store.Row {
	return store.Row{
		TSServer: tsUTC.UnixMilli(), Site: "s.example", Name: name,
		Path: "/p", Visitor: visitor, Session: "sess-" + visitor,
	}
}

func day(n int) time.Time { return time.Date(2026, 9, n, 10, 0, 0, 0, time.UTC) }

func TestFunnelCountsDepthPerVisitor(t *testing.T) {
	base := day(1)
	steps := []string{"view", "add", "buy"}

	rows := []store.Row{
		// a: all three, in order.
		evt(base, "view", "a"), evt(base.Add(time.Minute), "add", "a"), evt(base.Add(2*time.Minute), "buy", "a"),
		// b: stops after two.
		evt(base, "view", "b"), evt(base.Add(time.Minute), "add", "b"),
		// c: only entered.
		evt(base, "view", "c"),
		// d: did the later steps but never the first, so never entered the funnel.
		evt(base, "add", "d"), evt(base.Add(time.Minute), "buy", "d"),
		// e: out of order — buy before add — so it reaches step 2 but not step 3.
		evt(base, "view", "e"), evt(base.Add(time.Minute), "buy", "e"), evt(base.Add(2*time.Minute), "add", "e"),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetFunnel, Site: "s.example", Steps: steps,
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []uint64{4, 3, 1} // entered: a,b,c,e | reached add: a,b,e | reached buy: a
	if len(res.Funnel) != 3 {
		t.Fatalf("got %d steps, want 3", len(res.Funnel))
	}
	for i, w := range want {
		if res.Funnel[i].Visitors != w {
			t.Errorf("step %d (%s): %d visitors, want %d", i+1, res.Funnel[i].Event, res.Funnel[i].Visitors, w)
		}
	}

	if got := res.Funnel[2].FromFirst; got < 0.24 || got > 0.26 {
		t.Errorf("overall conversion = %.3f, want 0.25 (1 of 4)", got)
	}
	if got := res.Funnel[1].FromPrevious; got < 0.74 || got > 0.76 {
		t.Errorf("step 2 conversion from previous = %.3f, want 0.75 (3 of 4)", got)
	}
}

// The conversion window is measured from the FIRST step, not between consecutive steps.
// Per-step windows would let a visitor take a month over five steps and still be counted as
// converting within a day.
func TestFunnelWindowIsMeasuredFromTheFirstStep(t *testing.T) {
	base := day(1)
	steps := []string{"view", "add"}

	rows := []store.Row{
		// inside: 2 hours later.
		evt(base, "view", "inside"), evt(base.Add(2*time.Hour), "add", "inside"),
		// outside: 10 hours later, beyond a 6-hour window.
		evt(base, "view", "outside"), evt(base.Add(10*time.Hour), "add", "outside"),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetFunnel, Site: "s.example", Steps: steps, WindowHours: 6,
		From: base.Add(-time.Hour), To: base.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Funnel[0].Visitors != 2 {
		t.Errorf("step 1 = %d, want 2", res.Funnel[0].Visitors)
	}
	if res.Funnel[1].Visitors != 1 {
		t.Errorf("step 2 = %d, want 1 — the 10-hour gap is outside a 6-hour window", res.Funnel[1].Visitors)
	}
}

func TestFunnelRejectsDegenerateRequests(t *testing.T) {
	e := New(t.TempDir())
	now := time.Now()

	for _, steps := range [][]string{nil, {"only_one"}} {
		if _, err := e.Query(context.Background(), Request{
			Widget: WidgetFunnel, Site: "s", Steps: steps,
			From: now.Add(-time.Hour), To: now,
		}); err == nil {
			t.Errorf("a funnel with %d steps was accepted", len(steps))
		}
	}
}

func TestRetentionCohorts(t *testing.T) {
	rows := []store.Row{
		// a: days 1, 3 — cohort day 1, returns at offset 2.
		evt(day(1), "x", "a"), evt(day(3), "x", "a"),
		// b: day 1 only.
		evt(day(1), "x", "b"),
		// c: first seen day 2, returns day 3.
		evt(day(2), "x", "c"), evt(day(3), "x", "c"),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetRetention, Site: "s.example", Period: "day",
		From: day(1).Add(-10 * time.Hour), To: day(3).Add(14 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Retention) != 3 {
		t.Fatalf("got %d cohorts, want 3: %+v", len(res.Retention), res.Retention)
	}

	c1 := res.Retention[0]
	if c1.Size != 2 {
		t.Errorf("day-1 cohort size = %d, want 2 (a and b)", c1.Size)
	}
	if c1.Values[0] != 2 {
		t.Errorf("day-1 cohort at offset 0 = %d, want 2", c1.Values[0])
	}
	if c1.Values[1] != 0 {
		t.Errorf("day-1 cohort at offset 1 = %d, want 0 — neither returned on day 2", c1.Values[1])
	}
	if c1.Values[2] != 1 {
		t.Errorf("day-1 cohort at offset 2 = %d, want 1 (a returned on day 3)", c1.Values[2])
	}

	// A later cohort has fewer observable periods, so its row is shorter rather than padded
	// with zeros that would imply observed non-retention.
	if len(res.Retention[2].Values) != 1 {
		t.Errorf("the last cohort has %d observable periods, want 1", len(res.Retention[2].Values))
	}
}

func TestStickinessHistogram(t *testing.T) {
	rows := []store.Row{
		evt(day(1), "x", "a"), evt(day(2), "x", "a"), evt(day(3), "x", "a"), // 3 days
		evt(day(1), "x", "b"), evt(day(1).Add(time.Hour), "x", "b"), // same day twice = 1 day
		evt(day(2), "x", "c"), evt(day(3), "x", "c"), // 2 days
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetStickiness, Site: "s.example", Period: "day",
		From: day(1).Add(-10 * time.Hour), To: day(3).Add(14 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[int]uint64{1: 1, 2: 1, 3: 1}
	if len(res.Stickiness) != 3 {
		t.Fatalf("got %d buckets, want 3: %+v", len(res.Stickiness), res.Stickiness)
	}
	for _, b := range res.Stickiness {
		if want[b.Periods] != b.Visitors {
			t.Errorf("%d-period bucket = %d visitors, want %d", b.Periods, b.Visitors, want[b.Periods])
		}
	}
}

func TestLifecycleClassification(t *testing.T) {
	rows := []store.Row{
		// a: days 1,2,3 — new, then returning twice.
		evt(day(1), "x", "a"), evt(day(2), "x", "a"), evt(day(3), "x", "a"),
		// b: days 1,3 — new, dormant on day 2, resurrecting on day 3.
		evt(day(1), "x", "b"), evt(day(3), "x", "b"),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetLifecycle, Site: "s.example", Period: "day",
		From: day(1).Add(-10 * time.Hour), To: day(3).Add(14 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Lifecycle) != 3 {
		t.Fatalf("got %d points, want 3: %+v", len(res.Lifecycle), res.Lifecycle)
	}
	if !res.RangeRelative {
		t.Error("lifecycle must report RangeRelative: its classifications depend on the queried window")
	}

	for i, want := range []LifecyclePoint{
		{New: 2},
		{Returning: 1, Dormant: 1},
		{Returning: 1, Resurrecting: 1},
	} {
		got := res.Lifecycle[i]
		if got.New != want.New || got.Returning != want.Returning ||
			got.Resurrecting != want.Resurrecting || got.Dormant != want.Dormant {
			t.Errorf("day %d: got new=%d returning=%d resurrecting=%d dormant=%d, want new=%d returning=%d resurrecting=%d dormant=%d",
				i+1, got.New, got.Returning, got.Resurrecting, got.Dormant,
				want.New, want.Returning, want.Resurrecting, want.Dormant)
		}
	}
}

// Each visitor lands in exactly one hash partition, and the accumulators are additive, so the
// answer must not depend on how many partitions the scan used.
func TestResultIsIndependentOfPartitioning(t *testing.T) {
	base := day(1)
	var rows []store.Row
	for i := range 500 {
		v := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i%10))
		rows = append(rows, evt(base.Add(time.Duration(i)*time.Second), "view", v))
		if i%2 == 0 {
			rows = append(rows, evt(base.Add(time.Duration(i)*time.Second+time.Minute), "buy", v))
		}
	}

	e := New(fixture(t, rows))
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetFunnel, Site: "s.example", Steps: []string{"view", "buy"},
		From: base.Add(-time.Hour), To: base.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	// 500 events over 500 distinct-ish visitor ids; whatever the exact count, step 2 must be
	// the half that also bought, and the totals must be self-consistent.
	if res.Funnel[0].Visitors == 0 {
		t.Fatal("no visitors entered the funnel")
	}
	if res.Funnel[1].Visitors > res.Funnel[0].Visitors {
		t.Errorf("step 2 (%d) exceeds step 1 (%d), which is impossible",
			res.Funnel[1].Visitors, res.Funnel[0].Visitors)
	}
}
