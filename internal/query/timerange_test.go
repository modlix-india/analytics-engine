package query

import (
	"testing"
	"time"
)

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	return loc
}

// Every plan must account for the whole interval exactly once. If full hours and partials do
// not reconstruct the range, the query silently over- or under-counts at the edges — which is
// precisely the failure the hourly-rollup design exists to prevent.
func checkCoverage(t *testing.T, s span, p hourPlan) {
	t.Helper()
	var total time.Duration
	for range p.FullHours {
		total += time.Hour
	}
	for _, part := range p.Partials {
		total += part.End.Sub(part.Start)
	}
	if want := s.End.Sub(s.Start); total != want {
		t.Errorf("plan covers %v but the range is %v (full=%d partials=%d)",
			total, want, len(p.FullHours), len(p.Partials))
	}
}

// A whole-hour offset always lands on bucket boundaries, so rollups answer it alone.
func TestWholeHourOffsetNeedsNoRawCorrection(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*3600)
	start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
	s := span{start, start.Add(24 * time.Hour)}

	p := planHours(s)
	checkCoverage(t, s, p)

	if len(p.Partials) != 0 {
		t.Errorf("a whole-hour offset produced %d partial spans; it should need none", len(p.Partials))
	}
	if len(p.FullHours) != 24 {
		t.Errorf("got %d full hours, want 24", len(p.FullHours))
	}
}

// India is +5:30, so a local day starts at 18:30 UTC and straddles two buckets. Exactly two
// hours need raw correction, however long the range — which is what keeps this cheap.
func TestHalfHourOffsetCorrectsExactlyTwoBoundaryHours(t *testing.T) {
	loc := mustLoad(t, "Asia/Kolkata")

	for _, days := range []int{1, 7, 30} {
		start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
		s := span{start, start.AddDate(0, 0, days)}

		p := planHours(s)
		checkCoverage(t, s, p)

		if len(p.Partials) != 2 {
			t.Errorf("%d-day IST range: %d partials, want exactly 2", days, len(p.Partials))
		}
		for _, part := range p.Partials {
			if d := part.End.Sub(part.Start); d != 30*time.Minute {
				t.Errorf("%d-day IST range: partial of %v, want 30m", days, d)
			}
		}
		if want := days*24 - 1; len(p.FullHours) != want {
			t.Errorf("%d-day IST range: %d full hours, want %d", days, len(p.FullHours), want)
		}
	}
}

// Nepal is +5:45, the awkward case that a 15-minute bucket scheme would also have to handle.
func TestQuarterHourOffset(t *testing.T) {
	loc := mustLoad(t, "Asia/Kathmandu")
	start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)
	s := span{start, start.Add(24 * time.Hour)}

	p := planHours(s)
	checkCoverage(t, s, p)
	if len(p.Partials) != 2 {
		t.Errorf("got %d partials, want 2", len(p.Partials))
	}
}

func TestRangeInsideOneHour(t *testing.T) {
	start := time.Date(2026, 9, 19, 10, 10, 0, 0, time.UTC)
	s := span{start, start.Add(20 * time.Minute)}

	p := planHours(s)
	checkCoverage(t, s, p)

	if len(p.FullHours) != 0 {
		t.Errorf("got %d full hours for a 20-minute range, want 0", len(p.FullHours))
	}
	if len(p.Partials) != 1 {
		t.Fatalf("got %d partials, want 1", len(p.Partials))
	}
}

func TestEmptyAndInvertedRanges(t *testing.T) {
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	for _, s := range []span{{now, now}, {now, now.Add(-time.Hour)}} {
		p := planHours(s)
		if len(p.FullHours) != 0 || len(p.Partials) != 0 {
			t.Errorf("range %v..%v produced a non-empty plan", s.Start, s.End)
		}
	}
}

// A day is 23 or 25 hours long in any zone that observes DST. Adding 24 hours would be wrong
// twice a year — and IST, where this would be tested locally, does not observe it at all.
func TestDSTDaysAreNotAlways24Hours(t *testing.T) {
	loc := mustLoad(t, "America/New_York")

	// Spring forward 2026: 2am becomes 3am, so that local day is 23 hours.
	start := time.Date(2026, 3, 7, 0, 0, 0, 0, loc)
	days := daySpans(span{start, start.AddDate(0, 0, 3)}, loc)

	if len(days) != 3 {
		t.Fatalf("got %d day spans, want 3", len(days))
	}

	var short, normal int
	for _, d := range days {
		switch d.End.Sub(d.Start) {
		case 23 * time.Hour:
			short++
		case 24 * time.Hour:
			normal++
		}
	}
	if short != 1 {
		t.Errorf("expected exactly one 23-hour day across a spring-forward, got %d "+
			"(day lengths: %v, %v, %v)", short,
			days[0].End.Sub(days[0].Start), days[1].End.Sub(days[1].Start), days[2].End.Sub(days[2].Start))
	}
}

func TestDaySpansInIST(t *testing.T) {
	loc := mustLoad(t, "Asia/Kolkata")
	start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)

	days := daySpans(span{start, start.AddDate(0, 0, 3)}, loc)
	if len(days) != 3 {
		t.Fatalf("got %d days, want 3", len(days))
	}
	// Each IST day begins at 18:30 UTC the previous day.
	if h, m := days[0].Start.UTC().Hour(), days[0].Start.UTC().Minute(); h != 18 || m != 30 {
		t.Errorf("IST day starts at %02d:%02d UTC, want 18:30", h, m)
	}
}

// A local day touches two UTC date directories for any non-zero offset. Missing one would drop
// data silently, which is why the reader always opens the full set.
func TestUTCDatesSpanTheBoundary(t *testing.T) {
	loc := mustLoad(t, "Asia/Kolkata")
	start := time.Date(2026, 9, 19, 0, 0, 0, 0, loc)

	got := utcDates(span{start, start.Add(24 * time.Hour)})
	if len(got) < 2 {
		t.Fatalf("an IST day touched %d UTC dates (%v), want at least 2", len(got), got)
	}
	if got[0] != "2026-09-18" {
		t.Errorf("first UTC date = %s, want 2026-09-18 (an IST day starts the previous UTC day)", got[0])
	}
}

// A range that does not begin at midnight must still cover every calendar day it touches.
//
// Stepping by a fixed 24 hours visits one instant per day, so a "last 24 hours" range starting
// at 16:00 yielded a single period and silently dropped every event in the second calendar
// day. Every unit test here used midnight-aligned ranges and missed it; an end-to-end query
// did not.
func TestPeriodsBetweenCoversMisalignedRanges(t *testing.T) {
	loc := time.UTC

	start := time.Date(2026, 9, 18, 16, 0, 0, 0, loc)
	got := periodsBetween(span{start, start.Add(24 * time.Hour)}, loc, false)

	want := []string{"2026-09-18", "2026-09-19"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("period %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPeriodsBetweenWeekly(t *testing.T) {
	loc := time.UTC
	// A Wednesday, running three weeks.
	start := time.Date(2026, 9, 16, 9, 30, 0, 0, loc)

	got := periodsBetween(span{start, start.AddDate(0, 0, 21)}, loc, true)
	if len(got) != 4 {
		t.Errorf("21 days from a Wednesday touches %d ISO weeks (%v), want 4", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("weeks are not in order: %v", got)
		}
	}
}

// Periods must be contiguous and each appear once, or a retention grid's offsets stop meaning
// "periods since the cohort".
func TestPeriodsBetweenAreUniqueAndOrdered(t *testing.T) {
	loc := time.UTC
	start := time.Date(2026, 9, 18, 7, 15, 0, 0, loc)

	got := periodsBetween(span{start, start.AddDate(0, 0, 10)}, loc, false)
	if len(got) != 11 {
		t.Errorf("a 10-day range from 07:15 touches %d days (%v), want 11", len(got), got)
	}
	seen := map[string]bool{}
	for i, p := range got {
		if seen[p] {
			t.Errorf("period %q appears twice", p)
		}
		seen[p] = true
		if i > 0 && got[i-1] >= p {
			t.Errorf("periods out of order at %d: %v", i, got)
		}
	}
}
