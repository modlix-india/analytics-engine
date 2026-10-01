package query

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// The partition count is a MEMORY decision and must never be an ANSWER decision.
//
// Until 2026-09-30 it was fixed at sixteen, so nothing depended on it varying and nothing checked
// that it could. Sizing it per query is what makes a small site read its files once instead of
// sixteen times - and it is only safe because the accumulators are additive across partitions, a
// property no test had ever asserted.
//
// So these run the same fixture at one partition and at sixteen and require identical output. If
// a future widget accumulates something that is not additive - a median, a top-N taken per pass,
// anything order-dependent - this is what fails.
func TestPartitionCountDoesNotChangeResults(t *testing.T) {
	dir := fixture(t, visitData(8_000))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)

	widgets := []string{WidgetEntryPages, WidgetExitPages, WidgetPagesPerVisit, WidgetVisitLength}

	for _, w := range widgets {
		t.Run(w, func(t *testing.T) {
			run := func(rowsPerPartition int64) *Result {
				e := New(dir)
				e.RowsPerPartition = rowsPerPartition
				got, err := e.Query(context.Background(), Request{
					Widget: w, Site: "s.example", From: from, To: to, Limit: 10,
				})
				if err != nil {
					t.Fatal(err)
				}
				return got
			}

			// A budget larger than the fixture => one pass. A budget of one row => the cap, 16.
			single := run(1 << 40)
			many := run(1)

			if !reflect.DeepEqual(single.Rows, many.Rows) {
				t.Fatalf("rows differ between 1 partition and %d\n one: %+v\n many: %+v",
					visitorPartitions, single.Rows, many.Rows)
			}
			if single.Total != many.Total {
				t.Fatalf("total differs: one=%d many=%d", single.Total, many.Total)
			}
			if single.Denominator != many.Denominator {
				t.Fatalf("denominator differs: one=%d many=%d", single.Denominator, many.Denominator)
			}
		})
	}
}

// partitionsFor is the whole optimisation, so its arithmetic is pinned directly rather than
// inferred from a timing.
func TestPartitionsForArithmetic(t *testing.T) {
	dir := fixture(t, visitData(200))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)
	scan := span{from.Add(-sessIdle), to.Add(sessIdle)}

	e := New(dir)
	rows, err := e.countRows(context.Background(), "s.example", scan)
	if err != nil {
		t.Fatal(err)
	}
	if rows <= 0 {
		t.Fatalf("countRows read nothing from the footers; got %d", rows)
	}

	cases := []struct {
		name   string
		budget int64
		want   int
	}{
		{"a budget above the data is one pass", rows * 2, 1},
		{"exactly the row count is still one pass", rows, 1},
		{"half the rows is two passes", (rows + 1) / 2, 2},
		{"a tiny budget is capped at the maximum", 1, visitorPartitions},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := New(dir)
			e.RowsPerPartition = c.budget
			if got := e.partitionsFor(context.Background(), "s.example", scan); got != c.want {
				t.Fatalf("rows=%d budget=%d: partitions=%d, want %d", rows, c.budget, got, c.want)
			}
		})
	}
}

// Counting must never be able to fail a query. A site with no files at all is the cheapest way to
// reach the "nothing to count" path, and it must still produce a usable partition count.
func TestPartitionsForUnknownSiteFallsBack(t *testing.T) {
	dir := fixture(t, visitData(10))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	scan := span{from, from.Add(time.Hour)}

	got := New(dir).partitionsFor(context.Background(), "no-such-site.example", scan)
	if got < 1 || got > visitorPartitions {
		t.Fatalf("partitions=%d, want between 1 and %d", got, visitorPartitions)
	}
}

// A cancelled context must stop the scan rather than run it to completion for a response nobody
// will read. /q has a 30s write timeout and these scans could outlast it.
func TestScanStopsOnCancelledContext(t *testing.T) {
	dir := fixture(t, visitData(2_000))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := New(dir)
	err := e.eachVisit(ctx, "s.example", span{from, to}, func(v *visit) {
		t.Fatal("a visit was produced from a cancelled scan")
	})
	if err == nil {
		t.Fatal("expected an error from a cancelled scan, got nil")
	}
}
