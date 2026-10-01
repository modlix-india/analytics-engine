package query

import (
	"context"
	"testing"
	"time"
)

// What the change is worth, measured rather than argued.
//
// Same fixture, same widget, only the partition budget differs: one pass versus the fixed sixteen
// the code used before. The gap is the cost of decoding the same Parquet sixteen times, and a
// dashboard pays it four times over because the four visit widgets are separate queries.
func BenchmarkVisitWidgetPartitions(b *testing.B) {
	dir := fixture(b, visitData(8_000))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)

	run := func(b *testing.B, budget int64) {
		e := New(dir)
		e.RowsPerPartition = budget
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := e.Query(context.Background(), Request{
				Widget: WidgetEntryPages, Site: "s.example", From: from, To: to, Limit: 10,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("sixteen_passes_old", func(b *testing.B) { run(b, 1) })
	b.Run("one_pass_new", func(b *testing.B) { run(b, 1<<40) })
}
