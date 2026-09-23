package wal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The design rests on one node absorbing 1,667 events/sec — the 100k requests/minute target —
// with room to spare. This measures the WAL's share of that, which is the part that touches a
// disk and therefore the part most likely to be the ceiling.
//
// A realistic event payload: ~250 bytes, matching the packed record in PLAN.md section 6.
func BenchmarkAppendConcurrent(b *testing.B) {
	payload := make([]byte, 250)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	for _, writers := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			dir := b.TempDir()
			w, err := Open(Options{
				Dir:          dir,
				SegmentBytes: 256 << 20,
				SegmentAge:   time.Hour,
				SyncInterval: 100 * time.Millisecond, // the shipped default
				Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			var n atomic.Int64
			var wg sync.WaitGroup
			per := max(1, b.N/writers)

			b.ResetTimer()
			start := time.Now()
			for range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range per {
						if err := w.Append(context.Background(), payload); err != nil {
							b.Error(err)
							return
						}
						n.Add(1)
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			b.ReportMetric(float64(n.Load())/elapsed.Seconds(), "events/sec")
		})
	}
}

// Enqueue is what ingest actually calls. Same durability window as Append, without the caller
// waiting to be told about it — so this is the number that decides whether one node meets the
// 1,667 events/sec target.
func BenchmarkEnqueue(b *testing.B) {
	payload := make([]byte, 250)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	dir := b.TempDir()
	w, err := Open(Options{
		Dir:          dir,
		SegmentBytes: 1 << 30,
		SegmentAge:   time.Hour,
		SyncInterval: 100 * time.Millisecond,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Enqueue(payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/sec")
}
