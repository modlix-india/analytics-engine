package rollup

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// realisticDay mirrors the generator in store's size test: one distinct visitor per four
// events and an 800-path long tail, because cardinality is what decides both file sizes.
func realisticDay(n int) []store.Row {
	rng := rand.New(rand.NewSource(1))
	day := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)

	paths := make([]string, 800)
	for i := range paths {
		paths[i] = fmt.Sprintf("/blog/%d-some-article-slug-here", i)
	}
	hot := []string{"/", "/pricing", "/features", "/docs", "/contact"}
	browsers := []string{"Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"}
	oses := []string{"Windows", "macOS", "Android", "iOS", "Linux"}
	devices := []string{"desktop", "mobile", "tablet"}
	channels := []string{"direct", "organic", "social", "referral", "paid", "email"}
	countries := []string{"IN", "US", "GB", "DE", "AE", "SG", "AU", "CA"}
	refs := []string{"", "google.com", "facebook.com", "t.co", "news.ycombinator.com", "bing.com"}

	visitors := n / 4
	rows := make([]store.Row, 0, n)
	for i := range n {
		v := rng.Intn(visitors)
		p := hot[rng.Intn(len(hot))]
		if rng.Intn(100) < 40 {
			p = paths[rng.Intn(len(paths))]
		}
		name := "$pageview"
		if rng.Intn(100) < 25 {
			name = []string{"cta_clicked", "form_submitted", "scroll_depth", "video_played"}[rng.Intn(4)]
		}
		rows = append(rows, store.Row{
			// Spread across exactly one day regardless of n. Fixed spacing would make a
			// larger n span more days, which silently multiplies the hourly buckets and makes
			// any scaling comparison meaningless.
			TSServer:     day.Add(time.Duration(float64(i) / float64(n) * float64(24*time.Hour))).UnixMilli(),
			Site:         "shop.example",
			Name:         name,
			Path:         p,
			Label:        fmt.Sprintf("cta_%d", rng.Intn(40)),
			Visitor:      fmt.Sprintf("v-%016x", v),
			Session:      fmt.Sprintf("s-%016x", v*7%visitors),
			ReferrerHost: refs[rng.Intn(len(refs))],
			Channel:      channels[rng.Intn(len(channels))],
			Device:       devices[rng.Intn(len(devices))],
			Browser:      browsers[rng.Intn(len(browsers))],
			OS:           oses[rng.Intn(len(oses))],
			Platform:     []string{"web", "web", "web", "mobile_app"}[rng.Intn(4)],
			Country:      countries[rng.Intn(len(countries))],
		})
	}
	return rows
}

// Rollups are a SCALE optimisation, not a universal win, and the numbers say so plainly.
//
// Raw Parquet grows with event count. A rollup grows with distinct keys times hours, so it
// plateaus. Measured on one site's day, varying only the event count:
//
//	  200,000 events | raw  2.05 MB | rollup  3.97 MB | 193% of raw
//	1,000,000 events | raw 11.11 MB | rollup 10.52 MB |  95% of raw
//	4,000,000 events | raw 45.08 MB | rollup 17.67 MB |  39% of raw
//
// Break-even is around a million events a day for one site. Below that the rollup tier costs
// more storage than the data it summarises — and it is kept anyway, for two reasons. The
// absolute cost at low volume is a few MB a day, and the tier's real purpose is not saving
// bytes but answering UNIQUE VISITOR questions, which raw data cannot do without a distinct
// count over every visitor id in the range.
//
// An earlier version of this test asserted the rollup must always be smaller than raw. That
// was simply wrong about the shape of the problem, and it failed for the right reason.
func TestRollupScalesBetterThanRaw(t *testing.T) {
	type result struct {
		events int
		ratio  float64
		rows   int
	}
	var results []result

	for _, n := range []int{200_000, 1_000_000, 4_000_000} {
		rows := realisticDay(n)
		rolled := Build(rows)

		dir := t.TempDir()
		raw := filepath.Join(dir, "raw.parquet")
		roll := filepath.Join(dir, "roll.parquet")
		if err := store.WriteFile(raw, rows); err != nil {
			t.Fatal(err)
		}
		if err := WriteFile(roll, rolled); err != nil {
			t.Fatal(err)
		}
		rs, _ := os.Stat(raw)
		ls, _ := os.Stat(roll)
		ratio := float64(ls.Size()) / float64(rs.Size())

		t.Logf("%9d events | raw %7.2f MB | rollup %6.2f MB | rows %6d | %5.1f%% of raw",
			n, float64(rs.Size())/(1<<20), float64(ls.Size())/(1<<20), len(rolled), 100*ratio)
		results = append(results, result{n, ratio, len(rolled)})
	}

	// The property that matters: the advantage must improve with volume. If this reverses,
	// the sketches have gone dense too early or a dimension has become unbounded.
	for i := 1; i < len(results); i++ {
		if results[i].ratio >= results[i-1].ratio {
			t.Errorf("rollup/raw ratio did not improve with volume: %.2f at %d events, %.2f at %d",
				results[i-1].ratio, results[i-1].events, results[i].ratio, results[i].events)
		}
	}

	// Rollup rows must plateau rather than track event count, or the tier never pays off.
	if got := float64(results[2].rows) / float64(results[0].rows); got > 4 {
		t.Errorf("rollup rows grew %.1fx while events grew 20x; buckets are not plateauing", got)
	}

	// At the volume where this tier is meant to earn its keep, it must clearly win.
	if results[2].ratio > 0.5 {
		t.Errorf("at 4M events/day the rollup is %.0f%% of raw; it should be well under half",
			100*results[2].ratio)
	}
}
