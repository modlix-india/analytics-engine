package store

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// What a day of one site's traffic actually costs on disk.
//
// The figure matters because it decides the storage bill at the design rate, and it is easy to
// flatter by accident: synthetic traffic with a handful of distinct visitors compresses far
// better than real traffic, since visitor and session are the two high-cardinality columns and
// they dominate the file. This generates realistic cardinality — a distinct visitor roughly
// every few events, a long tail of paths — so the number is one we can plan against.
func TestParquetSizePerEvent(t *testing.T) {
	const events = 200_000

	rng := rand.New(rand.NewSource(1))
	day := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)

	// A long tail of paths, as a real site has: a few hot pages and many rare ones.
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

	rows := make([]Row, 0, events)
	// One visitor per ~4 events, which is a realistic pages-per-session figure and makes
	// visitor the highest-cardinality column in the file, as it is in production.
	visitors := events / 4

	for i := range events {
		v := rng.Intn(visitors)
		p := hot[rng.Intn(len(hot))]
		if rng.Intn(100) < 40 {
			p = paths[rng.Intn(len(paths))]
		}
		name := "$pageview"
		if rng.Intn(100) < 25 {
			name = []string{"cta_clicked", "form_submitted", "scroll_depth", "video_played"}[rng.Intn(4)]
		}

		rows = append(rows, Row{
			TSServer:     day.Add(time.Duration(i) * 430 * time.Millisecond).UnixMilli(),
			TSClient:     day.Add(time.Duration(i)*430*time.Millisecond - 80*time.Millisecond).UnixMilli(),
			Site:         "shop.example",
			Name:         name,
			Path:         p,
			Page:         "page_" + p,
			Label:        fmt.Sprintf("cta_%d", rng.Intn(40)),
			Visitor:      fmt.Sprintf("v-%016x", v),
			Session:      fmt.Sprintf("s-%016x", v*7%visitors),
			ReferrerHost: refs[rng.Intn(len(refs))],
			Channel:      channels[rng.Intn(len(channels))],
			UTMSource:    []string{"", "", "", "google", "newsletter"}[rng.Intn(5)],
			UTMMedium:    []string{"", "", "", "cpc", "email"}[rng.Intn(5)],
			UTMCampaign:  []string{"", "", "spring", "launch"}[rng.Intn(4)],
			Device:       devices[rng.Intn(len(devices))],
			Browser:      browsers[rng.Intn(len(browsers))],
			OS:           oses[rng.Intn(len(oses))],
			Platform:     []string{"web", "web", "web", "mobile_app"}[rng.Intn(4)],
			Country:      countries[rng.Intn(len(countries))],
			Props:        `{}`,
		})
	}

	path := filepath.Join(t.TempDir(), "day.parquet")
	if err := WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	perEvent := float64(st.Size()) / float64(events)
	t.Logf("events            %d", events)
	t.Logf("parquet           %.1f MB", float64(st.Size())/(1<<20))
	t.Logf("bytes/event       %.1f", perEvent)
	t.Logf("at 144M events/day %.1f GB/day", perEvent*144e6/(1<<30))

	// Read it back: a size measurement of a file that does not decode is worthless.
	back, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(back) != events {
		t.Fatalf("read back %d rows, wrote %d", len(back), events)
	}

	// The plan budgets ~40 bytes/event. Failing loudly if that is badly wrong is the point:
	// the storage estimates downstream all derive from it.
	if perEvent > 80 {
		t.Errorf("bytes/event = %.1f, well above the ~40 the plan assumes; the storage estimates need revisiting", perEvent)
	}
}
