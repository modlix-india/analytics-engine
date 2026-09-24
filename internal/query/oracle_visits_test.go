package query

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// visitData generates visits the way the beacon actually produces them: a burst of page views
// with gaps of seconds to minutes, never a gap longer than the session idle timeout, since a
// longer one would have minted a new session id.
//
// Crucially it generates visits that straddle both edges of the range the test asks about,
// because the edges are the only part of this that is difficult. A generator whose visits all
// sat comfortably inside the window would agree with an implementation that ignored the
// boundary entirely, which is exactly the bug worth catching.
func visitData(n int) []store.Row {
	rng := rand.New(rand.NewSource(11))
	// The range the test queries is 12:00 to 20:00; visits start anywhere from an hour before
	// it to an hour after, so both edges are crossed by many visits.
	base := time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC)
	paths := []string{"/", "/pricing", "/features", "/blog", "/contact", "/about"}

	var rows []store.Row
	for i := range n {
		session := fmt.Sprintf("s-%06d", i)
		start := base.Add(time.Duration(rng.Intn(10*3600)) * time.Second)

		// One to six page views, each 5 to 400 seconds after the last. Well inside the
		// 30-minute idle window, so one session id is correct for the whole burst.
		at := start
		for step, views := 0, 1+rng.Intn(6); step < views; step++ {
			r := store.Row{
				// Milliseconds are unique within a session, exactly as ingest guarantees by
				// spending one per event in a batch. Ties would make "the first page view"
				// ambiguous and the two implementations could order them differently.
				TSServer: at.UnixMilli() + int64(step),
				Site:     "s.example",
				Name:     "$pageview",
				Path:     paths[rng.Intn(len(paths))],
				Visitor:  fmt.Sprintf("v-%06d", i),
				Session:  session,
			}
			// Some visits carry a page name, some do not, so the page-beats-address rule is
			// exercised on both sides rather than being a branch nothing takes.
			if rng.Intn(3) == 0 {
				r.Page = "page" + r.Path
			}
			rows = append(rows, r)

			at = at.Add(time.Duration(5+rng.Intn(395)) * time.Second)
		}

		// A click in the middle of some visits. It must not become an entry or an exit: a
		// visit whose last event is a click still left from the page that click was on.
		if rng.Intn(4) == 0 {
			rows = append(rows, store.Row{
				TSServer: at.UnixMilli(), Site: "s.example", Name: "$click",
				Path: paths[rng.Intn(len(paths))], Visitor: fmt.Sprintf("v-%06d", i),
				Session: session, ClickX: 5000, ClickY: 200, Viewport: 1440,
			})
		}
	}
	return rows
}

// The SQL below states the same definition by a different route: window functions over the
// widened scan rather than a per-session sort in Go. The two agree only if the boundary rules
// agree, which is the point.
const visitOracleSQL = `
	WITH e AS (
		SELECT session, ts_server, event,
		       CASE WHEN page <> '' THEN page ELSE path END AS pg
		FROM read_parquet('%s')
		WHERE session <> ''
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
	),
	bounds AS (
		SELECT session, min(ts_server) AS first_ts, max(ts_server) AS last_ts
		FROM e GROUP BY session
	),
	in_range AS (
		SELECT * FROM e
		WHERE event = '$pageview'
		  AND ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
	),
	ranked AS (
		SELECT session, pg,
		       row_number() OVER (PARTITION BY session ORDER BY ts_server ASC)  AS rn_first,
		       row_number() OVER (PARTITION BY session ORDER BY ts_server DESC) AS rn_last
		FROM in_range
	)
	SELECT pg AS label, count(*) AS events, 0 AS visitors
	FROM ranked JOIN bounds USING (session)
	WHERE %s
	  AND first_ts >= TIMESTAMP '%s' AND first_ts < TIMESTAMP '%s'
	GROUP BY pg
	ORDER BY events DESC, label ASC
	LIMIT 10`

func TestOracleEntryPages(t *testing.T) {
	duckdbAvailable(t)

	dir := fixture(t, visitData(8_000))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetEntryPages, Site: "s.example", From: from, To: to, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// An entry is counted for every visit that began inside the range, finished or not, so
	// the only condition here is rn_first = 1.
	want := duckQuery(t, fmt.Sprintf(visitOracleSQL,
		rawGlob(dir),
		ts(from.Add(-sessIdle)), ts(to.Add(sessIdle)),
		ts(from), ts(to),
		"rn_first = 1",
		ts(from), ts(to)))

	compare(t, "entryPages", got.Rows, want, 0)
}

func TestOracleExitPages(t *testing.T) {
	duckdbAvailable(t)

	dir := fixture(t, visitData(8_000))
	from := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	to := from.Add(8 * time.Hour)

	got, err := New(dir).Query(context.Background(), Request{
		Widget: WidgetExitPages, Site: "s.example", From: from, To: to, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// An exit additionally requires the visit to have ENDED inside the range. `last_ts` is
	// taken over the widened scan, so a visit that ran on past `to` is excluded here exactly
	// as the engine excludes it.
	want := duckQuery(t, fmt.Sprintf(visitOracleSQL,
		rawGlob(dir),
		ts(from.Add(-sessIdle)), ts(to.Add(sessIdle)),
		ts(from), ts(to),
		fmt.Sprintf("rn_last = 1 AND last_ts < TIMESTAMP '%s'", ts(to)),
		ts(from), ts(to)))

	compare(t, "exitPages", got.Rows, want, 0)
}
