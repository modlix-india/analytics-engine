package query

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// funnelData generates visitors who progress varying distances through view → add → buy, with
// realistic gaps — including some that fall outside the conversion window, and some who do the
// steps out of order. A generator where everyone converts would agree with almost any
// implementation.
func funnelData(n int) []store.Row {
	rng := rand.New(rand.NewSource(7))
	base := time.Date(2026, 9, 19, 6, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range n {
		v := fmt.Sprintf("v-%05d", i)
		// Spread visitors across 12 hours; gaps within a visitor are minutes to days.
		start := base.Add(time.Duration(rng.Intn(12*3600)) * time.Second)

		rows = append(rows, evt(start, "view", v))

		switch r := rng.Intn(100); {
		case r < 20:
			// Entered only.
		case r < 50:
			rows = append(rows, evt(start.Add(time.Duration(rng.Intn(120))*time.Minute), "add", v))
		case r < 70:
			add := start.Add(time.Duration(rng.Intn(120)) * time.Minute)
			rows = append(rows, evt(add, "add", v))
			rows = append(rows, evt(add.Add(time.Duration(rng.Intn(60))*time.Minute), "buy", v))
		case r < 85:
			// Out of order: buys before adding, so reaches step 2 but never step 3.
			rows = append(rows, evt(start.Add(10*time.Minute), "buy", v))
			rows = append(rows, evt(start.Add(20*time.Minute), "add", v))
		default:
			// Adds far too late, outside any sane window.
			rows = append(rows, evt(start.Add(50*time.Hour), "add", v))
		}
	}
	return rows
}

type stepRow struct {
	Step     float64 `json:"step"`
	Visitors float64 `json:"visitors"`
}

func duckSteps(t *testing.T, sql string) []stepRow {
	t.Helper()
	bin := duckdbAvailable(t)
	out, err := exec.Command(bin, "-json", "-c", "SET TimeZone='UTC'; "+sql).CombinedOutput()
	if err != nil {
		t.Fatalf("duckdb failed: %v\nsql:\n%s\noutput:\n%s", err, sql, out)
	}
	var rows []stepRow
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("parsing duckdb output: %v\noutput:\n%s", err, out)
	}
	return rows
}

// The funnel, against SQL that implements the same definition by an entirely different route:
// the first occurrence of step one anchors the window, and each later step is the earliest
// occurrence strictly after the previous step and within that window.
//
// Funnels are the widget most able to be confidently wrong — a subtly different ordering or
// window rule still returns a plausible, monotonically decreasing set of numbers that nobody
// would question. This is the check that catches it.
func TestOracleFunnel(t *testing.T) {
	duckdbAvailable(t)

	rows := funnelData(5_000)
	dir := fixture(t, rows)

	from := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	to := from.Add(96 * time.Hour)
	const windowHours = 24

	got, err := New(dir).Query(Request{
		Widget: WidgetFunnel, Site: "s.example",
		Steps: []string{"view", "add", "buy"}, WindowHours: windowHours,
		From: from, To: to,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := duckSteps(t, fmt.Sprintf(`
		WITH ev AS (
		  SELECT visitor, event, ts_server FROM read_parquet('%s')
		  WHERE ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		),
		s1 AS (SELECT visitor, min(ts_server) AS t0 FROM ev WHERE event = 'view' GROUP BY 1),
		s2 AS (
		  SELECT s1.visitor, s1.t0, min(ev.ts_server) AS t1
		  FROM s1 JOIN ev USING (visitor)
		  WHERE ev.event = 'add'
		    AND ev.ts_server > s1.t0
		    AND ev.ts_server <= s1.t0 + INTERVAL %d HOUR
		  GROUP BY 1, 2
		),
		s3 AS (
		  SELECT s2.visitor
		  FROM s2 JOIN ev USING (visitor)
		  WHERE ev.event = 'buy'
		    AND ev.ts_server > s2.t1
		    AND ev.ts_server <= s2.t0 + INTERVAL %d HOUR
		  GROUP BY 1
		)
		SELECT 1 AS step, count(*) AS visitors FROM s1
		UNION ALL SELECT 2, count(*) FROM s2
		UNION ALL SELECT 3, count(*) FROM s3
		ORDER BY 1`,
		rawGlob(dir), ts(from), ts(to), windowHours, windowHours))

	if len(want) != 3 {
		t.Fatalf("oracle returned %d steps, want 3", len(want))
	}
	for i := range 3 {
		if want[i].Visitors == 0 {
			t.Fatalf("step %d: the oracle found no visitors, so this proves nothing", i+1)
		}
		if float64(got.Funnel[i].Visitors) != want[i].Visitors {
			t.Errorf("step %d (%s): %d visitors from the engine, %.0f from DuckDB",
				i+1, got.Funnel[i].Event, got.Funnel[i].Visitors, want[i].Visitors)
		}
	}
	t.Logf("funnel: %.0f → %.0f → %.0f", want[0].Visitors, want[1].Visitors, want[2].Visitors)
}

type cohortRow struct {
	Cohort   string  `json:"cohort"`
	Day      string  `json:"day"`
	Visitors float64 `json:"visitors"`
}

// Retention, against SQL that cohorts by first-seen day and counts distinct returners per day.
func TestOracleRetention(t *testing.T) {
	duckdbAvailable(t)

	// Visitors appearing on varied combinations of days, so cohorts and return patterns
	// genuinely differ rather than every visitor following one shape.
	rng := rand.New(rand.NewSource(11))
	var rows []store.Row
	for i := range 3_000 {
		v := fmt.Sprintf("r-%05d", i)
		for d := 1; d <= 5; d++ {
			if rng.Intn(100) < 35 {
				rows = append(rows, evt(
					time.Date(2026, 9, d, 8, 0, 0, 0, time.UTC).Add(time.Duration(rng.Intn(3600))*time.Second),
					"x", v))
			}
		}
	}

	dir := fixture(t, rows)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

	got, err := New(dir).Query(Request{
		Widget: WidgetRetention, Site: "s.example", Period: "day",
		From: from, To: to, Timezone: "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	bin := duckdbAvailable(t)
	out, err := exec.Command(bin, "-json", "-c", "SET TimeZone='UTC'; "+fmt.Sprintf(`
		WITH ev AS (
		  SELECT visitor, strftime(ts_server, '%%Y-%%m-%%d') AS d
		  FROM read_parquet('%s')
		  WHERE ts_server >= TIMESTAMP '%s' AND ts_server < TIMESTAMP '%s'
		),
		firsts AS (SELECT visitor, min(d) AS cohort FROM ev GROUP BY 1)
		SELECT f.cohort AS cohort, e.d AS day, count(DISTINCT e.visitor) AS visitors
		FROM firsts f JOIN ev e USING (visitor)
		GROUP BY 1, 2 ORDER BY 1, 2`, rawGlob(dir), ts(from), ts(to))).CombinedOutput()
	if err != nil {
		t.Fatalf("duckdb failed: %v\n%s", err, out)
	}
	var want []cohortRow
	if err := json.Unmarshal(out, &want); err != nil {
		t.Fatalf("parsing duckdb output: %v\n%s", err, out)
	}
	if len(want) == 0 {
		t.Fatal("the oracle returned no cohorts, so this proves nothing")
	}

	// Index the engine's grid by (cohort, absolute day) to compare like with like.
	engine := map[string]uint64{}
	for _, c := range got.Retention {
		cohortDay, err := time.Parse("2006-01-02", c.Label)
		if err != nil {
			t.Fatalf("cohort label %q is not a date", c.Label)
		}
		for offset, v := range c.Values {
			d := cohortDay.AddDate(0, 0, offset).Format("2006-01-02")
			engine[c.Label+"|"+d] = v
		}
	}

	var compared int
	for _, w := range want {
		key := w.Cohort + "|" + w.Day
		if float64(engine[key]) != w.Visitors {
			t.Errorf("cohort %s on %s: %d from the engine, %.0f from DuckDB",
				w.Cohort, w.Day, engine[key], w.Visitors)
		}
		compared++
	}
	t.Logf("compared %d cohort/day cells", compared)
}
