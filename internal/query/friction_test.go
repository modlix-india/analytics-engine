package query

import (
	"context"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/store"
)

// clicked builds one click the way the beacon records it. `live` is what the interactive
// column carries: 1 for something that does something, 0 for something inert.
func clicked(ts time.Time, session string, x, y, live int32) store.Row {
	return store.Row{
		TSServer: ts.UnixMilli(), Site: "s.example", Name: "$click",
		Path: "/p", Visitor: "v-" + session, Session: session,
		ClickX: x, ClickY: y, Viewport: 1440, Interactive: live,
	}
}

func TestRageNeedsThreeClicksCloseTogether(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		// A run: four clicks in one spot inside a second.
		clicked(base, "a", 5000, 1200, 1),
		clicked(base.Add(200*time.Millisecond), "a", 5010, 1205, 1),
		clicked(base.Add(400*time.Millisecond), "a", 5005, 1198, 1),
		clicked(base.Add(600*time.Millisecond), "a", 5000, 1200, 1),

		// A double click is not rage.
		clicked(base.Add(time.Minute), "b", 2000, 400, 1),
		clicked(base.Add(time.Minute+100*time.Millisecond), "b", 2000, 400, 1),

		// Three clicks in one place but minutes apart: reading, not jabbing.
		clicked(base.Add(2*time.Minute), "c", 8000, 2000, 1),
		clicked(base.Add(4*time.Minute), "c", 8000, 2000, 1),
		clicked(base.Add(6*time.Minute), "c", 8000, 2000, 1),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	f := res.Friction
	if f == nil {
		t.Fatal("no friction answer")
	}
	if len(f.Rage) != 1 {
		t.Fatalf("got %d rage spots, want 1: %+v", len(f.Rage), f.Rage)
	}
	if f.Rage[0].Clicks != 4 {
		t.Errorf("run length = %d, want 4", f.Rage[0].Clicks)
	}
	if f.Rage[0].Sessions != 1 {
		t.Errorf("sessions = %d, want 1", f.Rage[0].Sessions)
	}
	if f.Clicks != 9 {
		t.Errorf("total clicks = %d, want 9", f.Clicks)
	}
}

// Two people clicking the same spot a second apart are not one person clicking it twice, and
// merging them would manufacture a run out of ordinary traffic on a busy page.
func TestRageIsPerSession(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		clicked(base, "a", 5000, 1200, 1),
		clicked(base.Add(100*time.Millisecond), "b", 5000, 1200, 1),
		clicked(base.Add(200*time.Millisecond), "c", 5000, 1200, 1),
		clicked(base.Add(300*time.Millisecond), "d", 5000, 1200, 1),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Friction.Rage) != 0 {
		t.Errorf("four different visits became a rage spot: %+v", res.Friction.Rage)
	}
}

// One long burst is one spot, counted once. Sliding the window one click at a time would count
// the same clicks in several overlapping runs and turn one impatient moment into a hot region.
func TestOneBurstIsCountedOnce(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	var rows []store.Row
	for i := range 5 {
		rows = append(rows, clicked(base.Add(time.Duration(i*150)*time.Millisecond), "a", 5000, 1200, 1))
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Friction.Rage) != 1 || res.Friction.Rage[0].Clicks != 5 {
		t.Errorf("got %+v, want one spot of five clicks", res.Friction.Rage)
	}
}

func TestDeadClicksAreGroupedAndCounted(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		// Three visits clicking the same inert thing.
		clicked(base, "a", 3000, 800, 0),
		clicked(base.Add(time.Minute), "b", 3010, 805, 0),
		clicked(base.Add(2*time.Minute), "c", 2995, 795, 0),
		// And a live one elsewhere, which must not be listed.
		clicked(base.Add(3*time.Minute), "d", 7000, 1600, 1),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	f := res.Friction
	if !f.Measured {
		t.Error("the page has a live click on it, so the column was measured")
	}
	if len(f.Dead) != 1 {
		t.Fatalf("got %d dead spots, want 1: %+v", len(f.Dead), f.Dead)
	}
	if f.Dead[0].Clicks != 3 || f.Dead[0].Sessions != 3 {
		t.Errorf("dead spot = %+v, want 3 clicks from 3 visits", f.Dead[0])
	}
}

// The failure that would be most alarming and least true: a page recorded before the column
// existed has every click at zero, and reading those as dead reports a page where nothing
// works at all.
func TestClicksRecordedBeforeTheColumnAreNotReportedAsDead(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		clicked(base, "a", 3000, 800, 0),
		clicked(base.Add(time.Minute), "b", 5000, 1200, 0),
		clicked(base.Add(2*time.Minute), "c", 7000, 1600, 0),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Friction.Measured {
		t.Error("nothing on this page was ever marked interactive, so it was not measured")
	}
	if len(res.Friction.Dead) != 0 {
		t.Errorf("unmeasured clicks were reported as dead: %+v", res.Friction.Dead)
	}
	if res.Friction.Clicks != 3 {
		t.Errorf("the clicks themselves are still counted: got %d, want 3", res.Friction.Clicks)
	}
}

// A button that straddles a cell boundary is one problem, not two half-strength ones. Which
// half would have been reported first came down to where the button sat rather than to how
// bad it was.
func TestASpotStraddlingACellBoundaryIsOneSpot(t *testing.T) {
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	rows := []store.Row{
		// 2995 rounds into the cell at 2900; 3000 and 3010 into the one at 3000.
		clicked(base, "a", 2995, 795, 0),
		clicked(base.Add(time.Minute), "b", 3000, 800, 0),
		clicked(base.Add(2*time.Minute), "c", 3010, 805, 0),
		// One live click elsewhere, so the page counts as measured.
		clicked(base.Add(3*time.Minute), "d", 9000, 3000, 1),
	}

	res, err := New(fixture(t, rows)).Query(context.Background(), Request{
		Widget: WidgetClickFriction, Site: "s.example", Path: "/p",
		From: base.Add(-time.Hour), To: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Friction.Dead) != 1 {
		t.Fatalf("got %d dead spots, want 1: %+v", len(res.Friction.Dead), res.Friction.Dead)
	}
	if res.Friction.Dead[0].Clicks != 3 {
		t.Errorf("merged spot = %+v, want all 3 clicks", res.Friction.Dead[0])
	}
	// The merged spot sits where most of the clicks were, not at an average of two places.
	if res.Friction.Dead[0].X != 3000 {
		t.Errorf("merged spot x = %d, want 3000 — the busier of the two cells",
			res.Friction.Dead[0].X)
	}
}
