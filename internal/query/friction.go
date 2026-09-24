package query

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modlix-india/analytics-engine/internal/ingest"
)

// Click friction: the two things a click map can say beyond "people press this a lot".
//
// Both are framed as POSSIBLE. A run of clicks in one spot is consistent with impatience and
// equally consistent with a slider somebody was enjoying; a click on something inert is
// consistent with a mislabelled button and equally consistent with somebody selecting text.
// Neither is evidence of frustration, and a dashboard that says it is will be believed.

const (
	// rageWithin and rageRadius define a run: clicks close together in time and in space.
	//
	// One second and 30 thousandths of the viewport width (about 40px at 1440). Wider or
	// slower and an ordinary double click on a link qualifies; tighter and a real burst of
	// jabbing at a dead button is split into singletons.
	rageWithin = time.Second
	rageRadius = int32(300) // in the click's own ten-thousandths-of-width units

	// rageMinClicks is how many make a run. Three, because two is a double click.
	rageMinClicks = 3
)

// FrictionSpot is one place on the page worth looking at, and how sure we are it is a place
// rather than a coincidence.
type FrictionSpot struct {
	X int32 `json:"x"`
	Y int32 `json:"y"`

	// Clicks is how many landed here. For a rage spot it is the length of the longest run;
	// for a dead spot it is every click on something inert.
	Clicks int64 `json:"clicks"`

	// Sessions is how many different visits produced it. One visit clicking twenty times is a
	// person having a bad minute; twenty visits clicking once each is a design problem, and
	// the two look identical in a click count.
	Sessions int64 `json:"sessions"`
}

// Friction is the answer for one page.
type Friction struct {
	// Rage is spots where somebody clicked repeatedly in one place in quick succession.
	Rage []FrictionSpot `json:"rage,omitempty"`

	// Dead is spots where clicks landed on something that does nothing.
	Dead []FrictionSpot `json:"dead,omitempty"`

	// Clicks is every click on the page in range, so a reader can see how much of the page's
	// activity these spots account for.
	Clicks int64 `json:"clicks"`

	// Measured is false when no click on this page carries the interactive column — every row
	// recorded before it existed. The dead list is then empty because nothing was measured,
	// which is a different statement from "nothing was dead", and a dashboard that cannot
	// tell them apart will report a clean page.
	Measured bool `json:"measured"`

	// CellX and CellY are the grid a spot was rounded to, the same one the heatmap draws on,
	// so the two can be laid over each other without rescaling.
	CellX int32 `json:"cellX"`
	CellY int32 `json:"cellY"`
}

// clickFriction finds rage runs and dead clicks on one page.
//
// One widget rather than two because they are drawn on one canvas and read together: a spot
// that is both repeatedly clicked and inert is the finding, and two round trips to assemble it
// would let the two halves come from different ranges.
func (e *Engine) clickFriction(ctx context.Context, req Request) (*Result, error) {
	if req.Path == "" && req.Page == "" {
		return nil, fmt.Errorf("query: click friction needs a page or a path")
	}

	rows, err := e.rawRows(ctx, req.Site, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	band := bandOf(req.Viewport)

	type click struct {
		ts      int64
		x, y    int32
		session string
		inert   bool
	}

	var clicks []click
	var measured bool

	for i := range rows {
		r := &rows[i]
		if r.Name != ingest.NameClick || !matchesPage(r, req) || r.Viewport <= 0 {
			continue
		}
		ts := time.UnixMilli(r.TSServer)
		if ts.Before(req.From) || !ts.Before(req.To) {
			continue
		}
		if req.Variant != "" && r.Variant != req.Variant {
			continue
		}
		if band != 0 && bandOf(r.Viewport) != band {
			continue
		}

		// A 1 can only have been written by a client that measured, so its presence anywhere
		// on the page is what tells us the 0s mean something. Without this an old page reads
		// as entirely dead, which is the most alarming possible way to be wrong.
		if r.Interactive == 1 {
			measured = true
		}
		clicks = append(clicks, click{
			ts: r.TSServer, x: r.ClickX, y: r.ClickY,
			session: r.Session, inert: r.Interactive == 0,
		})
	}

	out := &Friction{Clicks: int64(len(clicks)), Measured: measured, CellX: heatCellX, CellY: heatCellY}

	// --- dead clicks -----------------------------------------------------------------
	//
	// Only where the column was actually written. Reported per cell so the answer lines up
	// with the heatmap the reader is looking at.
	if measured {
		type tally struct {
			clicks   int64
			sessions map[string]bool
		}
		cells := map[[2]int32]*tally{}
		for _, c := range clicks {
			if !c.inert {
				continue
			}
			key := [2]int32{(c.x / heatCellX) * heatCellX, (c.y / heatCellY) * heatCellY}
			t := cells[key]
			if t == nil {
				t = &tally{sessions: map[string]bool{}}
				cells[key] = t
			}
			t.clicks++
			t.sessions[c.session] = true
		}
		for k, t := range cells {
			out.Dead = append(out.Dead, FrictionSpot{
				X: k[0], Y: k[1], Clicks: t.clicks, Sessions: int64(len(t.sessions)),
			})
		}
		out.Dead = mergeAdjacent(out.Dead)
		sortSpots(out.Dead)
	}

	// --- rage runs -------------------------------------------------------------------
	//
	// Per session, in time order: a run is one person clicking, and merging two visitors who
	// happened to click the same spot a second apart would manufacture them.
	bySession := map[string][]click{}
	for _, c := range clicks {
		bySession[c.session] = append(bySession[c.session], c)
	}

	type tally struct {
		clicks   int64
		sessions map[string]bool
	}
	runs := map[[2]int32]*tally{}

	for session, cs := range bySession {
		sort.Slice(cs, func(i, j int) bool { return cs[i].ts < cs[j].ts })

		for i := 0; i < len(cs); {
			// Extend a run while each click is close enough to the one that started it, in
			// both time and space. Anchored on the first click rather than the previous one,
			// so a slow drift across the page is not one long run.
			j := i + 1
			for j < len(cs) &&
				cs[j].ts-cs[i].ts <= rageWithin.Milliseconds() &&
				abs32(cs[j].x-cs[i].x) <= rageRadius &&
				abs32(cs[j].y-cs[i].y) <= heatCellY*2 {
				j++
			}

			if n := int64(j - i); n >= rageMinClicks {
				key := [2]int32{(cs[i].x / heatCellX) * heatCellX, (cs[i].y / heatCellY) * heatCellY}
				t := runs[key]
				if t == nil {
					t = &tally{sessions: map[string]bool{}}
					runs[key] = t
				}
				t.clicks += n
				t.sessions[session] = true
			}

			// Past the run, never one click at a time: overlapping runs would count the same
			// clicks several times and turn one impatient moment into a hot spot.
			if j > i+1 {
				i = j
			} else {
				i++
			}
		}
	}

	for k, t := range runs {
		out.Rage = append(out.Rage, FrictionSpot{
			X: k[0], Y: k[1], Clicks: t.clicks, Sessions: int64(len(t.sessions)),
		})
	}
	out.Rage = mergeAdjacent(out.Rage)
	sortSpots(out.Rage)

	return &Result{Widget: req.Widget, Site: req.Site, Friction: out}, nil
}

// mergeAdjacent joins touching cells into one spot.
//
// The grid is the heatmap's, and it is fine for drawing: adjacent cells become one blob on a
// canvas because the blobs overlap. A ranked LIST has no such mercy — a button that happens to
// straddle a cell boundary appears as two half-strength spots, neither of which looks like the
// problem it is, and which one is reported first comes down to where the button sits rather
// than how bad it is.
//
// A plain flood fill over the eight neighbours, which is enough: these lists are short, and
// the alternative — clustering on the raw coordinates — would give the spot a position no
// click actually had.
func mergeAdjacent(spots []FrictionSpot) []FrictionSpot {
	if len(spots) < 2 {
		return spots
	}

	at := make(map[[2]int32]int, len(spots))
	for i, s := range spots {
		at[[2]int32{s.X, s.Y}] = i
	}

	seen := make([]bool, len(spots))
	var out []FrictionSpot

	for i := range spots {
		if seen[i] {
			continue
		}
		seen[i] = true

		// The merged spot takes the position of the busiest cell in the group, so it lands
		// where the clicks actually were rather than at an average of two places.
		group := []int{i}
		for k := 0; k < len(group); k++ {
			s := spots[group[k]]
			for dx := int32(-1); dx <= 1; dx++ {
				for dy := int32(-1); dy <= 1; dy++ {
					if dx == 0 && dy == 0 {
						continue
					}
					j, ok := at[[2]int32{s.X + dx*heatCellX, s.Y + dy*heatCellY}]
					if !ok || seen[j] {
						continue
					}
					seen[j] = true
					group = append(group, j)
				}
			}
		}

		merged := spots[group[0]]
		for _, j := range group[1:] {
			if spots[j].Clicks > merged.Clicks {
				merged.X, merged.Y = spots[j].X, spots[j].Y
			}
			merged.Clicks += spots[j].Clicks
			// Sessions are added rather than unioned: the same visit clicking two adjacent
			// cells would be counted twice. Accepted, and it is why the merge is only over
			// TOUCHING cells — the error is bounded by how far one hand can miss by, and
			// carrying every session id through the merge to avoid it would cost more memory
			// than the whole answer.
			merged.Sessions += spots[j].Sessions
		}
		out = append(out, merged)
	}
	return out
}

// sortSpots ranks by how many different visits produced a spot, then by clicks, then by
// position so the answer is byte-identical between a cached response and a fresh one.
func sortSpots(s []FrictionSpot) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Sessions != s[j].Sessions {
			return s[i].Sessions > s[j].Sessions
		}
		if s[i].Clicks != s[j].Clicks {
			return s[i].Clicks > s[j].Clicks
		}
		if s[i].Y != s[j].Y {
			return s[i].Y < s[j].Y
		}
		return s[i].X < s[j].X
	})
}

func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
