package query

import (
	"fmt"
	"sort"
	"time"
)

// FunnelStep is one step's result.
type FunnelStep struct {
	Step     int    `json:"step"`
	Event    string `json:"event"`
	Visitors uint64 `json:"visitors"`

	// FromPrevious is the share of the previous step's visitors who reached this one, and
	// FromFirst the share of those who entered the funnel at all. Both are reported because
	// they answer different questions — where the biggest single drop is, against how much of
	// the original cohort survives — and a dashboard showing only one invites the other to be
	// computed wrongly by hand.
	FromPrevious float64 `json:"conversionFromPrevious"`
	FromFirst    float64 `json:"conversionFromFirst"`
}

// RetentionCohort is one row of the retention grid.
type RetentionCohort struct {
	Label  string   `json:"label"`
	Size   uint64   `json:"size"`
	Values []uint64 `json:"values"`
}

// StickinessBucket counts visitors by how many distinct periods they were active in.
type StickinessBucket struct {
	Periods  int    `json:"periods"`
	Visitors uint64 `json:"visitors"`
}

// LifecyclePoint splits one period's activity by where those visitors came from.
type LifecyclePoint struct {
	Label        string `json:"label"`
	New          uint64 `json:"new"`
	Returning    uint64 `json:"returning"`
	Resurrecting uint64 `json:"resurrecting"`
	Dormant      uint64 `json:"dormant"`
}

// funnel counts how far each visitor got through an ordered sequence.
func (e *Engine) funnel(req Request, loc *time.Location) (*Result, error) {
	if len(req.Steps) < 2 {
		return nil, fmt.Errorf("query: a funnel needs at least two steps")
	}
	if len(req.Steps) > 10 {
		return nil, fmt.Errorf("query: a funnel is limited to ten steps")
	}

	window := time.Duration(req.WindowHours) * time.Hour
	if req.WindowHours == 0 {
		// A week. A funnel with no window at all counts a visitor who returned six months
		// later as a conversion, which is rarely what anyone means.
		window = 7 * 24 * time.Hour
	}

	counts := make([]uint64, len(req.Steps))
	err := e.eachVisitor(req.Site, span{req.From, req.To}, func(_ string, evs []seqEvent) {
		for i := range funnelDepth(evs, req.Steps, window) {
			counts[i]++
		}
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site}
	for i, step := range req.Steps {
		fs := FunnelStep{Step: i + 1, Event: step, Visitors: counts[i]}
		if i > 0 && counts[i-1] > 0 {
			fs.FromPrevious = float64(counts[i]) / float64(counts[i-1])
		} else if i == 0 {
			fs.FromPrevious = 1
		}
		if counts[0] > 0 {
			fs.FromFirst = float64(counts[i]) / float64(counts[0])
		}
		res.Funnel = append(res.Funnel, fs)
	}
	return res, nil
}

// retention groups visitors by the period they first appeared and tracks who came back.
func (e *Engine) retention(req Request, loc *time.Location) (*Result, error) {
	weekly := req.Period == "week"
	periods := periodsBetween(span{req.From, req.To}, loc, weekly)
	if len(periods) == 0 {
		return &Result{Widget: req.Widget, Site: req.Site}, nil
	}

	idx := make(map[string]int, len(periods))
	for i, p := range periods {
		idx[p] = i
	}

	size := make([]uint64, len(periods))
	grid := make([][]uint64, len(periods))
	for i := range grid {
		grid[i] = make([]uint64, len(periods))
	}

	err := e.eachVisitor(req.Site, span{req.From, req.To}, func(_ string, evs []seqEvent) {
		active := map[int]bool{}
		for _, ev := range evs {
			if req.Event != "" && ev.Name != req.Event {
				continue
			}
			if i, ok := idx[periodKey(ev.TS, loc, weekly)]; ok {
				active[i] = true
			}
		}
		if len(active) == 0 {
			return
		}

		first := len(periods)
		for i := range active {
			if i < first {
				first = i
			}
		}
		size[first]++
		for i := range active {
			grid[first][i-first]++
		}
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site}
	for i, label := range periods {
		// A cohort can only be observed for the periods that follow it inside the range, so
		// its row is shorter the later it starts. Padding to a full width would imply zero
		// retention where the truth is that nothing has been observed yet.
		res.Retention = append(res.Retention, RetentionCohort{
			Label: label, Size: size[i], Values: grid[i][:len(periods)-i],
		})
	}
	return res, nil
}

// stickiness counts, among visitors who did something at all, how many distinct periods they
// did it in. It answers habit formation rather than reach.
func (e *Engine) stickiness(req Request, loc *time.Location) (*Result, error) {
	weekly := req.Period == "week"
	hist := map[int]uint64{}

	err := e.eachVisitor(req.Site, span{req.From, req.To}, func(_ string, evs []seqEvent) {
		periods := map[string]bool{}
		for _, ev := range evs {
			if req.Event != "" && ev.Name != req.Event {
				continue
			}
			periods[periodKey(ev.TS, loc, weekly)] = true
		}
		if len(periods) > 0 {
			hist[len(periods)]++
		}
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site}
	keys := make([]int, 0, len(hist))
	for k := range hist {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		res.Stickiness = append(res.Stickiness, StickinessBucket{Periods: k, Visitors: hist[k]})
	}
	return res, nil
}

// lifecycle splits each period's visitors into new, returning, resurrecting and dormant.
//
// One honest limitation, which belongs in the response and not only here: every classification
// is relative to the QUERIED RANGE. A visitor who has used the site for years but whose first
// event inside the range is on day one counts as new. Reading activity from before the range
// would fix it and would mean scanning an unbounded history to answer a bounded question, so
// the range is the stated horizon instead.
func (e *Engine) lifecycle(req Request, loc *time.Location) (*Result, error) {
	weekly := req.Period == "week"
	periods := periodsBetween(span{req.From, req.To}, loc, weekly)
	if len(periods) == 0 {
		return &Result{Widget: req.Widget, Site: req.Site}, nil
	}

	idx := make(map[string]int, len(periods))
	for i, p := range periods {
		idx[p] = i
	}

	pts := make([]LifecyclePoint, len(periods))
	for i, label := range periods {
		pts[i].Label = label
	}

	err := e.eachVisitor(req.Site, span{req.From, req.To}, func(_ string, evs []seqEvent) {
		active := make([]bool, len(periods))
		any := false
		for _, ev := range evs {
			if req.Event != "" && ev.Name != req.Event {
				continue
			}
			if i, ok := idx[periodKey(ev.TS, loc, weekly)]; ok {
				active[i] = true
				any = true
			}
		}
		if !any {
			return
		}

		first := 0
		for !active[first] {
			first++
		}

		for i := first; i < len(periods); i++ {
			switch {
			case i == first:
				pts[i].New++
			case active[i] && active[i-1]:
				pts[i].Returning++
			case active[i]:
				// Active now, not last period, but seen before: they came back.
				pts[i].Resurrecting++
			case active[i-1]:
				// Was active last period and is not now.
				pts[i].Dormant++
			}
		}
	})
	if err != nil {
		return nil, err
	}

	res := &Result{Widget: req.Widget, Site: req.Site, Lifecycle: pts}
	return res, nil
}
