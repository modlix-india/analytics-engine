package query

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/modlix-india/analytics-engine/internal/objstore"

	"github.com/modlix-india/analytics-engine/internal/event"
	"github.com/modlix-india/analytics-engine/internal/rollup"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// Widget names. The complete set a caller may ask for — there is no free-form query, by
// design, and this list is the read surface in its entirety.
const (
	// Web analytics.
	WidgetPageviewsOverTime = "pageviewsOverTime"
	WidgetTopPages          = "topPages"
	WidgetTopReferrers      = "topReferrers"
	WidgetChannelBreakdown  = "channelBreakdown"
	WidgetDeviceBreakdown   = "deviceBreakdown"
	WidgetBrowserBreakdown  = "browserBreakdown"
	WidgetOSBreakdown       = "osBreakdown"
	WidgetGeoBreakdown      = "geoBreakdown"

	// Product analytics.
	WidgetEventTimeline       = "eventTimeline"
	WidgetTopEvents           = "topEvents"
	WidgetBreakdownByProperty = "breakdownByProperty"

	// Per-visitor analyses. These cannot be answered from rollups — they depend on the order
	// and spacing of one visitor's events — so they read raw Parquet through a narrow
	// projection. Their counts are EXACT rather than sketch estimates.
	// Where clicks landed on one page, and which pages have any. Neither reads rollups:
	// coordinates are not a dimension you can sum, and a rollup has thrown them away.
	WidgetHeatmap      = "heatmap"
	WidgetHeatmapPages = "heatmapPages"

	WidgetFunnel     = "funnel"
	WidgetRetention  = "retention"
	WidgetStickiness = "stickiness"
	WidgetLifecycle  = "lifecycle"

	// The visit analyses. Same sort-merge, partitioned on the session rather than the
	// visitor — which is why these four are answerable while retention, stickiness and
	// lifecycle are not: everything they ask about happens inside one visit, and the
	// visitor salt rotation cannot reach inside one.
	// Where a page is frustrating, as a hypothesis rather than a finding. Same page, same
	// grid and same bands as the heatmap, because it is drawn over it.
	WidgetClickFriction = "clickFriction"

	// How far down a page people got. Raw and per-page, like the heatmap, and bounded the
	// same way: one page at a time, asked for by somebody looking at it.
	WidgetScrollDepth = "scrollDepth"
	WidgetScrollPages = "scrollPages"

	WidgetEntryPages    = "entryPages"
	WidgetExitPages     = "exitPages"
	WidgetPagesPerVisit = "pagesPerVisit"
	WidgetVisitLength   = "visitLength"

	// Not a PostHog port. The mobile WebView stamps its identity into the user-agent, so
	// "how many of my users are on the app, and how many are on an old version" becomes an
	// ordinary rollup dimension rather than a guess from the OS string.
	WidgetPlatformBreakdown = "platformBreakdown"
	WidgetAppVersions       = "appVersionBreakdown"
)

// breakdownDims maps a widget to the dimension it groups by.
//
// A map rather than a switch because breakdownByProperty lets the caller name a dimension, and
// that name must be validated against exactly this set. Without the allow-list a caller could
// ask for any column the rollup happens to carry, which is a slow drift from "fixed widget
// API" back to "arbitrary query".
var breakdownDims = map[string]string{
	WidgetTopPages:          rollup.DimPath,
	WidgetTopReferrers:      rollup.DimReferrer,
	WidgetChannelBreakdown:  rollup.DimChannel,
	WidgetDeviceBreakdown:   rollup.DimDevice,
	WidgetBrowserBreakdown:  rollup.DimBrowser,
	WidgetOSBreakdown:       rollup.DimOS,
	WidgetGeoBreakdown:      rollup.DimCountry,
	WidgetPlatformBreakdown: rollup.DimPlatform,
	WidgetAppVersions:       rollup.DimAppVersion,
}

// propertyDims are the dimensions breakdownByProperty may name.
var propertyDims = map[string]bool{
	rollup.DimPath: true, rollup.DimPage: true, rollup.DimLabel: true,
	rollup.DimReferrer: true, rollup.DimChannel: true,
	rollup.DimUTMSource: true, rollup.DimUTMMedium: true, rollup.DimUTMCampaign: true,
	rollup.DimDevice: true, rollup.DimBrowser: true, rollup.DimOS: true,
	rollup.DimPlatform: true, rollup.DimAppVersion: true, rollup.DimCountry: true,
	rollup.DimExperiment: true, rollup.DimVariant: true,
}

type Request struct {
	Widget string
	Site   string

	// From and To are absolute and half-open, [From, To).
	From, To time.Time

	// Timezone is the site's reporting zone, IANA-named. Empty means UTC. It affects only
	// which buckets are summed, never what is stored — which is the point of keeping rollups
	// at a finer granularity than the reporting unit.
	Timezone string

	Event string // defaults to $pageview

	// Property names the dimension for breakdownByProperty. Ignored by every other widget,
	// and refused unless it is in propertyDims.
	Property string

	// Steps and WindowHours are the funnel's parameters: an ordered event sequence, and how
	// long a visitor has from the first step to complete the rest.
	Steps       []string
	WindowHours int

	// Period is "day" or "week", for retention, stickiness and lifecycle.
	Period string

	// Path names the ADDRESS a heatmap is for. Used when no Page is given, which is what an
	// event recorded before the application reported its own page name looks like.
	Path string

	// Page names the page a heatmap is for, and takes precedence over Path. An address is
	// not a page: routing serves two different definitions at one URL, so a heatmap keyed on
	// the address draws both arms of an A/B test over one of their two layouts.
	Page string

	// Variant narrows a heatmap to one arm of an experiment. Empty means every arm together,
	// which is the right default: a page with no experiment running has one arm named "".
	Variant string

	// Viewport picks the width band to draw — 600, 1024, or 0 for the widest. Clicks are only
	// comparable within a band, because a band is a layout.
	Viewport int32

	// Compare asks for the same question answered over the preceding period as well.
	//
	// "previous" is the only value, and an unknown one is refused rather than ignored: a
	// misspelling that silently returned no comparison would look exactly like a period with
	// no data in it.
	Compare string

	Limit int
}

// ComparePrevious is the one comparison this engine offers.
const ComparePrevious = "previous"

// comparable lists the widgets a comparison may be asked for.
//
// The rollup-backed ones only. A comparison doubles the work, and doubling a rollup read is
// two more cheap scans of pre-aggregated buckets, where doubling a funnel or a heatmap is a
// second pass over raw Parquet — the expensive thing this engine is careful about. If those
// are wanted later they can be added here; refusing now is honest about what it would cost.
var comparable = map[string]bool{
	WidgetPageviewsOverTime: true, WidgetEventTimeline: true, WidgetTopEvents: true,
	WidgetTopPages: true, WidgetTopReferrers: true, WidgetChannelBreakdown: true,
	WidgetDeviceBreakdown: true, WidgetBrowserBreakdown: true, WidgetOSBreakdown: true,
	WidgetGeoBreakdown: true, WidgetPlatformBreakdown: true, WidgetAppVersions: true,
	WidgetBreakdownByProperty: true,
}

type Row struct {
	Label    string `json:"label"`
	Events   int64  `json:"events"`
	Visitors uint64 `json:"visitors"`

	// Visits is the number of sessions, and it is set only on headline rows — the totals and
	// the time series — because only those carry a session sketch.
	//
	// Breakdown rows deliberately do not: a second sketch on every one of them doubled the
	// rollup tier for a figure almost nobody reads, so "visits to /pricing" comes from the
	// visit widgets' raw scan instead. The field is omitted rather than zeroed there, so a
	// reader can tell "no sessions" from "not measured at this grain".
	Visits uint64 `json:"visits,omitempty"`

	// Page and Path are set only by the heatmap page list, where one label is not enough to
	// act on: the page says which clicks to ask for, and the path says where to open it. A
	// page routed to from an address is not the same thing as the address.
	Page string `json:"page,omitempty"`
	Path string `json:"path,omitempty"`
}

// Audience is how big the site was over the same range: what a rate is a share OF.
//
// Returned on the event widgets, because those are where somebody works out a conversion
// rate, and a rate computed from two separate queries is a rate whose denominator nobody
// checked. One request, one denominator, one definition of who counted.
//
// Unlike PeriodTotal this DOES carry visitors, and legitimately: it is the union of the
// hourly sketches for page views over the range, not a sum across keys. The daily identity
// rotation still applies, which is what VisitorsDaily on the same answer is saying.
type Audience struct {
	Visitors uint64 `json:"visitors"`
	Visits   uint64 `json:"visits"`
	Views    int64  `json:"views"`
}

// PeriodTotal is a whole period in one line.
//
// Deliberately NOT a Row, and the missing field is the point: there is no visitor count here.
// Uniques do not add — summing them over keys counts one person once per page they read, and
// summing them over days counts them once per day, which the daily identity rotation makes
// unavoidable anyway. A Row would have carried a `visitors: 0` into every response, and a zero
// in a field somebody is reading is worse than no field at all.
type PeriodTotal struct {
	Events int64 `json:"events"`

	// Visits is the number of sessions, present only where the rows carried one.
	Visits uint64 `json:"visits,omitempty"`
}

type Result struct {
	Widget string `json:"widget"`
	Site   string `json:"site"`
	Rows   []Row  `json:"rows"`

	Funnel     []FunnelStep       `json:"funnel,omitempty"`
	Retention  []RetentionCohort  `json:"retention,omitempty"`
	Stickiness []StickinessBucket `json:"stickiness,omitempty"`
	Lifecycle  []LifecyclePoint   `json:"lifecycle,omitempty"`
	Heatmap    *Heatmap           `json:"heatmap,omitempty"`
	Scroll     *Scroll            `json:"scroll,omitempty"`
	Friction   *Friction          `json:"friction,omitempty"`

	// RangeRelative marks a result whose meaning depends on the queried window rather than
	// on all of history — lifecycle, where a long-standing visitor counts as new if their
	// first event inside the range is in its first period.
	RangeRelative bool `json:"rangeRelative,omitempty"`

	// VisitorsApproximate is always true when any row carries a visitor count, and it is
	// surfaced rather than buried: those figures come from HyperLogLog sketches, and a
	// dashboard that presents an estimate as exact invites somebody to reconcile it against
	// another tool and conclude the engine is broken.
	VisitorsApproximate bool `json:"visitorsApproximate"`

	// RawHoursScanned reports how many partial hours needed a raw scan. Zero for every
	// whole-hour timezone; two for a half-hour one, however long the range.
	RawHoursScanned int `json:"rawHoursScanned"`

	// Total sums EVERY key in the period, including the ones the limit cut off.
	//
	// Set on every rollup-backed answer, whether or not a comparison was asked for, because a
	// dashboard that adds up the top ten to produce a headline is wrong on exactly the sites
	// with a long tail — and it is wrong in a way that gets worse as the site grows.
	Total *PeriodTotal `json:"total,omitempty"`

	// Previous holds the same rows for the preceding period, ALIGNED INDEX FOR INDEX with
	// Rows, and is present only when the request asked to compare.
	//
	// Aligned rather than ranked on its own, because the two are read together: a chart draws
	// them as two series and a table takes a difference per line. A second independently
	// sorted top-ten would put last month's best page beside this month's third best, and the
	// resulting delta would be arithmetic performed on two different things.
	//
	// A key that existed then and does not now is therefore absent from this list. That is a
	// real loss of information, and it is the right trade for a widget whose question is "what
	// is biggest NOW": PreviousTotal covers the case where the reader only wants to know
	// whether the whole period moved.
	Previous []Row `json:"previous,omitempty"`

	// PreviousTotal sums the preceding period across EVERY key, not only the ones listed in
	// Previous, so a headline delta is not quietly computed over the top ten alone.
	PreviousTotal *PeriodTotal `json:"previousTotal,omitempty"`

	// Audience is the site's own size over this range, for turning a count into a rate.
	Audience *Audience `json:"audience,omitempty"`

	// Denominator is what the rows are a share OF, where that is a definite number.
	//
	// Exit pages over visits is the example that motivates it: the share is the question
	// ("what proportion of visits end here"), and a caller left to divide by a total from a
	// second query would sometimes divide by a total that was computed over a different set of
	// visits. One query, one denominator, one definition of which visits counted.
	Denominator int64 `json:"denominator,omitempty"`

	// VisitorsDaily says that a visitor count in this answer counts visitor-DAYS, not people.
	//
	// Visitor identity is derived from a salt that rotates at UTC midnight, so somebody who
	// came on Monday and again on Tuesday is two visitors. Within one day the figure is what
	// it looks like; across a range it is the sum of each day's uniques, and it exceeds the
	// number of people by however often they came back.
	//
	// Reported rather than corrected, because it cannot be corrected here: the information
	// needed to link the two is deliberately destroyed at ingest. A dashboard showing this
	// figure under the word "Visitors" without saying so is the thing this flag exists to
	// prevent, and it is the same contract as VisitorsApproximate.
	VisitorsDaily bool `json:"visitorsDaily,omitempty"`
}

type Engine struct {
	DataDir string

	// Remote, when set, is consulted for partitions that are not on local disk.
	//
	// This is what makes a node able to answer for history it never ingested: local retention
	// prunes old Parquet, the bucket keeps it, and a query transparently reaches through. It
	// is also why adding a node is a configuration change — the new one serves every site's
	// past from the same bucket without any backfill.
	Remote objstore.Store

	// CacheDir holds files fetched from Remote. Defaults to DataDir/cache.
	CacheDir string

	// DefaultTimezone is used when a request names none. Empty means UTC.
	//
	// A default rather than a required field because a query with no zone still has to mean
	// something definite. What it must NOT do is follow the host's zone: the same question
	// would then return different day boundaries on two machines, and nobody could reproduce
	// a number someone else was looking at.
	DefaultTimezone string

	Log *slog.Logger
}

func New(dataDir string) *Engine { return &Engine{DataDir: dataDir} }

func (e *Engine) Query(ctx context.Context, req Request) (*Result, error) {
	if req.Site == "" {
		return nil, fmt.Errorf("query: site is required")
	}
	if !req.To.After(req.From) {
		return nil, fmt.Errorf("query: To must be after From")
	}
	switch req.Widget {
	case WidgetFunnel, WidgetRetention, WidgetStickiness, WidgetLifecycle,
		WidgetHeatmap, WidgetHeatmapPages, WidgetScrollDepth, WidgetScrollPages,
		WidgetClickFriction,
		WidgetEntryPages, WidgetExitPages, WidgetPagesPerVisit, WidgetVisitLength:
		// Left empty deliberately: for these, "" means ANY event, which is the usual
		// question. Defaulting to $pageview here would make that impossible to express.
	default:
		if req.Event == "" {
			req.Event = event.NamePageview
		}
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	loc, err := loadLocation(cmp.Or(req.Timezone, e.DefaultTimezone))
	if err != nil {
		return nil, err
	}

	res, err := e.dispatch(ctx, req, loc)
	if err != nil {
		return nil, err
	}

	if req.Compare != "" {
		if err := e.attachComparison(ctx, req, loc, res); err != nil {
			return nil, err
		}
	}

	// Only the event widgets. Everything else already answers in the unit its reader is
	// thinking in — a breakdown of pages against each other needs no site-wide base, and
	// fetching one would be a second rollup read on every tile of the dashboard.
	if req.Widget == WidgetTopEvents || req.Widget == WidgetEventTimeline {
		aud, err := e.audience(ctx, req)
		if err != nil {
			return nil, err
		}
		res.Audience = aud
	}

	// Set once, here, rather than in each widget. It depends only on the range and on a
	// property of ingest, so deriving it per widget would be nine places to forget it.
	res.VisitorsDaily = spansSaltRotation(req.From, req.To)
	return res, nil
}

// attachComparison answers the same question over the preceding period and aligns it.
func (e *Engine) attachComparison(ctx context.Context, req Request, loc *time.Location, res *Result) error {
	if req.Compare != ComparePrevious {
		return fmt.Errorf("query: compare must be %q, not %q", ComparePrevious, req.Compare)
	}
	if !comparable[req.Widget] {
		return fmt.Errorf("query: %s cannot be compared against the previous period; "+
			"it reads raw events rather than rollups, and a second pass would cost as much as the first",
			req.Widget)
	}

	prevReq := req
	prev := previousSpan(span{req.From, req.To}, loc)
	prevReq.From, prevReq.To = prev.Start, prev.End
	// Every key, not the top N: PreviousTotal has to cover the whole period or a headline
	// delta computed from it is a delta of two different things.
	prevReq.Limit = 1 << 30

	prevRes, err := e.dispatch(ctx, prevReq, loc)
	if err != nil {
		return err
	}

	// Taken from the previous answer's own total, which was computed over every key before
	// its limit was applied — the same rule as Total, so the two are comparable by
	// construction rather than by coincidence.
	res.PreviousTotal = prevRes.Total

	// A time series is aligned by POSITION — the first day of one period against the first of
	// the other — and everything else by LABEL. Using labels for a time series would align
	// nothing at all, since the two periods share no dates by construction.
	if req.Widget == WidgetPageviewsOverTime || req.Widget == WidgetEventTimeline {
		res.Previous = alignByPosition(prevRes.Rows, len(res.Rows))
		return nil
	}
	res.Previous = alignByLabel(prevRes.Rows, res.Rows)
	return nil
}

// alignByPosition pads or truncates to n rows, so a caller can index both series together
// without checking a length it did not choose.
func alignByPosition(prev []Row, n int) []Row {
	out := make([]Row, n)
	for i := range out {
		if i < len(prev) {
			out[i] = prev[i]
		}
		// The label is the CURRENT period's business; leaving the previous period's date here
		// would put two different dates on one column of a chart.
		out[i].Label = ""
	}
	return out
}

// alignByLabel emits one row per current row, zero where the key did not exist before. A zero
// is the true answer for a page that is new this period, and it is what makes the delta read
// as "all of it is new" rather than as a missing row.
func alignByLabel(prev, current []Row) []Row {
	byKey := make(map[string]Row, len(prev))
	for _, r := range prev {
		byKey[r.Label] = r
	}

	out := make([]Row, len(current))
	for i, r := range current {
		p := byKey[r.Label]
		p.Label = r.Label
		out[i] = p
	}
	return out
}

// spansSaltRotation reports whether a range crosses a UTC midnight, which is where the visitor
// salt rotates and therefore where one person becomes two.
//
// UTC, not the reporting zone: the rotation happens on the engine's clock regardless of which
// day boundary the reader is asking about. A single IST day spans a UTC midnight and is
// therefore affected, which is exactly the sort of thing a reporting-zone comparison would
// have hidden.
func spansSaltRotation(from, to time.Time) bool {
	return !from.UTC().Truncate(24 * time.Hour).Equal(to.UTC().Add(-time.Nanosecond).Truncate(24 * time.Hour))
}

func (e *Engine) dispatch(ctx context.Context, req Request, loc *time.Location) (*Result, error) {
	// Both time series are the same code path; they differ only in which event they default
	// to, so keeping two names is about the caller's vocabulary, not about behaviour.
	if req.Widget == WidgetPageviewsOverTime || req.Widget == WidgetEventTimeline {
		return e.overTime(ctx, req, loc)
	}
	if dim, ok := breakdownDims[req.Widget]; ok {
		return e.breakdown(ctx, req, dim)
	}

	switch req.Widget {
	case WidgetFunnel:
		return e.funnel(ctx, req, loc)
	case WidgetRetention:
		return e.retention(ctx, req, loc)
	case WidgetStickiness:
		return e.stickiness(ctx, req, loc)
	case WidgetLifecycle:
		res, err := e.lifecycle(ctx, req, loc)
		if err == nil {
			res.RangeRelative = true
		}
		return res, err
	case WidgetTopEvents:
		return e.topEvents(ctx, req)
	case WidgetClickFriction:
		return e.clickFriction(ctx, req)
	case WidgetScrollDepth:
		return e.scrollDepth(ctx, req)
	case WidgetScrollPages:
		return e.scrollPages(ctx, req)
	case WidgetEntryPages:
		return e.entryPages(ctx, req)
	case WidgetExitPages:
		return e.exitPages(ctx, req)
	case WidgetPagesPerVisit:
		return e.pagesPerVisit(ctx, req)
	case WidgetVisitLength:
		return e.visitLength(ctx, req)
	case WidgetHeatmap:
		return e.heatmap(ctx, req)
	case WidgetHeatmapPages:
		return e.heatmapPages(ctx, req)
	case WidgetBreakdownByProperty:
		if !propertyDims[req.Property] {
			return nil, fmt.Errorf("query: %q is not a dimension this engine breaks down by", req.Property)
		}
		return e.breakdown(ctx, req, req.Property)
	default:
		return nil, fmt.Errorf("query: unknown widget %q", req.Widget)
	}
}

// audience counts the page views, visitors and visits of the whole site over the range.
//
// From the DimNone rows for $pageview, which is where the headline sketches live: a union of
// hourly sketches over the range rather than a sum over keys, so it is a real unique count
// within the limit the daily salt rotation sets.
func (e *Engine) audience(ctx context.Context, req Request) (*Audience, error) {
	merged, _, err := e.aggregate(ctx, req.Site, event.NamePageview, rollup.DimNone, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	var a Audience
	for _, m := range merged {
		a.Views += m.Events
		a.Visitors += m.Visitors
		a.Visits += m.Sessions
	}
	return &a, nil
}

// topEvents ranks event names rather than the values of one dimension.
//
// It reads the same DimNone rows every other widget uses for totals, but merges them keyed by
// event name instead of by dimension value. Remapping Key and reusing MergeByKey keeps one
// merge implementation rather than two that could drift.
func (e *Engine) topEvents(ctx context.Context, req Request) (*Result, error) {
	rows, rawHours, err := e.collect(ctx, req.Site, allEvents, rollup.DimNone, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	for i := range rows {
		rows[i].Key = rows[i].Event
	}
	merged, err := rollup.MergeByKey(rows)
	if err != nil {
		return nil, err
	}

	res := &Result{
		Widget: req.Widget, Site: req.Site,
		VisitorsApproximate: true, RawHoursScanned: rawHours,
		Total: totalOf(merged),
	}
	for i, m := range merged {
		if i >= req.Limit {
			break
		}
		res.Rows = append(res.Rows, Row{
			Label: m.Key, Events: m.Events, Visitors: m.Visitors, Visits: m.Sessions,
		})
	}
	return res, nil
}

// overTime produces one row per local calendar day.
//
// Each day is planned separately, because each has its own boundary hours — and its own
// length, since a DST day is 23 or 25 hours.
func (e *Engine) overTime(ctx context.Context, req Request, loc *time.Location) (*Result, error) {
	res := &Result{Widget: req.Widget, Site: req.Site, VisitorsApproximate: true}

	for _, day := range daySpans(span{req.From, req.To}, loc) {
		merged, rawHours, err := e.aggregate(ctx, req.Site, req.Event, rollup.DimNone, day)
		if err != nil {
			return nil, err
		}
		res.RawHoursScanned += rawHours

		var row Row
		row.Label = day.Start.In(loc).Format("2006-01-02")
		for _, m := range merged {
			row.Events += m.Events
			row.Visitors += m.Visitors
			row.Visits += m.Sessions
		}
		res.Rows = append(res.Rows, row)
	}

	// Every day is listed, so summing the rows IS every key. Visitors are deliberately left
	// out for the reason totalOf gives: a visitor active on three days would count three
	// times, and the union those sketches would give is a different question from this one.
	total := &PeriodTotal{}
	for _, r := range res.Rows {
		total.Events += r.Events
		total.Visits += r.Visits
	}
	res.Total = total
	return res, nil
}

// breakdown produces top-N rows for one dimension over the whole range.
func (e *Engine) breakdown(ctx context.Context, req Request, dim string) (*Result, error) {
	merged, rawHours, err := e.aggregate(ctx, req.Site, req.Event, dim, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	res := &Result{
		Widget: req.Widget, Site: req.Site,
		VisitorsApproximate: true, RawHoursScanned: rawHours,
		Total: totalOf(merged),
	}
	for i, m := range merged {
		if i >= req.Limit {
			break
		}
		res.Rows = append(res.Rows, Row{Label: m.Key, Events: m.Events, Visitors: m.Visitors})
	}
	return res, nil
}

// totalOf sums every key, which is what a headline number is.
//
// Visitors are NOT summed across keys — that would count one person once per page they read.
// A unique count over a set of sketches is the union of those sketches, and the union is
// already available as the DimNone row, so the honest thing here is to leave it out rather
// than to add up something that does not add. Events do add.
func totalOf(merged []rollup.Merged) *PeriodTotal {
	t := &PeriodTotal{}
	for _, m := range merged {
		t.Events += m.Events
	}
	return t
}

// aggregate is where the two tiers meet: whole hours from rollups, boundary remainders from
// raw. Callers never see the seam.
func (e *Engine) aggregate(ctx context.Context, site, event, dim string, s span) ([]rollup.Merged, int, error) {
	rows, rawHours, err := e.collect(ctx, site, event, dim, s)
	if err != nil {
		return nil, 0, err
	}
	merged, err := rollup.MergeByKey(rows)
	return merged, rawHours, err
}

// allEvents tells collect not to filter by event name. A sentinel rather than an empty string,
// because "" is a legitimate dimension value and confusing the two would silently widen a
// query.
const allEvents = "\x00all"

// collect gathers the rollup rows matching a query, correcting the partial boundary hours from
// raw data. It stops short of merging so a caller can re-key first.
func (e *Engine) collect(ctx context.Context, site, event, dim string, s span) ([]rollup.Row, int, error) {
	plan := planHours(s)

	hours := make(map[int64]bool, len(plan.FullHours))
	for _, h := range plan.FullHours {
		hours[h] = true
	}

	var rows []rollup.Row
	for _, date := range utcDates(s) {
		files, err := e.filesIn(ctx, "rollup", site, date)
		if err != nil {
			return nil, 0, err
		}
		for _, f := range files {
			rs, err := rollup.ReadFile(f)
			if err != nil {
				return nil, 0, fmt.Errorf("read rollup %s: %w", filepath.Base(f), err)
			}
			for i := range rs {
				r := &rs[i]
				if (event == allEvents || r.Event == event) && r.Dim == dim && inHours(r, hours) {
					rows = append(rows, *r)
				}
			}
		}
	}

	// The partial hours a rollup bucket cannot answer. Synthesised into rollup rows so the
	// merge below sees one uniform input, which keeps the correction from becoming a second
	// code path that could disagree with the first.
	for _, part := range plan.Partials {
		raw, err := e.rawRows(ctx, site, part)
		if err != nil {
			return nil, 0, err
		}
		filtered := raw[:0]
		for _, r := range raw {
			ts := time.UnixMilli(r.TSServer)
			if (event == allEvents || r.Name == event) && !ts.Before(part.Start) && ts.Before(part.End) {
				filtered = append(filtered, r)
			}
		}
		for _, r := range rollup.Build(filtered) {
			if r.Dim == dim && (event == allEvents || r.Event == event) {
				rows = append(rows, r)
			}
		}
	}

	return rows, len(plan.Partials), nil
}

func (e *Engine) rawRows(ctx context.Context, site string, s span) ([]store.Row, error) {
	var out []store.Row
	for _, date := range utcDates(s) {
		files, err := e.filesIn(ctx, "data", site, date)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			rs, err := store.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("read raw %s: %w", filepath.Base(f), err)
			}
			out = append(out, rs...)
		}
	}
	return out, nil
}

// filesIn lists one partition, from local disk and, where configured, from object storage.
//
// A missing local directory is not an error: a site with no traffic on a date simply has none,
// and that is what every range edge looks like. Nor is it evidence the data does not exist —
// local retention prunes old partitions while the bucket keeps them, so the remote listing is
// consulted whenever a store is configured.
func (e *Engine) filesIn(ctx context.Context, tier, site, date string) ([]string, error) {
	safeSite, safeDate := store.SafeSegment(site), store.SafeSegment(date)
	dir := filepath.Join(e.DataDir, tier, safeSite, safeDate)

	out, have, err := listParquet(dir)
	if err != nil {
		return nil, err
	}

	if e.Remote == nil {
		sort.Strings(out)
		return out, nil
	}

	prefix := tier + "/" + safeSite + "/" + safeDate + "/"
	objs, err := e.Remote.List(ctx, prefix)
	if err != nil {
		// A bucket that is briefly unreachable should degrade to whatever is local rather
		// than failing the query outright — partial data with a warning beats none.
		if e.Log != nil {
			e.Log.Warn("query: listing object storage failed; answering from local data only",
				"prefix", prefix, "err", err)
		}
		sort.Strings(out)
		return out, nil
	}

	for _, o := range objs {
		base := path.Base(o.Key)
		if have[base] {
			continue
		}
		cached, err := e.fetch(ctx, o.Key, tier, safeSite, safeDate, base)
		if err != nil {
			return nil, err
		}
		out = append(out, cached)
	}

	sort.Strings(out)
	return out, nil
}

// fetch downloads one object into the local cache, if it is not already there.
//
// Written to a temporary name and renamed, for the same reason compaction does: two queries
// can want the same cold partition at once, and a reader must never open a half-written file.
func (e *Engine) fetch(ctx context.Context, key, tier, site, date, base string) (string, error) {
	cacheDir := e.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(e.DataDir, "cache")
	}
	dir := filepath.Join(cacheDir, tier, site, date)
	dst := filepath.Join(dir, base)

	if _, err := os.Stat(dst); err == nil {
		return dst, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}

	rc, err := e.Remote.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", key, err)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp(dir, ".fetch-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())

	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", err
	}
	return dst, nil
}

func listParquet(dir string) ([]string, map[string]bool, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, map[string]bool{}, nil
	}
	if err != nil {
		return nil, nil, err
	}

	var out []string
	have := map[string]bool{}
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".parquet" {
			continue
		}
		have[ent.Name()] = true
		out = append(out, filepath.Join(dir, ent.Name()))
	}
	return out, have, nil
}
