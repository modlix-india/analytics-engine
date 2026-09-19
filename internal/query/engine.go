package query

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

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
	WidgetFunnel     = "funnel"
	WidgetRetention  = "retention"
	WidgetStickiness = "stickiness"
	WidgetLifecycle  = "lifecycle"

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
	rollup.DimVariant: true,
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

	Limit int
}

type Row struct {
	Label    string `json:"label"`
	Events   int64  `json:"events"`
	Visitors uint64 `json:"visitors"`
}

type Result struct {
	Widget string `json:"widget"`
	Site   string `json:"site"`
	Rows   []Row  `json:"rows"`

	Funnel     []FunnelStep       `json:"funnel,omitempty"`
	Retention  []RetentionCohort  `json:"retention,omitempty"`
	Stickiness []StickinessBucket `json:"stickiness,omitempty"`
	Lifecycle  []LifecyclePoint   `json:"lifecycle,omitempty"`

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
}

type Engine struct {
	DataDir string
}

func New(dataDir string) *Engine { return &Engine{DataDir: dataDir} }

func (e *Engine) Query(req Request) (*Result, error) {
	if req.Site == "" {
		return nil, fmt.Errorf("query: site is required")
	}
	if !req.To.After(req.From) {
		return nil, fmt.Errorf("query: To must be after From")
	}
	switch req.Widget {
	case WidgetFunnel, WidgetRetention, WidgetStickiness, WidgetLifecycle:
		// Left empty deliberately: for these, "" means ANY event, which is the usual
		// question. Defaulting to $pageview here would make that impossible to express.
	default:
		if req.Event == "" {
			req.Event = "$pageview"
		}
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	loc, err := loadLocation(req.Timezone)
	if err != nil {
		return nil, err
	}

	// Both time series are the same code path; they differ only in which event they default
	// to, so keeping two names is about the caller's vocabulary, not about behaviour.
	if req.Widget == WidgetPageviewsOverTime || req.Widget == WidgetEventTimeline {
		return e.overTime(req, loc)
	}
	if dim, ok := breakdownDims[req.Widget]; ok {
		return e.breakdown(req, dim)
	}

	switch req.Widget {
	case WidgetFunnel:
		return e.funnel(req, loc)
	case WidgetRetention:
		return e.retention(req, loc)
	case WidgetStickiness:
		return e.stickiness(req, loc)
	case WidgetLifecycle:
		res, err := e.lifecycle(req, loc)
		if err == nil {
			res.RangeRelative = true
		}
		return res, err
	case WidgetTopEvents:
		return e.topEvents(req)
	case WidgetBreakdownByProperty:
		if !propertyDims[req.Property] {
			return nil, fmt.Errorf("query: %q is not a dimension this engine breaks down by", req.Property)
		}
		return e.breakdown(req, req.Property)
	default:
		return nil, fmt.Errorf("query: unknown widget %q", req.Widget)
	}
}

// topEvents ranks event names rather than the values of one dimension.
//
// It reads the same DimNone rows every other widget uses for totals, but merges them keyed by
// event name instead of by dimension value. Remapping Key and reusing MergeByKey keeps one
// merge implementation rather than two that could drift.
func (e *Engine) topEvents(req Request) (*Result, error) {
	rows, rawHours, err := e.collect(req.Site, allEvents, rollup.DimNone, span{req.From, req.To})
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
	}
	for i, m := range merged {
		if i >= req.Limit {
			break
		}
		res.Rows = append(res.Rows, Row{Label: m.Key, Events: m.Events, Visitors: m.Visitors})
	}
	return res, nil
}

// overTime produces one row per local calendar day.
//
// Each day is planned separately, because each has its own boundary hours — and its own
// length, since a DST day is 23 or 25 hours.
func (e *Engine) overTime(req Request, loc *time.Location) (*Result, error) {
	res := &Result{Widget: req.Widget, Site: req.Site, VisitorsApproximate: true}

	for _, day := range daySpans(span{req.From, req.To}, loc) {
		merged, rawHours, err := e.aggregate(req.Site, req.Event, rollup.DimNone, day)
		if err != nil {
			return nil, err
		}
		res.RawHoursScanned += rawHours

		var row Row
		row.Label = day.Start.In(loc).Format("2006-01-02")
		for _, m := range merged {
			row.Events += m.Events
			row.Visitors += m.Visitors
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
}

// breakdown produces top-N rows for one dimension over the whole range.
func (e *Engine) breakdown(req Request, dim string) (*Result, error) {
	merged, rawHours, err := e.aggregate(req.Site, req.Event, dim, span{req.From, req.To})
	if err != nil {
		return nil, err
	}

	res := &Result{
		Widget: req.Widget, Site: req.Site,
		VisitorsApproximate: true, RawHoursScanned: rawHours,
	}
	for i, m := range merged {
		if i >= req.Limit {
			break
		}
		res.Rows = append(res.Rows, Row{Label: m.Key, Events: m.Events, Visitors: m.Visitors})
	}
	return res, nil
}

// aggregate is where the two tiers meet: whole hours from rollups, boundary remainders from
// raw. Callers never see the seam.
func (e *Engine) aggregate(site, event, dim string, s span) ([]rollup.Merged, int, error) {
	rows, rawHours, err := e.collect(site, event, dim, s)
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
func (e *Engine) collect(site, event, dim string, s span) ([]rollup.Row, int, error) {
	plan := planHours(s)

	hours := make(map[int64]bool, len(plan.FullHours))
	for _, h := range plan.FullHours {
		hours[h] = true
	}

	var rows []rollup.Row
	for _, date := range utcDates(s) {
		files, err := e.filesIn("rollup", site, date)
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
		raw, err := e.rawRows(site, part)
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

func (e *Engine) rawRows(site string, s span) ([]store.Row, error) {
	var out []store.Row
	for _, date := range utcDates(s) {
		files, err := e.filesIn("data", site, date)
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

// filesIn lists one partition directory.
//
// A missing directory is not an error: a site with no traffic on a date simply has none, and
// that is the common case at the edges of every range.
func (e *Engine) filesIn(tier, site, date string) ([]string, error) {
	dir := filepath.Join(e.DataDir, tier, store.SafeSegment(site), store.SafeSegment(date))

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var out []string
	for _, ent := range entries {
		if !ent.IsDir() && filepath.Ext(ent.Name()) == ".parquet" {
			out = append(out, filepath.Join(dir, ent.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}
