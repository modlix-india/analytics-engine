// Package rollup pre-aggregates events so the common queries never read raw data.
//
// # Why hourly, and not daily
//
// A rollup discards the timestamps it summarises, so its bucket size is the finest question it
// can ever answer. Daily buckets keyed to UTC cannot be re-sliced into any other timezone's
// day — an IST day runs 18:30 UTC to 18:30 UTC, and a UTC daily total simply does not contain
// the information. The customer sees numbers that disagree with their own and nothing in the
// system can explain why. Hourly buckets answer every whole-hour offset exactly, and the
// half-hour offsets — India among them — are handled by correcting the two boundary hours from
// raw Parquet. See PLAN.md section 7.
//
// # Why partial rollups, merged at read time
//
// Each compaction emits rollup rows for the events it just wrote, and never updates an
// existing row. Queries sum across whatever partials exist. That buys three things at once:
// compaction stays idempotent (no read-modify-write to get half-done), nodes need no
// coordination to write into one bucket, and a site split across nodes is still counted
// correctly — because counts add and HyperLogLog sketches merge.
package rollup

import (
	"fmt"
	"sort"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// Dimension names. The empty dimension is the event's own total, with no breakdown.
const (
	DimNone        = ""
	DimPath        = "path"
	DimPage        = "page"
	DimLabel       = "label"
	DimReferrer    = "referrer_host"
	DimChannel     = "channel"
	DimUTMSource   = "utm_source"
	DimUTMMedium   = "utm_medium"
	DimUTMCampaign = "utm_campaign"
	DimDevice      = "device"
	DimBrowser     = "browser"
	DimOS          = "os"
	DimPlatform    = "platform"
	DimAppVersion  = "app_version"
	DimCountry     = "country"
	DimVariant     = "variant"
)

// Row is one pre-aggregated bucket.
//
// Visitors and Sessions hold serialised HyperLogLog sketches rather than counts, because a
// count cannot be merged: summing per-hour uniques over a day would count a visitor once per
// hour they were active. Sketches union exactly, which is what makes a 30-day unique figure
// the union of 720 hourly sketches and not an overestimate.
type Row struct {
	Hour  int64  `parquet:"hour,timestamp,delta"`
	Site  string `parquet:"site,dict,zstd"`
	Event string `parquet:"event,dict,zstd"`
	Dim   string `parquet:"dim,dict,zstd"`
	Key   string `parquet:"key,dict,zstd"`

	Events int64 `parquet:"events,delta"`

	Visitors []byte `parquet:"visitors,zstd"`
	Sessions []byte `parquet:"sessions,zstd"`
}

// HourOf truncates a millisecond timestamp to its UTC hour.
func HourOf(tsMillis int64) int64 {
	return time.UnixMilli(tsMillis).UTC().Truncate(time.Hour).UnixMilli()
}

type key struct {
	hour        int64
	site, event string
	dim, value  string
}

// Sketch precision is chosen per row type, and it is the single biggest lever on the size of
// this tier.
//
//   - Headline rows — the DimNone total for an event — carry p=14, ~0.8% error. This is the
//     number on the dashboard and the one somebody will compare against another tool.
//   - Breakdown rows — uniques for one path, one referrer, one country — carry p=11, ~2.3%
//     error and an eighth of the dense size. Nobody reads the third significant digit of
//     "unique visitors to /blog/some-article", and there are two orders of magnitude more of
//     these rows than headline ones.
//
// Measured: p=14 everywhere made the rollup tier five times LARGER than the raw data it
// summarises, which would have made the whole tier pointless. See TestRollupSize.
const (
	precHeadline  = 14
	precBreakdown = 11
)

type acc struct {
	events   int64
	visitors *hyperloglog.Sketch

	// sessions is kept only on headline rows. Sessions-per-page is rarely asked for, and a
	// second sketch on every breakdown row doubled this tier for a column almost nobody reads.
	sessions *hyperloglog.Sketch
}

// Build aggregates raw rows into rollup rows.
//
// Each event contributes one row for its own total plus one per non-empty dimension. Empty
// values are skipped rather than rolled up under "": a row counting events whose country we do
// not know is not the same as a country breakdown, and mixing them makes both wrong.
func Build(rows []store.Row) []Row {
	accs := map[key]*acc{}

	add := func(k key, r *store.Row) {
		a := accs[k]
		if a == nil {
			a = &acc{}
			if k.dim == DimNone {
				a.visitors = mustSketch(precHeadline)
				a.sessions = mustSketch(precHeadline)
			} else {
				a.visitors = mustSketch(precBreakdown)
			}
			accs[k] = a
		}
		a.events++
		if r.Visitor != "" {
			a.visitors.Insert([]byte(r.Visitor))
		}
		if a.sessions != nil && r.Session != "" {
			a.sessions.Insert([]byte(r.Session))
		}
	}

	for i := range rows {
		r := &rows[i]
		h := HourOf(r.TSServer)

		add(key{hour: h, site: r.Site, event: r.Name, dim: DimNone}, r)

		for _, d := range []struct{ dim, val string }{
			{DimPath, r.Path},
			{DimPage, r.Page},
			{DimLabel, r.Label},
			{DimReferrer, r.ReferrerHost},
			{DimChannel, r.Channel},
			{DimUTMSource, r.UTMSource},
			{DimUTMMedium, r.UTMMedium},
			{DimUTMCampaign, r.UTMCampaign},
			{DimDevice, r.Device},
			{DimBrowser, r.Browser},
			{DimOS, r.OS},
			{DimPlatform, r.Platform},
			{DimAppVersion, r.AppVersion},
			{DimCountry, r.Country},
			{DimVariant, r.Variant},
		} {
			if d.val == "" {
				continue
			}
			add(key{hour: h, site: r.Site, event: r.Name, dim: d.dim, value: d.val}, r)
		}
	}

	out := make([]Row, 0, len(accs))
	for k, a := range accs {
		vb, err := a.visitors.MarshalBinary()
		if err != nil {
			continue
		}
		var sb []byte
		if a.sessions != nil {
			if sb, err = a.sessions.MarshalBinary(); err != nil {
				continue
			}
		}
		out = append(out, Row{
			Hour: k.hour, Site: k.site, Event: k.event, Dim: k.dim, Key: k.value,
			Events: a.events, Visitors: vb, Sessions: sb,
		})
	}

	// Deterministic order, so a re-run of the same compaction produces a byte-identical file
	// and the idempotency property extends to the rollup tier.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Hour != b.Hour {
			return a.Hour < b.Hour
		}
		if a.Event != b.Event {
			return a.Event < b.Event
		}
		if a.Dim != b.Dim {
			return a.Dim < b.Dim
		}
		return a.Key < b.Key
	})
	return out
}

// Merged is a rollup bucket after partials have been combined.
type Merged struct {
	Key      string
	Events   int64
	Visitors uint64
	Sessions uint64
}

// MergeByKey combines partial rows sharing a key into one result each.
//
// This is where the additive property is actually used: counts sum, sketches union. Callers
// pass rows already filtered to the event, dimension and time range they care about.
func MergeByKey(rows []Row) ([]Merged, error) {
	type agg struct {
		events             int64
		visitors, sessions *hyperloglog.Sketch
	}
	byKey := map[string]*agg{}

	for i := range rows {
		r := &rows[i]
		a := byKey[r.Key]
		if a == nil {
			// Zero-value sketches, which adopt the precision of the first sketch merged
			// into them. That matters because headline and breakdown rows use different
			// precisions, and merging mismatched ones is an error rather than a silent
			// downgrade.
			a = &agg{visitors: &hyperloglog.Sketch{}, sessions: &hyperloglog.Sketch{}}
			byKey[r.Key] = a
		}
		a.events += r.Events

		if err := mergeInto(a.visitors, r.Visitors); err != nil {
			return nil, fmt.Errorf("merge visitors for %q: %w", r.Key, err)
		}
		if err := mergeInto(a.sessions, r.Sessions); err != nil {
			return nil, fmt.Errorf("merge sessions for %q: %w", r.Key, err)
		}
	}

	out := make([]Merged, 0, len(byKey))
	for k, a := range byKey {
		out = append(out, Merged{
			Key: k, Events: a.events,
			Visitors: a.visitors.Estimate(),
			Sessions: a.sessions.Estimate(),
		})
	}

	// Descending by events, then by key so equal counts have a stable order rather than
	// shuffling between identical queries.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Events != out[j].Events {
			return out[i].Events > out[j].Events
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func mustSketch(p uint8) *hyperloglog.Sketch {
	// Sparse, so a bucket with a handful of visitors — which is most of them — costs a
	// handful of bytes rather than a full register array.
	sk, err := hyperloglog.NewSketch(p, true)
	if err != nil {
		panic(fmt.Sprintf("rollup: invalid HLL precision %d: %v", p, err))
	}
	return sk
}

func mergeInto(dst *hyperloglog.Sketch, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var s hyperloglog.Sketch
	if err := s.UnmarshalBinary(raw); err != nil {
		return err
	}
	return dst.Merge(&s)
}
