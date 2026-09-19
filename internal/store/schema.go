// Package store writes events to Parquet and names the files.
//
// The layout is data/{site}/{utc-date}/{node}-{walseq}[-{part}].parquet, and every element of
// that path is load-bearing:
//
//   - {site} first, because every query filters on it. A query reads one directory subtree
//     and never lists another tenant's data.
//   - {utc-date} is a PRUNING HINT, never a semantic boundary. Rows carry exact timestamps and
//     queries filter on those, so a request for an IST day reads two date directories and
//     returns exactly the right rows. Rolling up by date would be the bug; partitioning by it
//     is not. See PLAN.md section 6.
//   - {node} is a filename element and never a directory, which is what lets several nodes
//     write into one shared bucket with no coordination and no collisions.
//   - {walseq} makes compaction idempotent: the output name is derived from the input segment,
//     so re-running after a crash overwrites rather than duplicates.
package store

import (
	"strings"
	"time"

	"github.com/modlix-india/analytics-engine/internal/event"
)

// Row is the Parquet schema.
//
// Encoding choices per column, which is where most of the file size is decided:
//
//   - Timestamps use delta encoding. Files are sorted by ts_server, so consecutive deltas are
//     small and this costs a few bits per row instead of eight bytes.
//   - Low-cardinality strings use dictionary encoding: a site has a handful of channels, a
//     dozen browsers, a few hundred paths. The column becomes an integer per row plus one
//     copy of each distinct value.
//   - High-cardinality strings — visitor, session, referrer_url, props — are deliberately NOT
//     dictionary encoded. A dictionary of a million distinct visitor ids is larger than the
//     column it replaces, and it would be rebuilt per row group.
//
// zstd throughout: this data is written once and read many times, so decompression speed
// matters more than compression speed, and zstd is better than snappy on both against the
// repetitive text that dominates here.
type Row struct {
	TSServer int64 `parquet:"ts_server,timestamp,delta"`
	TSClient int64 `parquet:"ts_client,timestamp,delta"`

	Site  string `parquet:"site,dict,zstd"`
	Name  string `parquet:"event,dict,zstd"`
	Path  string `parquet:"path,dict,zstd"`
	Page  string `parquet:"page,dict,zstd"`
	Label string `parquet:"label,dict,zstd"`

	Visitor string `parquet:"visitor,zstd"`
	Session string `parquet:"session,zstd"`

	ReferrerHost string `parquet:"referrer_host,dict,zstd"`
	ReferrerURL  string `parquet:"referrer_url,zstd"`
	Channel      string `parquet:"channel,dict,zstd"`

	UTMSource   string `parquet:"utm_source,dict,zstd"`
	UTMMedium   string `parquet:"utm_medium,dict,zstd"`
	UTMCampaign string `parquet:"utm_campaign,dict,zstd"`
	UTMTerm     string `parquet:"utm_term,dict,zstd"`
	UTMContent  string `parquet:"utm_content,dict,zstd"`

	Device     string `parquet:"device,dict,zstd"`
	Browser    string `parquet:"browser,dict,zstd"`
	OS         string `parquet:"os,dict,zstd"`
	Platform   string `parquet:"platform,dict,zstd"`
	AppVersion string `parquet:"app_version,dict,zstd"`
	Country    string `parquet:"country,dict,zstd"`

	Experiment string `parquet:"experiment,dict,zstd"`
	Variant    string `parquet:"variant,dict,zstd"`

	Props string `parquet:"props,json,zstd"`
}

func RowOf(e *event.Event) Row {
	return Row{
		TSServer: e.TSServer, TSClient: e.TSClient,
		Site: e.Site, Name: e.Name, Path: e.Path, Page: e.Page, Label: e.Label,
		Visitor: e.Visitor, Session: e.Session,
		ReferrerHost: e.ReferrerHost, ReferrerURL: e.ReferrerURL, Channel: e.Channel,
		UTMSource: e.UTMSource, UTMMedium: e.UTMMedium, UTMCampaign: e.UTMCampaign,
		UTMTerm: e.UTMTerm, UTMContent: e.UTMContent,
		Device: e.Device, Browser: e.Browser, OS: e.OS,
		Platform: e.Platform, AppVersion: e.AppVersion, Country: e.Country,
		Experiment: e.Experiment, Variant: e.Variant,
		Props: e.Props,
	}
}

// PartitionOf returns the site and UTC date a row belongs to.
//
// UTC because the partition is only a pruning hint; see the package comment. Using a site's
// local date here would bake a timezone into the storage layout and make changing that
// timezone a data migration.
func PartitionOf(r *Row) (site, date string) {
	return r.Site, time.UnixMilli(r.TSServer).UTC().Format("2006-01-02")
}

// SafeSegment makes a string usable as one path element.
//
// The site comes from a resolver, and a resolver is an embedder's code. Treating its output as
// a trusted path component is how "../" ends up escaping the data directory, so this is a
// containment boundary rather than tidiness. Anything outside a conservative set becomes an
// underscore, and the result can never be empty, "." or "..".
func SafeSegment(s string) string {
	if s == "" {
		return "_empty"
	}

	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			// Lowercased so two spellings of one hostname cannot become two directories on
			// a case-sensitive filesystem and one on a case-insensitive one.
			b.WriteRune(r + ('a' - 'A'))
		case r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}

	out := b.String()
	// A path element of dots would traverse. Nothing below this point may be a relative path.
	if strings.Trim(out, ".") == "" {
		return "_dots"
	}
	if len(out) > 128 {
		out = out[:128]
	}
	return out
}
