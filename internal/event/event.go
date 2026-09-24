// Package event defines the record that flows from ingest, through the WAL, into Parquet.
//
// Two representations, deliberately kept apart:
//
//   - Event is the decoded, enriched record. Ingest produces it once; everything downstream
//     consumes it.
//   - The packed encoding below is what sits in the WAL. It exists so the compactor does not
//     re-parse JSON: a segment is read exactly once, and paying for JSON decoding twice on
//     every event is the kind of cost that only shows up under load.
//
// The wire format the browser sends is NOT here — it lives in package ingest, because it is a
// public contract with its own compatibility rules, while this one is internal to a single
// binary and may change whenever the version byte is bumped.
package event

import (
	"encoding/binary"
	"fmt"
)

// Event is one analytics event, after enrichment.
//
// Field order matters: it is the encoding order, and appending rather than inserting is what
// keeps an old segment readable by a newer binary.
type Event struct {
	// TSServer is when we received it, in unix milliseconds UTC. Every aggregation uses this.
	TSServer int64
	// TSClient is what the client claimed. Kept for diagnosis and never trusted: a device
	// with a wrong clock would otherwise silently move events between days.
	TSClient int64

	Site  string
	Name  string // "$pageview", or a custom event name
	Path  string
	Page  string // the host application's page identity
	Label string // the click target's stable label

	Visitor string
	Session string

	ReferrerHost string
	ReferrerURL  string
	Channel      string

	UTMSource   string
	UTMMedium   string
	UTMCampaign string
	UTMTerm     string
	UTMContent  string

	Device     string
	Browser    string
	OS         string
	Platform   string // "web" or "mobile_app"
	AppVersion string
	Country    string

	Experiment string
	Variant    string

	// Where a click landed, and how wide the window was when it did. Zero on every event
	// that is not a click — which is most of them, and costs nothing: a column of zeros
	// compresses to almost nothing.
	//
	// ClickX is a PROPORTION of the viewport width, in ten-thousandths, not a pixel. A pixel
	// abscissa means different things on a phone and a desktop, so averaging the two produces
	// a picture of nowhere. ClickY is absolute pixels from the top of the document, because
	// vertical position does not scale with width — a header is a header at any size.
	ClickX   int32
	ClickY   int32
	Viewport int32 // css pixels of viewport width, for banding

	// How far down the page a view got, and the two heights that give it meaning. Zero on
	// everything that is not a $scroll report.
	//
	// ScrollPct is 0..100 of the document's FINAL height, measured to the bottom of the
	// window rather than its top: the question is what somebody could have seen, and a page
	// that fits on one screen was seen in full without anyone scrolling it.
	//
	// ViewportH and DocH travel with it because the percentage alone cannot answer the
	// question underneath it — where the fold is. Median viewport height over median document
	// height is the share of a page visible without scrolling, and that is the number that
	// says whether a call to action is below it.
	ScrollPct int32
	ViewportH int32
	DocH      int32

	// Interactive is 1 when a click landed on something that does anything when clicked, and
	// 0 when it did not. Only a $click carries it, and a 0 on a row written before the column
	// existed is indistinguishable from a real dead click — which is why the reader requires
	// the column to be present at all, rather than trusting a zero.
	Interactive int32

	// Props is the JSON tail: anything the caller sent that has no column. Stored as received
	// so an unmodelled field is never silently dropped, and nothing in the fixed widget set
	// filters on it.
	Props string
}

// encodingVersion prefixes every packed record.
//
// It is what makes a rolling deploy safe: a new binary can still read segments written by the
// previous one, which matters because a restart leaves up to one compaction interval of
// events on disk. Adding a field means appending it and bumping this.
// NamePageview is the canonical name of a page view.
//
// It has a definition here rather than a string literal at each end because both ends must
// agree exactly: ingest stores what the client sent, and every traffic widget filters on this
// name. Disagree by one character and the dashboard reports zero for data that is present,
// correct and queryable — no error anywhere, because nothing is wrong except the spelling.
const NamePageview = "$pageview"

const encodingVersion byte = 4

// Encode appends the packed form of e to dst.
//
// Appends rather than allocating so a caller encoding a batch reuses one buffer. The format is
// the version byte, two varint-zigzag timestamps, then every string as a varint length
// followed by its bytes, in struct order.
func Encode(dst []byte, e *Event) []byte {
	dst = append(dst, encodingVersion)
	dst = binary.AppendVarint(dst, e.TSServer)
	dst = binary.AppendVarint(dst, e.TSClient)

	for _, s := range e.strings() {
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		dst = append(dst, s...)
	}

	// Numbers last, after every string, so that a v1 record — which has none of them — is
	// exactly a v2 record that ended early.
	for _, n := range e.numbers() {
		dst = binary.AppendVarint(dst, int64(n))
	}
	return dst
}

// Decode reads a packed record.
//
// It tolerates a record that ends early by leaving the remaining fields empty, which is what
// makes appending a field backward compatible: yesterday's segment simply has no value for
// today's column. It does not tolerate a wrong version byte or a length that overruns the
// buffer, because those mean the bytes are not what they claim to be.
func Decode(b []byte) (*Event, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("event: empty record")
	}
	if b[0] > encodingVersion {
		return nil, fmt.Errorf("event: encoding version %d is newer than this binary understands", b[0])
	}
	b = b[1:]

	var e Event
	var n int

	e.TSServer, n = binary.Varint(b)
	if n <= 0 {
		return nil, fmt.Errorf("event: bad server timestamp")
	}
	b = b[n:]

	e.TSClient, n = binary.Varint(b)
	if n <= 0 {
		return nil, fmt.Errorf("event: bad client timestamp")
	}
	b = b[n:]

	targets := e.stringPtrs()
	for _, p := range targets {
		if len(b) == 0 {
			// Written by an older encoder that did not have this field yet.
			break
		}
		length, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("event: bad string length")
		}
		b = b[n:]
		if uint64(len(b)) < length {
			return nil, fmt.Errorf("event: string of %d bytes overruns the record", length)
		}
		*p = string(b[:length])
		b = b[length:]
	}

	for _, p := range e.numberPtrs() {
		if len(b) == 0 {
			// A v1 record, or a v2 one written before this field existed.
			break
		}
		v, n := binary.Varint(b)
		if n <= 0 {
			return nil, fmt.Errorf("event: bad number")
		}
		*p = int32(v)
		b = b[n:]
	}

	return &e, nil
}

// strings and stringPtrs must stay in the same order. They are next to each other so that a
// field added to one and not the other is visible in a single screen rather than being a
// silent off-by-one that shifts every column after it.
func (e *Event) strings() []string {
	return []string{
		e.Site, e.Name, e.Path, e.Page, e.Label,
		e.Visitor, e.Session,
		e.ReferrerHost, e.ReferrerURL, e.Channel,
		e.UTMSource, e.UTMMedium, e.UTMCampaign, e.UTMTerm, e.UTMContent,
		e.Device, e.Browser, e.OS, e.Platform, e.AppVersion, e.Country,
		e.Experiment, e.Variant,
		e.Props,
	}
}

// numbers and numberPtrs carry the same rule as strings and stringPtrs: same order, next to
// each other, so a field added to one and not the other is visible rather than being a silent
// shift of everything after it.
func (e *Event) numbers() []int32 {
	return []int32{e.ClickX, e.ClickY, e.Viewport, e.ScrollPct, e.ViewportH, e.DocH, e.Interactive}
}

func (e *Event) numberPtrs() []*int32 {
	return []*int32{&e.ClickX, &e.ClickY, &e.Viewport, &e.ScrollPct, &e.ViewportH, &e.DocH, &e.Interactive}
}

func (e *Event) stringPtrs() []*string {
	return []*string{
		&e.Site, &e.Name, &e.Path, &e.Page, &e.Label,
		&e.Visitor, &e.Session,
		&e.ReferrerHost, &e.ReferrerURL, &e.Channel,
		&e.UTMSource, &e.UTMMedium, &e.UTMCampaign, &e.UTMTerm, &e.UTMContent,
		&e.Device, &e.Browser, &e.OS, &e.Platform, &e.AppVersion, &e.Country,
		&e.Experiment, &e.Variant,
		&e.Props,
	}
}
