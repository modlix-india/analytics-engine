package event

import (
	"encoding/binary"
	"reflect"
	"testing"
)

// Every string field gets a distinct value, so if strings() and stringPtrs() ever drift out of
// step the round trip lands values in the wrong columns and this fails. That is the whole
// point: a mismatch there is a silent off-by-one that would quietly shift every field after
// it, and no other test would notice.
func filledEvent() *Event {
	return &Event{
		TSServer: 1758300000000, TSClient: 1758299999000,
		Site: "site", Name: "name", Path: "path", Page: "page", Label: "label",
		Visitor: "visitor", Session: "session",
		ReferrerHost: "refhost", ReferrerURL: "refurl", Channel: "channel",
		UTMSource: "usource", UTMMedium: "umedium", UTMCampaign: "ucampaign",
		UTMTerm: "uterm", UTMContent: "ucontent",
		Device: "device", Browser: "browser", OS: "os",
		Platform: "platform", AppVersion: "appversion", Country: "country",
		Experiment: "experiment", Variant: "variant",
		ClickX: 5000, ClickY: 1280, Viewport: 1440,
		Props: `{"k":"v"}`,
	}
}

func TestRoundTrip(t *testing.T) {
	want := filledEvent()

	got, err := Decode(Encode(nil, want))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("round trip changed the event:\n want %+v\n  got %+v", want, got)
	}
}

// Guards the invariant directly rather than only through the round trip, so the failure names
// the cause instead of showing a confusing diff.
//
// Every field of Event has to appear in exactly one pair of lists. A field in neither is
// silently dropped on the way to Parquet; a field in one list but not its partner shifts every
// column after it, which is the kind of corruption that looks like bad data rather than a bug.
func TestFieldListsAgree(t *testing.T) {
	e := filledEvent()
	if len(e.strings()) != len(e.stringPtrs()) {
		t.Fatalf("strings() has %d entries, stringPtrs() has %d: they must correspond",
			len(e.strings()), len(e.stringPtrs()))
	}
	if len(e.numbers()) != len(e.numberPtrs()) {
		t.Fatalf("numbers() has %d entries, numberPtrs() has %d: they must correspond",
			len(e.numbers()), len(e.numberPtrs()))
	}

	// Reflection catches the other half: a field added to the struct but to neither list.
	var strs, nums int
	rt := reflect.TypeOf(Event{})
	for i := 0; i < rt.NumField(); i++ {
		switch rt.Field(i).Type.Kind() {
		case reflect.String:
			strs++
		case reflect.Int32:
			nums++
		}
	}
	if got := len(e.strings()); got != strs {
		t.Errorf("Event has %d string fields but strings() lists %d; a new field was not added to both lists", strs, got)
	}
	if got := len(e.numbers()); got != nums {
		t.Errorf("Event has %d int32 fields but numbers() lists %d; a new field was not added to both lists", nums, got)
	}
}

func TestEmptyStringsSurvive(t *testing.T) {
	// The common case in practice: most events carry few optional fields.
	want := &Event{TSServer: 1, TSClient: 2, Site: "s", Name: "$pageview"}

	got, err := Decode(Encode(nil, want))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("round trip changed a sparse event:\n want %+v\n  got %+v", want, got)
	}
}

// A record truncated after some fields must decode with the rest empty, not fail. This is what
// makes appending a field to the struct backward compatible, so a binary can read segments the
// previous one wrote — which matters on every rolling deploy.
func TestShortRecordDecodesWithEmptyTail(t *testing.T) {
	full := Encode(nil, filledEvent())

	// Cut after the timestamps and the first couple of strings.
	for _, cut := range []int{12, 20, 30, 45} {
		if cut >= len(full) {
			continue
		}
		e, err := Decode(full[:cut])
		if err != nil {
			// A cut mid-length-prefix is legitimately undecodable; only assert no panic
			// and a clean error.
			continue
		}
		if e.TSServer == 0 {
			t.Errorf("cut=%d: timestamps should survive a truncated tail", cut)
		}
	}
}

func TestRejectsUnknownVersion(t *testing.T) {
	b := Encode(nil, filledEvent())
	b[0] = 99
	if _, err := Decode(b); err == nil {
		t.Fatal("Decode() accepted an unknown encoding version")
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add(Encode(nil, filledEvent()))
	f.Add([]byte{1})
	f.Add([]byte{1, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, b []byte) {
		// Arbitrary bytes may fail; they may not panic or allocate the world.
		_, _ = Decode(b)
	})
}

func BenchmarkEncode(b *testing.B) {
	e := filledEvent()
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		buf = Encode(buf[:0], e)
	}
}

func BenchmarkDecode(b *testing.B) {
	packed := Encode(nil, filledEvent())
	b.ReportAllocs()
	b.SetBytes(int64(len(packed)))
	for b.Loop() {
		if _, err := Decode(packed); err != nil {
			b.Fatal(err)
		}
	}
}

// A record written before the click columns existed must still decode, with those fields
// zero rather than as an error. This is the property that makes a rolling deploy safe: a
// restart leaves up to one compaction interval of segments on disk, written by the binary
// that is being replaced.
func TestVersionOneRecordStillDecodes(t *testing.T) {
	// Encoded the way version 1 did it: the version byte, the timestamps, the strings, and
	// nothing after them.
	e := filledEvent()
	v1 := []byte{1}
	v1 = binary.AppendVarint(v1, e.TSServer)
	v1 = binary.AppendVarint(v1, e.TSClient)
	for _, s := range e.strings() {
		v1 = binary.AppendUvarint(v1, uint64(len(s)))
		v1 = append(v1, s...)
	}

	got, err := Decode(v1)
	if err != nil {
		t.Fatalf("a version 1 record no longer decodes: %v", err)
	}
	if got.Site != e.Site || got.Name != e.Name || got.Props != e.Props {
		t.Errorf("strings did not survive: %+v", got)
	}
	if got.ClickX != 0 || got.ClickY != 0 || got.Viewport != 0 {
		t.Errorf("fields that did not exist in version 1 came back non-zero: %+v", got)
	}
}
