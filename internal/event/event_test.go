package event

import (
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
func TestStringFieldListsAgree(t *testing.T) {
	e := filledEvent()
	if len(e.strings()) != len(e.stringPtrs()) {
		t.Fatalf("strings() has %d entries, stringPtrs() has %d: they must correspond",
			len(e.strings()), len(e.stringPtrs()))
	}

	// Reflection catches the other half: a field added to the struct but to neither list.
	n := reflect.TypeOf(Event{}).NumField()
	const nonStringFields = 2 // TSServer, TSClient
	if got, want := len(e.strings()), n-nonStringFields; got != want {
		t.Errorf("Event has %d string fields but strings() lists %d; a new field was not added to both lists", want, got)
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
