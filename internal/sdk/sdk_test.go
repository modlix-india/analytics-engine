package sdk

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestServesTheScriptWithAValidatorAndCORS(t *testing.T) {
	h := Handler()

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/a.js", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/javascript") {
		t.Errorf("Content-Type %q, want javascript", ct)
	}
	// Loaded by a page on another origin, always.
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("the script is served cross-origin and carries no ACAO header")
	}
	if !strings.Contains(rec.Body.String(), "sendBeacon") {
		t.Error("the served body does not look like the beacon")
	}

	tag := rec.Header().Get("ETag")
	if tag == "" {
		t.Fatal("no ETag: every page of every site loads this, so revalidation has to be cheap")
	}

	// A second request with the validator must not resend the body.
	req := httptest.NewRequest(http.MethodGet, "/a.js", nil)
	req.Header.Set("If-None-Match", tag)
	rec2 := httptest.NewRecorder()
	h(rec2, req)

	if rec2.Code != http.StatusNotModified {
		t.Fatalf("status %d for a matching If-None-Match, want 304", rec2.Code)
	}
	if rec2.Body.Len() != 0 {
		t.Errorf("304 carried %d bytes of body", rec2.Body.Len())
	}
}

// TestTheBeaconBehaves runs the script's own tests.
//
// The behaviour under test is JavaScript, so its tests are too; this is the bridge that
// keeps `go test ./...` the one command that checks the repo. Skipped rather than failed
// when node is absent: a Go toolchain is the only thing this project has ever required to
// build, and adding a second one as a hard dependency to run the suite would be a tax on
// everyone who touches the Go side.
func TestTheBeaconBehaves(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; analytics_test.mjs not run")
	}

	out, err := exec.Command(node, "--test", "analytics_test.mjs").CombinedOutput()
	if err != nil {
		t.Fatalf("the beacon's own tests failed:\n%s", out)
	}
}

// TestTheMinifiedBeaconBehavesIdentically runs that same suite against the file the engine
// actually serves.
//
// The suite above tests analytics.js, which no browser ever receives. Between it and the
// visitor sits a minifier that renames every identifier and rewrites syntax, and a bug
// introduced there would be invisible in review — the source still reads correctly, and the
// only artefact is a file nobody opens. This is the test that would catch it.
//
// Skipped without node for the same reason as the suite above. TestMinifiedScriptIsCurrent is
// the unconditional half: it needs no node, so a build host still cannot ship a stale beacon.
func TestTheMinifiedBeaconBehavesIdentically(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; the minified beacon was not exercised")
	}

	cmd := exec.Command(node, "--test", "analytics_test.mjs")
	cmd.Env = append(os.Environ(), "MLX_BEACON_FILE=analytics.min.js")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the beacon passes its tests as source but FAILS once minified:\n%s", out)
	}
}
