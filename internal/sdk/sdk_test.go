package sdk

import (
	"net/http"
	"net/http/httptest"
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
