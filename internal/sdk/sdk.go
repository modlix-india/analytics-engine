// Package sdk serves the browser beacon.
//
// The engine serves its own client. A page-generating service then needs one script tag and
// no knowledge of the wire format, and the two cannot drift apart across a deployment: the
// script and the endpoint that receives its events ship as one binary.
package sdk

import (
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"net/http"
	"strconv"
	"time"
)

// The SERVED copy is the minified one; analytics.js is the readable one and the thing to edit.
// Regenerate with `go generate ./...` after changing it — or let the test tell you, which it
// will.
//
// The split is worth its one generated file. Roughly 60% of analytics.js is comment, and those
// comments are the only documentation this script has, so stripping them at the source is not
// an option. Minified and brotli-compressed, which is what the edge actually serves, the beacon
// goes from 7.8 KB to 2.1 KB on every cold load of every page of every customer site.
//
//go:generate go run ./minifier/cmd -in analytics.js -out analytics.min.js
//go:embed analytics.min.js
var script []byte

// etag is content-derived, so a rebuild that does not change the script does not invalidate
// anyone's cache, and one that does invalidates everyone's immediately.
var etag = func() string {
	sum := sha256.Sum256(script)
	return `"` + base64.RawURLEncoding.EncodeToString(sum[:12]) + `"`
}()

// Handler serves the beacon at /a.js.
//
// Cached for an hour rather than a year: this script is loaded by every page of every site,
// so a bad one is the kind of mistake that has to be fixable within a lunch break. The ETag
// makes the revalidation cheap.
func Handler() http.HandlerFunc {
	modified := time.Now().UTC().Format(http.TimeFormat)

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", modified)
		// Served cross-origin by definition — the analytics host is not the site's host.
		w.Header().Set("Access-Control-Allow-Origin", "*")

		if match := r.Header.Get("If-None-Match"); match == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(script)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(script)
		}
	}
}
