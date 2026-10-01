package sdk

import (
	"bytes"
	"os"
	"testing"

	"github.com/modlix-india/analytics-engine/internal/sdk/minifier"
)

// A committed build artifact can go stale, and this one would go stale QUIETLY: the engine
// would keep serving a beacon that no longer matches the source anybody reads or reviews, with
// nothing failing and nothing to notice. So the check is unconditional.
//
// It is also why the minifier is a Go dependency rather than an npx call. Shelling out to node
// would mean skipping this test wherever node is absent — which includes the Docker build —
// and the one environment where staleness matters most is the one that builds the image.
func TestMinifiedScriptIsCurrent(t *testing.T) {
	src, err := os.ReadFile("analytics.js")
	if err != nil {
		t.Fatal(err)
	}

	want, err := minifier.Minify(src)
	if err != nil {
		t.Fatalf("the source no longer minifies: %v", err)
	}

	got, err := os.ReadFile("analytics.min.js")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("analytics.min.js is stale: it is %d bytes, regenerating gives %d.\n"+
			"Run: go generate ./internal/sdk/", len(got), len(want))
	}
}

// The embedded copy is what the handler serves, and `go:embed` reads from disk at COMPILE time.
// Checking the file on disk therefore proves nothing about a binary built before the last
// regeneration, which is exactly the window this guards.
func TestEmbeddedScriptIsTheMinifiedFile(t *testing.T) {
	onDisk, err := os.ReadFile("analytics.min.js")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(script, onDisk) {
		t.Fatal("the embedded script differs from analytics.min.js on disk")
	}
	if bytes.Contains(script, []byte("The Modlix analytics beacon.")) {
		t.Error("the served script still carries the source's comment block; it is not minified")
	}
}
