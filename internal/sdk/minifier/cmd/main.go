// Command minify regenerates the served beacon from the readable one.
//
// Invoked by the `go:generate` directive in sdk.go, so the usual way to run it is:
//
//	go generate ./...
//
// The output is committed. The engine embeds it, so a build needs nothing but a Go toolchain —
// and a stale copy cannot ship unnoticed, because TestMinifiedScriptIsCurrent regenerates and
// compares on every `go test`.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/modlix-india/analytics-engine/internal/sdk/minifier"
)

func main() {
	in := flag.String("in", "analytics.js", "the readable source")
	out := flag.String("out", "analytics.min.js", "the file the engine embeds")
	flag.Parse()

	src, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	min, err := minifier.Minify(src)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := os.WriteFile(*out, min, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("%s: %d bytes -> %s: %d bytes (%d%% smaller)\n",
		*in, len(src), *out, len(min), 100-100*len(min)/len(src))
}
