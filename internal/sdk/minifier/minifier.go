// Package minifier produces the beacon that is actually served from the beacon that is
// actually readable.
//
// It exists as its own package for one reason: `cmd/engine` must never import it. Go links
// only what is reachable, so keeping esbuild out of the engine's import graph keeps it out of
// the engine's binary. The generator and the staleness test import this; the server does not.
//
// Minifying at RUNTIME instead would avoid the generated file, and is the wrong trade — it
// would ship a JavaScript bundler inside a production binary to transform a constant once per
// boot. Minification is a build concern.
package minifier

import (
	"errors"
	"fmt"
	"strings"

	"github.com/evanw/esbuild/pkg/api"
)

// Banner survives minification and is the only comment that does.
//
// Someone who opens a.js in devtools — which is how anyone first meets this file — should not
// have to guess where it came from or try to patch the minified copy.
const Banner = "/* Modlix analytics beacon. Generated from analytics.js; edit that, not this. */\n"

// Minify compiles the readable source down to what the browser receives.
//
// Target is pinned to ES5, and that is load-bearing rather than cautious. The source is ES5 by
// hand — `var`, no arrows, an IIFE — because this script runs on every visitor's browser on
// every customer's site, which is the widest compatibility surface the platform has. Without a
// target, esbuild's syntax minifier happily REWRITES correct ES5 into shorter modern syntax
// (arrow functions, `??`, template literals), and the file would silently stop parsing on an
// old browser while every test here still passed. Pinning it means esbuild errors instead,
// loudly, if anything in the source ever needs lowering it cannot do.
func Minify(src []byte) ([]byte, error) {
	result := api.Transform(string(src), api.TransformOptions{
		Loader:            api.LoaderJS,
		Target:            api.ES5,
		MinifyWhitespace:  true,
		MinifyIdentifiers: true,
		MinifySyntax:      true,
		// The file's comments are its documentation, not licence text, and they are 60% of it.
		// None of them belong in the served copy.
		LegalComments: api.LegalCommentsNone,
	})

	if len(result.Errors) > 0 {
		msgs := make([]string, 0, len(result.Errors))
		for _, e := range result.Errors {
			if e.Location != nil {
				msgs = append(msgs, fmt.Sprintf("line %d: %s", e.Location.Line, e.Text))
				continue
			}
			msgs = append(msgs, e.Text)
		}
		return nil, errors.New("minify: " + strings.Join(msgs, "; "))
	}

	return append([]byte(Banner), result.Code...), nil
}
