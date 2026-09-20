// Package modlix adapts the generic engine to the Modlix platform.
//
// Everything here is the embedder's vocabulary — appCodes, clientCodes, the security service,
// the platform's Redis conventions — kept in one package so the rest of the engine stays a
// general-purpose analytics engine that knows only about "sites".
package modlix

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// NoApp is what security answers for a host it does not recognise.
//
// It is a sentinel, not an error: `getClientNAppCodeNType` defaults to
// ("SYSTEM", "nothing", "LIVE") rather than returning empty, so an unknown host is reported
// as a resolved pair whose appCode happens to be this. Anything that forgets to check it
// would file every scanner and stale snippet on the planet under one very busy site.
const NoApp = "nothing"

// Security calls the platform's internal security endpoints.
//
// These live behind nginx, not behind the gateway: the gateway's `(.*internal.*)` rule does
// not protect them. The engine must therefore reach security over a network path that is not
// itself reachable from outside, and BaseURL is that path — a service address, never a public
// hostname.
type Security struct {
	BaseURL string
	HTTP    *http.Client
}

// Site is one resolution of a host.
type Site struct {
	ClientCode string
	AppCode    string
	Surface    string // LIVE or DRAFT; carried because the caller may want to drop drafts
}

// Resolved reports whether this names a real application.
func (s Site) Resolved() bool { return s.AppCode != "" && s.AppCode != NoApp }

func NewSecurity(baseURL string, timeout time.Duration) *Security {
	return &Security{
		BaseURL: baseURL,
		// A client of its own rather than http.DefaultClient: this call sits on the ingest
		// path, and a default client with no timeout turns one slow dependency into a
		// pile of stuck goroutines.
		HTTP: &http.Client{Timeout: timeout},
	}
}

// HostSite resolves a host the way the gateway does, through the same endpoint and with the
// same arguments, so the two cannot disagree about which app a hostname belongs to.
//
// The response is a Reactor Tuple3, which Jackson renders as a JSON array — verified against
// a running security service rather than assumed:
//
//	["THRM", "theorem", "LIVE"]     a host with an app
//	["SYSTEM", "nothing", "LIVE"]   a host without one
func (s *Security) HostSite(ctx context.Context, scheme, host, port string) (Site, error) {
	q := url.Values{}
	q.Set("scheme", scheme)
	q.Set("host", host)
	q.Set("port", port)

	endpoint := s.BaseURL + "/api/security/clients/internal/getClientNAppCodeNType?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Site{}, err
	}

	res, err := s.HTTP.Do(req)
	if err != nil {
		return Site{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return Site{}, fmt.Errorf("security answered %d for host %q", res.StatusCode, host)
	}

	var t []string
	if err := json.NewDecoder(res.Body).Decode(&t); err != nil {
		return Site{}, fmt.Errorf("decoding the security response for host %q: %w", host, err)
	}
	if len(t) < 2 {
		return Site{}, fmt.Errorf("security returned %d fields for host %q, want 3", len(t), host)
	}

	site := Site{ClientCode: t[0], AppCode: t[1], Surface: "LIVE"}
	if len(t) > 2 && t[2] != "" {
		site.Surface = t[2]
	}
	return site, nil
}
