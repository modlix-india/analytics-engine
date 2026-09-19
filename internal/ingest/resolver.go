package ingest

import (
	"context"
	"net/url"
	"strings"
)

// Signals are the inputs a site is resolved from.
//
// Every field here is something the page cannot freely choose: the browser sets Origin and
// Referer itself, and the user-agent is set by the client software rather than by page script.
// There is deliberately no "site" field. A page that could name its own site could write into
// another tenant's numbers, and no amount of validation downstream would fix that.
type Signals struct {
	UserAgent string
	PageURL   string // host and path, which is what covers a shared host serving many sites
	Origin    string
	Referer   string
}

// Resolution is what a resolver determined.
//
// Platform and AppVersion are optional. They exist because a host application may recognise
// its own client far more precisely than a generic user-agent parse can — a wrapper that
// stamps its name and version into the UA knows exactly what it is, where this engine can only
// tell that it is some embedded browser. Left empty, the generic parse stands.
type Resolution struct {
	Site       string
	Platform   string
	AppVersion string
}

// SiteResolver maps signals to a site.
//
// This is the seam that keeps the engine generic. The default implementation treats the host
// as the site, which is right for a single-tenant deployment and for anyone using this
// standalone. An embedder supplies its own to map hosts onto whatever it calls a tenant,
// consulting its own services and caches, without that vocabulary appearing in this repo.
type SiteResolver interface {
	// Resolve returns the site, or false to discard the event.
	//
	// Discarding is the correct answer for an unknown host, and it must be cheap: an
	// unrecognised host is far more likely to be a stale snippet or a scanner than a bug,
	// and both will arrive at a steady rate forever.
	Resolve(ctx context.Context, s Signals) (Resolution, bool)
}

// HostResolver treats the hostname as the site.
//
// Preference order is PageURL, then Origin, then Referer, matching how much each can be
// trusted to name the page actually being measured. Referer is last because a referrer policy
// can strip it, making it the only one of the three the page itself can suppress.
type HostResolver struct {
	// Allow, when non-empty, restricts ingest to these hosts. A public endpoint with no
	// allow-list will happily accumulate events for any hostname anyone points at it, and
	// the first sign of that is usually a storage bill.
	Allow map[string]bool
}

func (r HostResolver) Resolve(_ context.Context, s Signals) (Resolution, bool) {
	host := firstHost(s.PageURL, s.Origin, s.Referer)
	if host == "" {
		return Resolution{}, false
	}
	// www and the bare domain are one site; counting them separately splits every number a
	// customer looks at.
	host = strings.TrimPrefix(host, "www.")

	if len(r.Allow) > 0 && !r.Allow[host] {
		return Resolution{}, false
	}
	return Resolution{Site: host}, true
}

func firstHost(candidates ...string) string {
	for _, c := range candidates {
		if c == "" {
			continue
		}
		u, err := url.Parse(c)
		if err != nil {
			continue
		}
		if h := strings.ToLower(u.Hostname()); h != "" {
			return h
		}
	}
	return ""
}
