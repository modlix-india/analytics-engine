package modlix

import (
	"context"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/modlix-india/analytics-engine/internal/ingest"
)

// Resolver maps a request onto a Modlix site: `appcode_clientcode`.
//
// Three ways in, in order of how much they can be trusted and how much they cost:
//
//  1. The mobile app states its own site in the user-agent. No lookup at all.
//  2. A path-prefixed URL names one: /appCode/clientCode/page/... — the shared-host form,
//     which is the DEFAULT for any app without a custom domain, not an edge case.
//  3. The hostname, resolved through security and cached.
//
// Anything else is discarded, and so is a host security does not recognise. That is the
// common case on a public endpoint — a stale snippet or a scanner — not an error worth
// reporting to the caller.
type Resolver struct {
	Security *Security
	Shared   Shared
	Log      *slog.Logger

	// LocalTTL is how long a resolution is kept in this process. Short, because it is the
	// layer no eviction message reaches if Redis is absent.
	LocalTTL time.Duration

	// NegativeTTL is how long an unrecognised host is remembered as unrecognised. Its own
	// setting because the two are different risks: caching a real site too long delays a
	// customer's first numbers, caching an unknown host too briefly lets a scanner drive
	// one security call per request.
	NegativeTTL time.Duration

	// Budget is the longest Resolve will wait for security before giving up and dropping
	// the event. The ingest path must never block on a dependency: a slow security service
	// has to cost events, never the WAL.
	Budget time.Duration

	// OnResolve reports the outcome of every resolution for metrics. An adapter that
	// silently drops is indistinguishable from one that is working.
	OnResolve func(outcome string)

	mu     sync.Mutex
	local  map[string]localEntry
	flight map[string]*call
}

type localEntry struct {
	site    string // empty means "known not to resolve"
	expires time.Time
}

// call is one in-flight security lookup, shared by everyone who asks for the same host while
// it runs. Without this, a burst on a cold cache sends one request per event to security —
// exactly when it is least able to answer them.
type call struct {
	done chan struct{}
	site string
}

func (r *Resolver) init() {
	if r.local == nil {
		r.local = map[string]localEntry{}
		r.flight = map[string]*call{}
	}
	if r.LocalTTL == 0 {
		r.LocalTTL = 5 * time.Minute
	}
	if r.NegativeTTL == 0 {
		r.NegativeTTL = time.Minute
	}
	if r.Budget == 0 {
		r.Budget = 250 * time.Millisecond
	}
}

// Start begins listening for the platform's cache evictions. Optional: without it the
// resolver is correct but only as fresh as its TTLs.
func (r *Resolver) Start(ctx context.Context) {
	r.mu.Lock()
	r.init()
	r.mu.Unlock()

	if r.Shared == nil {
		return
	}
	r.Shared.Watch(ctx, func(key string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if key == "" {
			r.local = map[string]localEntry{}
			return
		}
		delete(r.local, key)
	})
}

func (r *Resolver) count(outcome string) {
	if r.OnResolve != nil {
		r.OnResolve(outcome)
	}
}

// Resolve implements ingest.SiteResolver.
func (r *Resolver) Resolve(ctx context.Context, s ingest.Signals) (ingest.Resolution, bool) {
	r.mu.Lock()
	r.init()
	r.mu.Unlock()

	// 1. The app says who it is. Free, and more precise than anything we could infer: the
	// wrapper knows its own client, app and version, where a user-agent parse can only tell
	// that this is some embedded browser.
	if tag, ok := parseModlixApp(s.UserAgent); ok {
		r.count("ua_tag")
		return ingest.Resolution{
			Site:       SiteKey(tag.ClientCode, tag.AppCode),
			Platform:   "mobile_app",
			AppVersion: tag.Version,
		}, true
	}

	// 2. A path-prefixed page URL, which is how every app without a custom domain is
	// served. Trusted only as far as the browser-set Origin agrees with it: the page URL
	// travels in the payload, so a script could otherwise name any app it liked and write
	// into another tenant's numbers. Where Origin is absent the host still has to resolve
	// on its own below.
	if app, client, ok := codesFromPath(s.PageURL); ok && originAgrees(s.PageURL, s.Origin) {
		r.count("path")
		return ingest.Resolution{Site: SiteKey(client, app)}, true
	}

	host := hostFor(s)
	if host == "" {
		r.count("no_host")
		return ingest.Resolution{}, false
	}

	site, ok := r.siteForHost(ctx, host)
	if !ok {
		return ingest.Resolution{}, false
	}
	return ingest.Resolution{Site: site}, true
}

// siteForHost is the cached lookup: this process, then Redis, then security.
func (r *Resolver) siteForHost(ctx context.Context, host string) (string, bool) {
	key := cacheKey(host)

	if site, found := r.localGet(key); found {
		if site == "" {
			r.count("unknown_host_cached")
			return "", false
		}
		r.count("local_hit")
		return site, true
	}

	if r.Shared != nil {
		if v, found := r.Shared.Get(ctx, key); found {
			r.localPut(key, v)
			if v == "" {
				r.count("unknown_host_cached")
				return "", false
			}
			r.count("shared_hit")
			return v, true
		}
	}

	site, ok := r.lookup(ctx, host, key)
	if !ok {
		return "", false
	}
	if site == "" {
		r.count("unknown_host")
		return "", false
	}
	r.count("lookup")
	return site, true
}

// lookup asks security, at most once per host at a time, and never for longer than Budget.
//
// The second return is false only when the answer did not arrive in time — the event is
// dropped and nothing is cached, because a timeout says nothing about whether the host is
// real. A dropped pageview is a rounding error; a stalled ingest path is an outage.
func (r *Resolver) lookup(ctx context.Context, host, key string) (string, bool) {
	r.mu.Lock()
	c, running := r.flight[key]
	if !running {
		c = &call{done: make(chan struct{})}
		r.flight[key] = c
	}
	r.mu.Unlock()

	if !running {
		go r.fetch(host, key, c)
	}

	timer := time.NewTimer(r.Budget)
	defer timer.Stop()

	select {
	case <-c.done:
		return c.site, true
	case <-timer.C:
		r.count("timeout")
		return "", false
	case <-ctx.Done():
		r.count("cancelled")
		return "", false
	}
}

// fetch runs the actual call, detached from the request that triggered it.
//
// Detached on purpose: whoever asked may give up at Budget, and the answer is still worth
// having for the next event a moment later. Cancelling it with the request would mean a
// busy site that keeps timing out never populates its own cache.
func (r *Resolver) fetch(host, key string, c *call) {
	defer func() {
		close(c.done)
		r.mu.Lock()
		delete(r.flight, key)
		r.mu.Unlock()
	}()

	// Its own deadline, generous relative to Budget: this is no longer on anyone's
	// critical path, and the point is to fill the cache.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	site, err := r.Security.HostSite(ctx, "https", host, "")
	if err != nil {
		if r.Log != nil {
			r.Log.Warn("resolve: security lookup failed", "host", host, "err", err)
		}
		r.count("lookup_error")
		// Deliberately not cached, not even negatively. Security being unreachable is
		// not evidence about the host, and remembering it as unknown would keep a real
		// customer's events in the bin after the outage ended.
		return
	}

	if !site.Resolved() {
		r.localPutFor(key, "", r.NegativeTTL)
		if r.Shared != nil {
			r.Shared.Set(ctx, key, "")
		}
		return
	}

	c.site = SiteKey(site.ClientCode, site.AppCode)
	r.localPut(key, c.site)
	if r.Shared != nil {
		r.Shared.Set(ctx, key, c.site)
	}
}

func (r *Resolver) localGet(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.local[key]
	if !ok || time.Now().After(e.expires) {
		return "", false
	}
	return e.site, true
}

func (r *Resolver) localPut(key, site string) {
	ttl := r.LocalTTL
	if site == "" {
		ttl = r.NegativeTTL
	}
	r.localPutFor(key, site, ttl)
}

func (r *Resolver) localPutFor(key, site string, ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.local[key] = localEntry{site: site, expires: time.Now().Add(ttl)}
}

// cacheKey mirrors the gateway's own key for the same question, `scheme:host:port`, so the
// two caches can be read side by side when a resolution looks wrong.
func cacheKey(host string) string { return "https:" + host + ":" }

// SiteKey is the storage identity of a Modlix site.
//
// Canonicalised, because two spellings of one site would split every number a customer
// looks at, and the platform is genuinely inconsistent here: clientCodes are conventionally
// upper case and appCodes lower, the gateway compares them case-insensitively, and a URL can
// carry either. One rule, applied at the only place a site key is ever made.
func SiteKey(clientCode, appCode string) string {
	return strings.ToLower(appCode) + "_" + strings.ToUpper(clientCode)
}

type modlixApp struct {
	Version    string
	ClientCode string
	AppCode    string
}

// parseModlixApp reads the tag the Flutter wrapper appends to its user-agent:
//
//	ModlixApp/1.4.2 SYSTEM/monkbars
//
// Older generated apps predate the tag, so absence is ordinary and must fall through to the
// URL rather than discard the event.
func parseModlixApp(ua string) (modlixApp, bool) {
	i := strings.Index(ua, "ModlixApp/")
	if i < 0 {
		return modlixApp{}, false
	}

	fields := strings.Fields(ua[i:])
	if len(fields) < 2 {
		return modlixApp{}, false
	}

	version := strings.TrimPrefix(fields[0], "ModlixApp/")
	client, app, found := strings.Cut(fields[1], "/")
	if !found || client == "" || app == "" {
		return modlixApp{}, false
	}
	return modlixApp{Version: version, ClientCode: client, AppCode: app}, true
}

// codesFromPath extracts an app and client from a path-prefixed URL.
//
// Mirrors GatewayFilter: the prefix is whatever precedes /page/ or /manifest/, and it reads
// /appCode/clientCode — app first, which is the opposite of the order the tuple returns them
// in and a very easy thing to get backwards. Only those two markers count; a bare
// /a/b/anything is some other application's routing, not ours to interpret.
func codesFromPath(pageURL string) (appCode, clientCode string, ok bool) {
	u, err := url.Parse(pageURL)
	if err != nil {
		return "", "", false
	}

	path := u.Path
	idx := strings.Index(path, "/page/")
	if idx < 0 {
		idx = strings.Index(path, "/manifest/")
	}
	if idx <= 0 {
		return "", "", false
	}

	parts := strings.Split(path[:idx], "/")
	// ["", appCode, clientCode]
	if len(parts) < 3 || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// originAgrees reports whether the page URL and the browser-set Origin name the same host.
//
// The page URL is payload and a script may write anything in it; Origin is set by the
// browser and cannot be forged from page script. Requiring agreement costs nothing for real
// traffic and removes the only way one site could file events under another's name without
// leaving the browser. With no Origin at all there is nothing to check, and resolution falls
// through to the hostname, which is the conservative path anyway.
func originAgrees(pageURL, origin string) bool {
	if origin == "" {
		return false
	}
	p, err1 := url.Parse(pageURL)
	o, err2 := url.Parse(origin)
	if err1 != nil || err2 != nil {
		return false
	}
	return strings.EqualFold(p.Hostname(), o.Hostname()) && p.Hostname() != ""
}

// hostFor picks the host to resolve from.
//
// The page URL first, because it is the only signal that names the page actually being
// measured — but only while the browser-set Origin agrees with it. Where they disagree the
// page URL is the forgeable one, so Origin wins: a script claiming to be somewhere else gets
// counted where it really is, which is both the safe answer and the honest one. Referer is
// last because a referrer policy can strip it, making it the only signal a page can suppress.
func hostFor(s ingest.Signals) string {
	page, origin := hostOf(s.PageURL), hostOf(s.Origin)

	if page != "" && origin != "" && !strings.EqualFold(page, origin) {
		return origin
	}
	if page != "" {
		return page
	}
	if origin != "" {
		return origin
	}
	return hostOf(s.Referer)
}

func hostOf(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}
