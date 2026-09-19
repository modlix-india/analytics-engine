package ingest

import (
	"net/url"
	"strings"
)

// UTM carries the campaign parameters.
//
// These come from the LANDING URL's query string, never from the referrer — the referrer says
// where a visitor came from, the UTM says which campaign link they followed. Conflating the
// two is the most common mistake in this area and it produces attribution that looks
// plausible and is wrong, so the two live in separate functions that take separate arguments.
type UTM struct {
	Source   string
	Medium   string
	Campaign string
	Term     string
	Content  string

	// Paid click identifiers. Not stored as columns, but their presence alone proves a paid
	// click even when whoever built the ad forgot the utm_medium — which is common.
	hasClickID bool
}

// ParsePageURL splits a page URL into the parts we keep.
//
// Path is normalised: no query, no fragment, no trailing slash except at the root. Without
// that, "/pricing", "/pricing/" and "/pricing?ref=x" are three rows in a top-pages table that
// should have one, and the table becomes useless exactly when a site gets enough traffic to
// need it.
func ParsePageURL(raw string) (host, path string, utm UTM, ok bool) {
	if raw == "" {
		return "", "", UTM{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", "", UTM{}, false
	}

	host = strings.ToLower(u.Hostname())

	path = u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if len(path) > 1 {
		path = strings.TrimRight(path, "/")
		if path == "" {
			path = "/"
		}
	}

	q := u.Query()
	utm = UTM{
		Source:   q.Get("utm_source"),
		Medium:   q.Get("utm_medium"),
		Campaign: q.Get("utm_campaign"),
		Term:     q.Get("utm_term"),
		Content:  q.Get("utm_content"),
	}
	for _, k := range []string{"gclid", "gbraid", "wbraid", "fbclid", "msclkid", "ttclid", "li_fat_id"} {
		if q.Get(k) != "" {
			utm.hasClickID = true
			break
		}
	}

	return host, path, utm, true
}

// ReferrerHost returns the lowercased host of a referrer, or empty.
func ReferrerHost(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// Channel classifies where a visit came from, in the order the rules must be applied.
//
// Order is the substance here. A visitor arriving from a Google search result and one arriving
// from a Google ad both have google.com as their referrer; only the campaign parameters
// separate them, so paid has to be decided before organic. Getting this order wrong silently
// reports paid traffic as free, which is the single most expensive mistake this function can
// make.
func Channel(refHost, pageHost string, utm UTM) string {
	// Self-referral: a link within the same site is not an acquisition channel at all.
	if refHost != "" && sameSite(refHost, pageHost) {
		return "internal"
	}

	switch strings.ToLower(utm.Medium) {
	case "cpc", "ppc", "paid", "paidsearch", "paid_search", "cpm", "cpv", "display", "banner":
		return "paid"
	case "email", "e-mail", "newsletter":
		return "email"
	case "social", "social-network", "social_media", "paid-social":
		return "social"
	case "affiliate":
		return "affiliate"
	case "referral":
		return "referral"
	case "organic":
		return "organic"
	}

	// A click identifier proves a paid click even with no utm_medium.
	if utm.hasClickID {
		return "paid"
	}

	if refHost == "" {
		// No referrer and no campaign: typed, bookmarked, or a referrer the browser
		// stripped. All of it lands in direct, which is why direct is always overstated.
		return "direct"
	}
	if isSearchEngine(refHost) {
		return "organic"
	}
	if isSocial(refHost) {
		return "social"
	}
	return "referral"
}

// sameSite treats www and the bare domain as one site, since a link from www.example.com to
// example.com is not a referral by any useful definition.
func sameSite(a, b string) bool {
	return strings.TrimPrefix(a, "www.") == strings.TrimPrefix(b, "www.")
}

func isSearchEngine(host string) bool {
	h := strings.TrimPrefix(host, "www.")
	for _, s := range []string{
		"google.", "bing.com", "duckduckgo.com", "yahoo.", "yandex.",
		"baidu.com", "ecosia.org", "brave.com", "search.marginalia.nu",
		"startpage.com", "qwant.com", "naver.com", "ask.com",
	} {
		// Prefix match so every google.co.* and yahoo.co.* country domain is covered
		// without listing them all.
		if strings.HasPrefix(h, s) || h == strings.TrimSuffix(s, ".") {
			return true
		}
	}
	return false
}

func isSocial(host string) bool {
	h := strings.TrimPrefix(host, "www.")
	for _, s := range []string{
		"facebook.com", "instagram.com", "twitter.com", "x.com", "t.co",
		"linkedin.com", "lnkd.in", "reddit.com", "pinterest.", "tiktok.com",
		"youtube.com", "youtu.be", "whatsapp.com", "telegram.", "t.me",
		"threads.net", "mastodon.", "bsky.app", "news.ycombinator.com", "quora.com",
	} {
		if strings.HasPrefix(h, s) {
			return true
		}
	}
	return false
}
