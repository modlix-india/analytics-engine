package ingest

import "strings"

// User-agent parsing, hand-written rather than pulled from a library.
//
// Two reasons. The permissively-licensed Go UA parsers are either large regex tables that need
// periodic data updates, or carry licences worth checking one by one — and this engine exists
// partly to stop having that conversation. And the output here is a dictionary-encoded column
// with perhaps a dozen useful values, not a forensic device fingerprint: "Chrome on Android,
// mobile" is the entire requirement.
//
// The ordering below is the whole trick. Every browser lies about being every other browser,
// so the checks run from most specific to least: Edge claims Chrome and Safari, Chrome claims
// Safari, and everything claims Mozilla.

// UAInfo is what we keep from a user-agent string.
type UAInfo struct {
	Browser  string
	OS       string
	Device   string // desktop | mobile | tablet
	Platform string // web | mobile_app
	Bot      bool
}

// ParseUA classifies a user-agent.
//
// Unknown values stay empty rather than becoming "Other": an empty dictionary entry costs
// nothing and is honest, whereas "Other" invites somebody to read it as a real browser with a
// real share.
func ParseUA(ua string) UAInfo {
	if ua == "" {
		return UAInfo{}
	}
	l := strings.ToLower(ua)

	info := UAInfo{
		Browser:  browserOf(ua, l),
		OS:       osOf(l),
		Device:   deviceOf(l),
		Platform: platformOf(l),
		Bot:      looksLikeBot(l),
	}
	return info
}

func browserOf(ua, l string) string {
	switch {
	// Edge before Chrome and Safari: it claims both.
	case strings.Contains(l, "edg/"), strings.Contains(l, "edga/"), strings.Contains(l, "edgios/"):
		return "Edge"
	case strings.Contains(l, "opr/"), strings.Contains(l, "opera"):
		return "Opera"
	case strings.Contains(l, "samsungbrowser"):
		return "Samsung Internet"
	// Chrome and Firefox on iOS are Safari underneath but report their own tokens, and a
	// user who chose Chrome should be counted as Chrome.
	case strings.Contains(l, "crios/"):
		return "Chrome"
	case strings.Contains(l, "fxios/"):
		return "Firefox"
	case strings.Contains(l, "firefox/"):
		return "Firefox"
	// Chrome before Safari: Chrome's UA contains "Safari" for historical compatibility.
	case strings.Contains(l, "chrome/"), strings.Contains(l, "chromium/"):
		return "Chrome"
	case strings.Contains(l, "safari/"):
		return "Safari"
	case strings.Contains(l, "msie "), strings.Contains(l, "trident/"):
		return "Internet Explorer"
	}
	return ""
}

func osOf(l string) string {
	switch {
	// Android before Linux: every Android UA also says Linux.
	case strings.Contains(l, "android"):
		return "Android"
	case strings.Contains(l, "iphone"), strings.Contains(l, "ipad"), strings.Contains(l, "ipod"):
		return "iOS"
	case strings.Contains(l, "windows nt"):
		return "Windows"
	case strings.Contains(l, "cros"):
		return "ChromeOS"
	// "mac os x" also appears on iOS iPad UAs, which the iPhone/iPad case above already took.
	case strings.Contains(l, "mac os x"), strings.Contains(l, "macintosh"):
		return "macOS"
	case strings.Contains(l, "linux"):
		return "Linux"
	}
	return ""
}

func deviceOf(l string) string {
	switch {
	case strings.Contains(l, "ipad"), strings.Contains(l, "tablet"):
		return "tablet"
	// An Android tablet omits "Mobile" while an Android phone includes it, which is the only
	// reliable signal Android gives.
	case strings.Contains(l, "android") && !strings.Contains(l, "mobile"):
		return "tablet"
	case strings.Contains(l, "mobile"), strings.Contains(l, "iphone"), strings.Contains(l, "ipod"):
		return "mobile"
	}
	return "desktop"
}

// platformOf distinguishes a page inside an app's embedded browser from one in a real browser.
//
// This is the generic half of the signal. A host application that stamps its own token into
// the user-agent can identify itself far more precisely, and does so through SiteResolver
// rather than here, because the token's format is that application's business and not this
// engine's.
func platformOf(l string) string {
	switch {
	// Android WebView's marker. The "; wv)" form is unambiguous; Version/x.y with Chrome is
	// the older signature.
	case strings.Contains(l, "; wv)"), strings.Contains(l, " wv "):
		return "mobile_app"
	// iOS in-app browsers omit the Safari token entirely while still being iOS WebKit.
	case (strings.Contains(l, "iphone") || strings.Contains(l, "ipad")) &&
		strings.Contains(l, "applewebkit") && !strings.Contains(l, "safari/"):
		return "mobile_app"
	}
	return "web"
}

// looksLikeBot catches the self-declaring majority.
//
// It will not catch a crawler that impersonates a browser, and it is not meant to: this keeps
// obvious crawler traffic out of a customer's pageview count, which is a data-quality measure,
// not a security one. Anything cleverer belongs in front of the engine.
func looksLikeBot(l string) bool {
	for _, s := range []string{
		"bot", "crawler", "spider", "crawling",
		"slurp", "curl/", "wget/", "python-requests", "go-http-client",
		"headlesschrome", "phantomjs", "lighthouse", "pingdom", "uptimerobot",
		"facebookexternalhit", "preview", "monitoring", "scrapy",
	} {
		if strings.Contains(l, s) {
			return true
		}
	}
	return false
}
