package ingest

import "testing"

// The ordering traps. Every one of these strings contains a token belonging to a browser it is
// not, which is why the checks in browserOf run most-specific first. A regression here is
// silent: the numbers stay plausible and simply attribute Edge users to Chrome.
func TestParseUABrowserOrdering(t *testing.T) {
	for _, tc := range []struct{ name, ua, want string }{
		{
			"Edge claims both Chrome and Safari",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36 Edg/120.0",
			"Edge",
		},
		{
			"Chrome claims Safari",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
			"Chrome",
		},
		{
			"Opera claims Chrome and Safari",
			"Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 Chrome/120.0 Safari/537.36 OPR/106.0",
			"Opera",
		},
		{
			"Samsung Internet claims Chrome",
			"Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 SamsungBrowser/23.0 Chrome/115.0 Mobile Safari/537.36",
			"Samsung Internet",
		},
		{
			"Chrome on iOS reports CriOS, and the user chose Chrome",
			"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 CriOS/120.0 Mobile/15E148 Safari/604.1",
			"Chrome",
		},
		{
			"real Safari",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
			"Safari",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseUA(tc.ua).Browser; got != tc.want {
				t.Errorf("Browser = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseUAOSAndDevice(t *testing.T) {
	for _, tc := range []struct{ name, ua, os, device string }{
		{
			"Android phone says Linux too, and Android must win",
			"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36",
			"Android", "mobile",
		},
		{
			"Android tablet omits Mobile",
			"Mozilla/5.0 (Linux; Android 13; SM-X200) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
			"Android", "tablet",
		},
		{
			"iPad",
			"Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
			"iOS", "tablet",
		},
		{
			"desktop Windows",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
			"Windows", "desktop",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseUA(tc.ua)
			if got.OS != tc.os {
				t.Errorf("OS = %q, want %q", got.OS, tc.os)
			}
			if got.Device != tc.device {
				t.Errorf("Device = %q, want %q", got.Device, tc.device)
			}
		})
	}
}

func TestParseUAPlatform(t *testing.T) {
	webview := "Mozilla/5.0 (Linux; Android 13; Pixel 7 Build/TQ3A; wv) AppleWebKit/537.36 Version/4.0 Chrome/120.0 Mobile Safari/537.36"
	if got := ParseUA(webview).Platform; got != "mobile_app" {
		t.Errorf("Android WebView Platform = %q, want mobile_app", got)
	}

	browser := "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36"
	if got := ParseUA(browser).Platform; got != "web" {
		t.Errorf("Android Chrome Platform = %q, want web", got)
	}
}

func TestBotDetection(t *testing.T) {
	for _, ua := range []string{
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"curl/8.4.0",
		"python-requests/2.31.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 HeadlessChrome/120.0 Safari/537.36",
	} {
		if !ParseUA(ua).Bot {
			t.Errorf("ParseUA(%q).Bot = false, want true", ua)
		}
	}

	real := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
	if ParseUA(real).Bot {
		t.Errorf("a real browser was classified as a bot")
	}
}

// Paid must be decided before organic. A visitor from a Google ad and one from a Google search
// result have the same referrer; only the campaign parameters separate them. Reporting paid
// traffic as organic is the most expensive mistake this function can make.
func TestChannelPaidBeatsOrganic(t *testing.T) {
	utmPaid := UTM{Source: "google", Medium: "cpc"}
	if got := Channel("google.com", "site.com", utmPaid); got != "paid" {
		t.Errorf("Channel with utm_medium=cpc = %q, want paid", got)
	}

	// The common real-world case: an ad link with a gclid and no utm_medium at all.
	utmClickID := UTM{hasClickID: true}
	if got := Channel("google.com", "site.com", utmClickID); got != "paid" {
		t.Errorf("Channel with a gclid = %q, want paid", got)
	}

	if got := Channel("google.com", "site.com", UTM{}); got != "organic" {
		t.Errorf("Channel from a plain Google referrer = %q, want organic", got)
	}
}

func TestChannelClassification(t *testing.T) {
	for _, tc := range []struct{ name, ref, page, want string }{
		{"no referrer is direct", "", "site.com", "direct"},
		{"same site is internal", "site.com", "site.com", "internal"},
		{"www and bare are the same site", "www.site.com", "site.com", "internal"},
		{"social", "facebook.com", "site.com", "social"},
		{"social shortlink", "t.co", "site.com", "social"},
		{"search country domain", "google.co.in", "site.com", "organic"},
		{"anything else is referral", "someblog.example", "site.com", "referral"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Channel(tc.ref, tc.page, UTM{}); got != tc.want {
				t.Errorf("Channel(%q, %q) = %q, want %q", tc.ref, tc.page, got, tc.want)
			}
		})
	}
}

// Without normalisation these are three rows in a top-pages table that should be one, and the
// table becomes useless exactly when a site has enough traffic to need it.
func TestPathNormalisation(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"https://site.com/pricing", "/pricing"},
		{"https://site.com/pricing/", "/pricing"},
		{"https://site.com/pricing?utm_source=x", "/pricing"},
		{"https://site.com/pricing#features", "/pricing"},
		{"https://site.com", "/"},
		{"https://site.com/", "/"},
	} {
		_, got, _, ok := ParsePageURL(tc.raw)
		if !ok {
			t.Fatalf("ParsePageURL(%q) not ok", tc.raw)
		}
		if got != tc.want {
			t.Errorf("ParsePageURL(%q) path = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestUTMComesFromPageURLNotReferrer(t *testing.T) {
	// The campaign parameters are on our own landing URL, placed there by the campaign link.
	_, _, utm, ok := ParsePageURL("https://site.com/p?utm_source=google&utm_medium=cpc&utm_campaign=spring")
	if !ok {
		t.Fatal("not ok")
	}
	if utm.Source != "google" || utm.Medium != "cpc" || utm.Campaign != "spring" {
		t.Errorf("utm = %+v, want source=google medium=cpc campaign=spring", utm)
	}
}
