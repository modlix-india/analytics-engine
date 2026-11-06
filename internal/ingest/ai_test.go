package ingest

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAISourceFromReferrerAndUTM(t *testing.T) {
	for _, tc := range []struct{ ref, utm, want string }{
		{"chatgpt.com", "", "ChatGPT"},
		{"www.chatgpt.com", "", "ChatGPT"},
		{"chat.openai.com", "", "ChatGPT"},
		{"", "chatgpt.com", "ChatGPT"},
		{"", " ChatGPT.com ", "ChatGPT"},
		{"www.perplexity.ai", "", "Perplexity"},
		{"copilot.microsoft.com", "", "Copilot"},
		{"gemini.google.com", "", "Gemini"},
		{"claude.ai", "", "Claude"},
		{"com.openai.chatgpt", "", "ChatGPT"},
		// The referrer wins over a campaign that names someone else.
		{"perplexity.ai", "chatgpt.com", "Perplexity"},
		{"google.com", "", ""},
		{"openai.com", "", ""},
		{"notchatgpt.com", "", ""},
		{"", "newsletter", ""},
		{"", "", ""},
	} {
		if got := AISource(tc.ref, tc.utm); got != tc.want {
			t.Errorf("AISource(%q, %q) = %q, want %q", tc.ref, tc.utm, got, tc.want)
		}
	}
}

func TestChannelAI(t *testing.T) {
	// gemini.google.com would read as a Google search without the AI rule going first.
	if got := Channel("gemini.google.com", "site.com", UTM{}); got != ChannelAI {
		t.Errorf("gemini referrer = %q, want ai", got)
	}
	// ChatGPT's own links: a utm_source and nothing else, often with no referrer.
	if got := Channel("", "site.com", UTM{Source: "chatgpt.com"}); got != ChannelAI {
		t.Errorf("utm_source=chatgpt.com = %q, want ai", got)
	}
	if got := Channel("chatgpt.com", "site.com", UTM{Source: "chatgpt.com", Medium: "referral"}); got != ChannelAI {
		t.Errorf("chatgpt with medium referral = %q, want ai", got)
	}
	// A paid campaign naming an assistant is still paid.
	if got := Channel("", "site.com", UTM{Source: "chatgpt", Medium: "cpc"}); got != "paid" {
		t.Errorf("paid chatgpt campaign = %q, want paid", got)
	}
	// Ordinary search and referral are untouched.
	if got := Channel("google.com", "site.com", UTM{}); got != "organic" {
		t.Errorf("google = %q, want organic", got)
	}
	if got := Channel("blog.example", "site.com", UTM{}); got != "referral" {
		t.Errorf("blog = %q, want referral", got)
	}
}

func newCrawl(sink Sink, outcomes *[]string) *CrawlIngester {
	return NewCrawl(CrawlOptions{
		Sink: sink, Secret: "s3cret",
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEvent: func(o string) { *outcomes = append(*outcomes, o) },
		Now:     func() time.Time { return time.UnixMilli(1_790_000_000_000) },
	})
}

func postCrawl(c *CrawlIngester, auth, body string) int {
	req := httptest.NewRequest(http.MethodPost, "/c", strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	c.Handle(rec, req)
	return rec.Code
}

func TestCrawlNeedsTheSecret(t *testing.T) {
	sink := &memSink{}
	var outcomes []string
	c := newCrawl(sink, &outcomes)
	body := `{"b":[{"s":"monkbars_SYSTEM","u":"/page/home","bot":"GPTBot","ai":true,"st":200}]}`
	for _, auth := range []string{"", "Bearer wrong", "s3cret", "Bearer s3cret2"} {
		if code := postCrawl(c, auth, body); code != http.StatusForbidden {
			t.Errorf("auth %q: %d, want 403", auth, code)
		}
	}
	if len(sink.recs) != 0 {
		t.Fatalf("stored %d events without the secret", len(sink.recs))
	}
	empty := NewCrawl(CrawlOptions{Sink: sink})
	if code := postCrawl(empty, "Bearer ", body); code != http.StatusForbidden {
		t.Errorf("no secret configured: %d, want 403", code)
	}
}

func TestCrawlStoresUnderTheCrawlSite(t *testing.T) {
	sink := &memSink{}
	var outcomes []string
	c := newCrawl(sink, &outcomes)
	body := `{"b":[
		{"s":"monkbars_SYSTEM","u":"/page/home/?x=1#top","bot":"GPTBot","ai":true,"st":200},
		{"s":"monkbars_SYSTEM","u":"/page/missing","bot":"Googlebot","st":404},
		{"s":"../etc","u":"/","bot":"GPTBot"},
		{"s":"monkbars_SYSTEM.crawl","u":"/","bot":"GPTBot"},
		{"s":"monkbars_SYSTEM","u":"no-slash","bot":"GPTBot"},
		{"s":"monkbars_SYSTEM","u":"/","bot":""}
	]}`
	if code := postCrawl(c, "Bearer s3cret", body); code != http.StatusNoContent {
		t.Fatalf("status %d", code)
	}
	evs := sink.events(t)
	if len(evs) != 2 {
		t.Fatalf("stored %d, want 2 (%v)", len(evs), outcomes)
	}
	a, b := evs[0], evs[1]
	if a.Site != "monkbars_SYSTEM.crawl" || a.Name != NameCrawl || a.Path != "/page/home" ||
		a.Label != "GPTBot" || a.Channel != ChannelAI || a.Props != `{"status":200}` {
		t.Errorf("first = %+v", a)
	}
	if b.Channel != ChannelCrawler || b.Props != `{"status":404}` || b.Label != "Googlebot" {
		t.Errorf("second = %+v", b)
	}
	if a.Visitor != "" || a.Session != "" {
		t.Errorf("a crawler visit carries no identity: %+v", a)
	}
	invalid := 0
	for _, o := range outcomes {
		if o == "crawl_dropped_invalid" {
			invalid++
		}
	}
	if invalid != 4 {
		t.Errorf("invalid = %d, want 4 (%v)", invalid, outcomes)
	}
}
