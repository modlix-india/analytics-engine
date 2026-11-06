package query

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/rollup"
	"github.com/modlix-india/analytics-engine/internal/store"
)

// fixtureSites is fixture for rows spread over several sites.
func fixtureSites(t *testing.T, rows []store.Row) string {
	t.Helper()
	dir := t.TempDir()
	type key struct{ site, date string }
	by := map[key][]store.Row{}
	for _, r := range rows {
		k := key{r.Site, time.UnixMilli(r.TSServer).UTC().Format("2006-01-02")}
		by[k] = append(by[k], r)
	}
	for k, rs := range by {
		site := store.SafeSegment(k.site)
		if err := store.WriteFile(filepath.Join(dir, "data", site, k.date, "n1-000000000001.parquet"), rs); err != nil {
			t.Fatal(err)
		}
		if err := rollup.WriteFile(filepath.Join(dir, "rollup", site, k.date, "n1-000000000001.parquet"), rollup.Build(rs)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func crawlHit(ts time.Time, bot, path, channel string, status string) store.Row {
	return store.Row{
		TSServer: ts.UnixMilli(), Site: "m_SYSTEM.crawl", Name: "$crawl",
		Path: path, Label: bot, Channel: channel, Props: `{"status":` + status + `}`,
	}
}

func TestCrawlerVisitsByBotAndPage(t *testing.T) {
	d := utc(2026, 11, 2, 10, 0)
	dir := fixtureSites(t, []store.Row{
		crawlHit(d, "GPTBot", "/page/home", "ai", "200"),
		crawlHit(d.Add(time.Minute), "GPTBot", "/page/home", "ai", "200"),
		crawlHit(d.Add(2*time.Minute), "GPTBot", "/page/gone", "ai", "404"),
		crawlHit(d.Add(24*time.Hour), "PerplexityBot", "/page/home", "ai", "200"),
		crawlHit(d.Add(25*time.Hour), "Googlebot", "/page/home", "crawler", "200"),
		// Outside the range.
		crawlHit(d.Add(-48*time.Hour), "GPTBot", "/page/home", "ai", "200"),
		// A real visitor on the site itself: never in the crawl answer.
		pvOn(d, "m_SYSTEM", "/page/home", "v1", "", ""),
	})
	e := New(dir)
	req := Request{Widget: WidgetCrawlerVisits, Site: "m_SYSTEM", From: utc(2026, 11, 1, 0, 0), To: utc(2026, 11, 8, 0, 0), Limit: 50}
	res, err := e.Query(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	c := res.Crawl
	if c == nil || c.Hits != 5 || c.AIHits != 4 {
		t.Fatalf("crawl = %+v", c)
	}
	if len(c.Bots) != 3 || c.Bots[0].Bot != "GPTBot" || c.Bots[0].Hits != 3 || c.Bots[0].Pages != 2 || !c.Bots[0].AI {
		t.Errorf("bots = %+v", c.Bots)
	}
	if c.Pages[0].Bot != "GPTBot" || c.Pages[0].Path != "/page/home" || c.Pages[0].Hits != 2 || c.Pages[0].Status != 200 {
		t.Errorf("pages[0] = %+v", c.Pages[0])
	}
	var gone *CrawlPage
	for i := range c.Pages {
		if c.Pages[i].Path == "/page/gone" {
			gone = &c.Pages[i]
		}
	}
	if gone == nil || gone.Errors != 1 || gone.Status != 404 {
		t.Errorf("gone = %+v", gone)
	}
	if len(c.Days) != 2 || c.Days[0].Hits != 3 || c.Days[1].Hits != 2 || c.Days[1].AIHits != 1 {
		t.Errorf("days = %+v", c.Days)
	}
	if len(res.Rows) != 3 || res.Rows[0].Label != "GPTBot" || res.Rows[0].Events != 3 {
		t.Errorf("rows = %+v", res.Rows)
	}

	req.AIOnly = true
	res, err = e.Query(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Crawl.Hits != 4 || len(res.Crawl.Bots) != 2 {
		t.Errorf("aiOnly = %+v", res.Crawl)
	}

	// The site's own traffic widgets never see a crawler.
	tp, err := e.Query(context.Background(), Request{Widget: WidgetTopEvents, Site: "m_SYSTEM", From: req.From, To: req.To})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range tp.Rows {
		if r.Label == "$crawl" {
			t.Errorf("topEvents counts crawler visits: %+v", tp.Rows)
		}
	}
}

func pvOn(ts time.Time, site, path, visitor, ref, utmSource string) store.Row {
	ch := "direct"
	if ref != "" {
		ch = "referral"
	}
	return store.Row{
		TSServer: ts.UnixMilli(), Site: site, Name: "$pageview", Path: path,
		Visitor: visitor, Session: "sess-" + visitor, ReferrerHost: ref, UTMSource: utmSource, Channel: ch,
	}
}

func TestAIReferralsBySourceAndLanding(t *testing.T) {
	d := utc(2026, 11, 3, 9, 0)
	gem := pvOn(d.Add(3*time.Minute), "m_SYSTEM", "/page/faq", "v3", "gemini.google.com", "")
	// Stored before the ai channel existed: filed as organic. Still counted.
	gem.Channel = "organic"
	paid := pvOn(d.Add(4*time.Minute), "m_SYSTEM", "/page/home", "v5", "", "chatgpt")
	paid.Channel = "paid"
	dir := fixtureSites(t, []store.Row{
		pvOn(d, "m_SYSTEM", "/page/pricing", "v1", "chatgpt.com", "chatgpt.com"),
		pvOn(d.Add(time.Minute), "m_SYSTEM", "/page/home", "v1", "chatgpt.com", ""),
		pvOn(d.Add(2*time.Minute), "m_SYSTEM", "/page/home", "v2", "", "chatgpt.com"),
		gem,
		pvOn(d.Add(5*time.Minute), "m_SYSTEM", "/page/home", "v4", "google.com", ""),
		paid,
		pvOn(d.Add(6*time.Minute), "m_SYSTEM", "/page/home", "v6", "www.perplexity.ai", ""),
	})
	e := New(dir)
	res, err := e.Query(context.Background(), Request{
		Widget: WidgetAIReferrals, Site: "m_SYSTEM", From: utc(2026, 11, 1, 0, 0), To: utc(2026, 11, 8, 0, 0), Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	a := res.AIReferrals
	if a == nil || a.Visits != 4 || a.Visitors != 4 || a.Pageviews != 5 {
		t.Fatalf("aiReferrals = %+v", a)
	}
	if len(res.Rows) != 3 || res.Rows[0].Label != "ChatGPT" || res.Rows[0].Visits != 2 || res.Rows[0].Events != 3 {
		t.Errorf("rows = %+v", res.Rows)
	}
	found := false
	for _, l := range a.Landing {
		if l.Source == "ChatGPT" && l.Path == "/page/pricing" && l.Visits == 1 {
			found = true
		}
		if l.Source == "ChatGPT" && l.Path == "/page/home" && l.Visits != 1 {
			t.Errorf("v1's second page is not a landing: %+v", a.Landing)
		}
	}
	if !found {
		t.Errorf("landing = %+v", a.Landing)
	}
	if res.Audience == nil || res.Audience.Views != 7 {
		t.Errorf("audience = %+v", res.Audience)
	}
}
