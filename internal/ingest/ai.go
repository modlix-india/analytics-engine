package ingest

import "strings"

// ChannelAI is a visit that came from an AI answer: somebody asked ChatGPT, Perplexity,
// Copilot, Gemini or Claude, and followed the link it cited.
//
// A channel of its own rather than "referral", because it is the number answer engine work
// is judged by, and inside "referral" it is indistinguishable from a link on a blog.
const ChannelAI = "ai"

// aiSources names the assistants, each with the hosts its links arrive from and the
// utm_source values it stamps on them. The name is what a report shows, so it is the
// product's own name rather than a hostname.
//
// Hosts match exactly or as a parent domain (www.perplexity.ai is perplexity.ai). The
// android-app:// referrers of the mobile apps parse to their package name as the host, which
// is why those appear here as hosts.
var aiSources = []struct {
	name  string
	hosts []string
	utm   []string
}{
	{"ChatGPT", []string{"chatgpt.com", "chat.openai.com", "com.openai.chatgpt"}, []string{"chatgpt.com", "chatgpt", "openai", "chat.openai.com"}},
	{"Perplexity", []string{"perplexity.ai", "ai.perplexity.app"}, []string{"perplexity", "perplexity.ai"}},
	{"Copilot", []string{"copilot.microsoft.com", "copilot.cloud.microsoft", "edgeservices.bing.com", "com.microsoft.copilot"}, []string{"copilot", "copilot.microsoft.com", "bing_chat"}},
	{"Gemini", []string{"gemini.google.com", "bard.google.com", "com.google.android.apps.bard"}, []string{"gemini", "gemini.google.com", "bard"}},
	{"Claude", []string{"claude.ai", "com.anthropic.claude"}, []string{"claude", "claude.ai"}},
	{"Mistral", []string{"chat.mistral.ai"}, []string{"mistral", "chat.mistral.ai"}},
	{"DeepSeek", []string{"chat.deepseek.com"}, []string{"deepseek", "chat.deepseek.com"}},
	{"Meta AI", []string{"meta.ai"}, []string{"meta.ai", "metaai"}},
	{"Grok", []string{"grok.com"}, []string{"grok", "grok.com"}},
	{"You.com", []string{"you.com"}, []string{"you.com"}},
	{"Phind", []string{"phind.com"}, []string{"phind", "phind.com"}},
	{"Poe", []string{"poe.com"}, []string{"poe", "poe.com"}},
	{"Duck.ai", []string{"duck.ai"}, []string{"duck.ai"}},
}

// AISource names the assistant a visit came from, or "" when it did not come from one.
//
// The referrer is asked first, the landing URL's utm_source second: a browser that sent a
// referrer saw where the click happened, whereas a utm_source is whatever the link said. Both
// are kept because each misses cases the other catches. ChatGPT's in-app browser often sends
// no referrer, and plenty of assistants link without any campaign parameters.
func AISource(refHost, utmSource string) string {
	if h := strings.TrimPrefix(strings.ToLower(refHost), "www."); h != "" {
		for _, s := range aiSources {
			for _, d := range s.hosts {
				if h == d || strings.HasSuffix(h, "."+d) {
					return s.name
				}
			}
		}
	}
	if u := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(utmSource)), "www."); u != "" {
		for _, s := range aiSources {
			for _, v := range s.utm {
				if u == v {
					return s.name
				}
			}
		}
	}
	return ""
}
