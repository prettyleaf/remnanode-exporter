// Package ua classifies subscription-fetch User-Agents.
//
// The point is not browser analytics: it is telling a real VPN client apart
// from a script that hammers the subscription endpoint, and spotting one
// account whose subscription is pulled by a dozen different clients.
package ua

import "strings"

// Kind is the coarse category of a client.
type Kind string

const (
	KindClient  Kind = "client"  // known VPN/proxy app
	KindBrowser Kind = "browser" // someone opened the link by hand
	KindScript  Kind = "script"  // curl/wget/python/go — bot or scraper
	KindBot     Kind = "bot"     // declared crawler
	KindUnknown Kind = "unknown"
)

type rule struct {
	needle string
	name   string
	kind   Kind
}

// Ordered: the first match wins, so put specific tokens before generic ones.
var rules = []rule{
	{"happ", "Happ", KindClient},
	{"streisand", "Streisand", KindClient},
	{"hiddify", "Hiddify", KindClient},
	{"v2rayng", "v2rayNG", KindClient},
	{"v2raytun", "v2RayTun", KindClient},
	{"v2rayn", "v2rayN", KindClient},
	{"nekobox", "NekoBox", KindClient},
	{"nekoray", "NekoRay", KindClient},
	{"shadowrocket", "Shadowrocket", KindClient},
	{"stash", "Stash", KindClient},
	{"loon", "Loon", KindClient},
	{"quantumult", "Quantumult", KindClient},
	{"surge", "Surge", KindClient},
	{"clash-verge", "Clash Verge", KindClient},
	{"clashx", "ClashX", KindClient},
	{"clash", "Clash", KindClient},
	{"mihomo", "Mihomo", KindClient},
	{"sing-box", "sing-box", KindClient},
	{"singbox", "sing-box", KindClient},
	{"foxray", "FoXray", KindClient},
	{"karing", "Karing", KindClient},
	{"throne", "Throne", KindClient},
	{"husi", "Husi", KindClient},
	{"exclave", "Exclave", KindClient},
	{"matsuri", "Matsuri", KindClient},
	{"v2box", "V2Box", KindClient},
	{"sfa", "sing-box for Android", KindClient},
	{"sfi", "sing-box for iOS", KindClient},
	{"sfm", "sing-box for macOS", KindClient},

	{"curl", "curl", KindScript},
	{"wget", "wget", KindScript},
	{"python-requests", "python-requests", KindScript},
	{"python-urllib", "python-urllib", KindScript},
	{"aiohttp", "aiohttp", KindScript},
	{"httpx", "httpx", KindScript},
	{"go-http-client", "Go-http-client", KindScript},
	{"okhttp", "OkHttp", KindScript},
	{"axios", "axios", KindScript},
	{"node-fetch", "node-fetch", KindScript},
	{"powershell", "PowerShell", KindScript},
	{"java/", "Java", KindScript},
	{"libwww-perl", "libwww-perl", KindScript},
	{"postman", "Postman", KindScript},
	{"insomnia", "Insomnia", KindScript},

	{"telegrambot", "TelegramBot", KindBot},
	{"crawler", "Crawler", KindBot},
	{"spider", "Spider", KindBot},
	{"bot", "Bot", KindBot},

	{"edg/", "Edge", KindBrowser},
	{"opr/", "Opera", KindBrowser},
	{"yabrowser", "Yandex Browser", KindBrowser},
	{"firefox", "Firefox", KindBrowser},
	{"chrome", "Chrome", KindBrowser},
	{"safari", "Safari", KindBrowser},
	{"mozilla", "Browser", KindBrowser},
}

// Result is the classification of one User-Agent string.
type Result struct {
	Family string
	Kind   Kind
}

// Classify maps a raw User-Agent to a stable family name and category.
// An empty User-Agent is itself a signal, so it gets its own family.
func Classify(raw string) Result {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return Result{Family: "(empty)", Kind: KindUnknown}
	}
	for _, r := range rules {
		if strings.Contains(s, r.needle) {
			return Result{Family: r.name, Kind: r.kind}
		}
	}
	return Result{Family: "Other", Kind: KindUnknown}
}
