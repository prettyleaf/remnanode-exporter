package ua

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		raw    string
		family string
		kind   Kind
	}{
		{"Happ/1.24.0 (iOS)", "Happ", KindClient},
		{"v2rayNG/1.8.19", "v2rayNG", KindClient},
		{"Hiddify/2.0.5", "Hiddify", KindClient},
		{"SFA/1.9.0 (sing-box 1.9.0)", "sing-box", KindClient},
		{"clash-verge/1.5.11", "Clash Verge", KindClient},
		{"curl/8.5.0", "curl", KindScript},
		{"python-requests/2.31.0", "python-requests", KindScript},
		{"Go-http-client/2.0", "Go-http-client", KindScript},
		{"Mozilla/5.0 (Windows NT 10.0) Chrome/120.0", "Chrome", KindBrowser},
		{"TelegramBot (like TwitterBot)", "TelegramBot", KindBot},
		{"", "(empty)", KindUnknown},
		{"totally-made-up/1", "Other", KindUnknown},
	}
	for _, c := range cases {
		got := Classify(c.raw)
		if got.Family != c.family || got.Kind != c.kind {
			t.Errorf("Classify(%q) = %+v, want {%s %s}", c.raw, got, c.family, c.kind)
		}
	}
}

func TestClassifyPrefersSpecificClientOverBrowserToken(t *testing.T) {
	// Several clients ship a Mozilla-style UA; the client token must win.
	got := Classify("Mozilla/5.0 Streisand/1.6.7")
	if got.Family != "Streisand" {
		t.Errorf("Classify = %+v, want Streisand", got)
	}
}
