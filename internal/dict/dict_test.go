package dict

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// okConfiguration is a full /api/system/configuration body as Remnawave 3.2.0
// serves it, including the fields the exporter deliberately ignores.
const okConfiguration = `{"response":{
	"notifications":{"webhook":false,"bandwidthUsage":null,"notConnectedAfter":null,"expirationNotifications":null},
	"service":{"cleanUsageHistory":true,"disableUserUsageRecords":false,"disableSrhRecords":true,"exportToRedisStream":true},
	"misc":{"shortUuidLength":16,"subPublicDomain":"example.com","userUsageIgnoreBelowBytes":4096}}}`

// newTestSyncer points a Syncer at url and captures its log output. The sink
// stays nil on purpose: checkConfiguration never writes to ClickHouse.
func newTestSyncer(url string) (*Syncer, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(Options{APIURL: url, APIToken: "token", Interval: time.Minute}, nil, log), buf
}

// logged decodes the captured JSON log lines.
func logged(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestCheckConfigurationReportsPanelSettings(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okConfiguration))
	}))
	defer srv.Close()

	s, buf := newTestSyncer(srv.URL)
	s.checkConfiguration(context.Background())

	if gotPath != "/api/system/configuration" {
		t.Errorf("requested %q, want /api/system/configuration", gotPath)
	}
	if gotAuth != "Bearer token" {
		t.Errorf("Authorization = %q, want Bearer token", gotAuth)
	}

	recs := logged(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1: %v", len(recs), recs)
	}
	rec := recs[0]
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}
	// Reported verbatim: it is the only setting that drops traffic upstream of
	// the stream, so it explains small deltas missing from ClickHouse.
	if rec["user_usage_ignore_below_bytes"] != float64(4096) {
		t.Errorf("user_usage_ignore_below_bytes = %v, want 4096", rec["user_usage_ignore_below_bytes"])
	}
	if rec["user_usage_records_disabled"] != false {
		t.Errorf("user_usage_records_disabled = %v, want false", rec["user_usage_records_disabled"])
	}
	if rec["srh_records_disabled"] != true {
		t.Errorf("srh_records_disabled = %v, want true", rec["srh_records_disabled"])
	}
}

func TestCheckConfigurationWarnsWhenStreamExportIsOff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Replace(okConfiguration,
			`"exportToRedisStream":true`, `"exportToRedisStream":false`, 1)))
	}))
	defer srv.Close()

	s, buf := newTestSyncer(srv.URL)
	s.checkConfiguration(context.Background())

	recs := logged(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1: %v", len(recs), recs)
	}
	if recs[0]["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", recs[0]["level"])
	}
	// The message has to name the panel setting; that is the whole point.
	if msg, _ := recs[0]["msg"].(string); !strings.Contains(msg, "EXPORT_TO_STREAM_ENABLED") {
		t.Errorf("msg = %q, want it to name EXPORT_TO_STREAM_ENABLED", msg)
	}
}

// A panel older than 3.2.0 and a narrowly scoped token are both ordinary
// setups, so neither may produce anything louder than a debug line.
func TestCheckConfigurationStaysQuietOnErrors(t *testing.T) {
	for name, code := range map[string]int{
		"pre-3.2.0 panel":  http.StatusNotFound,
		"method not known": http.StatusMethodNotAllowed,
		"token unscoped":   http.StatusForbidden,
		"token rejected":   http.StatusUnauthorized,
		"panel broken":     http.StatusInternalServerError,
		"garbage body":     http.StatusOK,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
				if code == http.StatusOK {
					_, _ = w.Write([]byte("not json"))
				}
			}))
			defer srv.Close()

			s, buf := newTestSyncer(srv.URL)
			s.checkConfiguration(context.Background())

			for _, rec := range logged(t, buf) {
				if rec["level"] != "DEBUG" {
					t.Errorf("level = %v, want DEBUG (msg %v)", rec["level"], rec["msg"])
				}
			}
		})
	}
}

func TestCheckConfigurationSkippedWithoutCredentials(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	defer srv.Close()

	for name, opt := range map[string]Options{
		"no token": {APIURL: srv.URL},
		"no url":   {APIToken: "token"},
	} {
		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			log := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			New(opt, nil, log).checkConfiguration(context.Background())

			if called {
				t.Error("panel was called without credentials configured")
			}
			if buf.Len() != 0 {
				t.Errorf("logged %q, want silence", buf.String())
			}
		})
	}
}

// The typed error keeps the message the dictionary sync has always logged.
func TestStatusErrorMessage(t *testing.T) {
	err := &statusError{URL: "http://panel/api/nodes", Status: "404 Not Found", Code: 404}
	if got, want := err.Error(), "GET http://panel/api/nodes: 404 Not Found"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestParsePanelVersion(t *testing.T) {
	for raw, want := range map[string]panelVersion{
		"3.3.2":     {3, 3, 2},
		"v3.1.0":    {3, 1, 0},
		" 3.2.0 ":   {3, 2, 0},
		"3.4.0-dev": {3, 4, 0},
		"10.0.11":   {10, 0, 11},
		// Tolerated on purpose: a floor check only ever needs the first three
		// numbers, whatever a build appends after them.
		"3.3.2.1": {3, 3, 2},
	} {
		got, ok := parsePanelVersion(raw)
		if !ok || got != want {
			t.Errorf("parsePanelVersion(%q) = %v, %v; want %v, true", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"", "3.3", "next", "3.x.2", "3.3.x"} {
		if got, ok := parsePanelVersion(raw); ok {
			t.Errorf("parsePanelVersion(%q) = %v, true; want false", raw, got)
		}
	}
}

func TestPanelVersionOrdering(t *testing.T) {
	if !(panelVersion{3, 0, 9}).less(minPanelVersion) {
		t.Error("3.0.9 must sort below the 3.1.0 floor")
	}
	if (panelVersion{3, 1, 0}).less(minPanelVersion) {
		t.Error("3.1.0 is the floor itself, not below it")
	}
	if (panelVersion{3, 3, 2}).less(minPanelVersion) {
		t.Error("3.3.2 must sort above the 3.1.0 floor")
	}
}

func TestCheckVersionReportsPanelVersion(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"response":{"version":"3.3.2",` +
			`"build":{"time":"2026-08-20T03:00:00Z","number":"1"},` +
			`"git":{"backend":{"commitSha":"abc","branch":"main","commitUrl":"u"},` +
			`"frontend":{"commitSha":"def","commitUrl":"u"}}}}`))
	}))
	defer srv.Close()

	s, buf := newTestSyncer(srv.URL)
	s.checkVersion(context.Background())

	if gotPath != "/api/system/metadata" {
		t.Errorf("requested %q, want /api/system/metadata", gotPath)
	}
	recs := logged(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1: %v", len(recs), recs)
	}
	if recs[0]["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", recs[0]["level"])
	}
	if recs[0]["panel_version"] != "3.3.2" {
		t.Errorf("panel_version = %v, want 3.3.2", recs[0]["panel_version"])
	}
}

// A panel below 3.1.0 still runs, it just cannot name nodes or carry the SRR
// fields, so the exporter has to say so out loud once.
func TestCheckVersionWarnsOnOldPanel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"response":{"version":"3.0.5"}}`))
	}))
	defer srv.Close()

	s, buf := newTestSyncer(srv.URL)
	s.checkVersion(context.Background())

	recs := logged(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1: %v", len(recs), recs)
	}
	if recs[0]["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", recs[0]["level"])
	}
	if recs[0]["panel_version"] != "3.0.5" {
		t.Errorf("panel_version = %v, want 3.0.5", recs[0]["panel_version"])
	}
}

// Same rule as the configuration preflight: an endpoint the panel does not
// have, a token without the scope and an unreadable version are all ordinary,
// and none of them may be louder than a debug line.
func TestCheckVersionStaysQuietOnErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		code int
		body string
	}{
		"endpoint missing": {http.StatusNotFound, ""},
		"token unscoped":   {http.StatusForbidden, ""},
		"panel broken":     {http.StatusInternalServerError, ""},
		"garbage body":     {http.StatusOK, "not json"},
		"version unusable": {http.StatusOK, `{"response":{"version":"next"}}`},
		"version missing":  {http.StatusOK, `{"response":{}}`},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			s, buf := newTestSyncer(srv.URL)
			s.checkVersion(context.Background())

			for _, rec := range logged(t, buf) {
				if rec["level"] != "DEBUG" {
					t.Errorf("level = %v, want DEBUG (msg %v)", rec["level"], rec["msg"])
				}
			}
		})
	}
}

func TestCheckVersionSkippedWithoutCredentials(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer srv.Close()

	for name, opt := range map[string]Options{
		"no token": {APIURL: srv.URL},
		"no url":   {APIToken: "token"},
	} {
		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			log := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			New(opt, nil, log).checkVersion(context.Background())

			if called {
				t.Error("panel was called without credentials configured")
			}
			if buf.Len() != 0 {
				t.Errorf("logged %q, want silence", buf.String())
			}
		})
	}
}
