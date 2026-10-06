package dict

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// okConfiguration is a full /api/system/configuration body as Remnawave 3.2.0
// through 3.4.5 serve it, including the fields the exporter deliberately
// ignores.
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

// TestDirectPanelRequestsPassProxyCheck mimics the panel's proxyCheckMiddleware,
// which drops the connection without a response unless the request carries
// X-Forwarded-For and X-Forwarded-Proto: https.
func TestDirectPanelRequestsPassProxyCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forwarded-For") == "" || r.Header.Get("X-Forwarded-Proto") != "https" {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"response":[]}`))
	}))
	defer srv.Close()

	s, _ := newTestSyncer(srv.URL)
	var decoded nodesResponse
	if err := s.getJSON(context.Background(), "/api/nodes", &decoded); err != nil {
		t.Fatalf("direct request to the panel was dropped: %v", err)
	}
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

// A user as /api/users/stream returns it on Remnawave 3.4.5, secrets included,
// so the test also pins that none of them reach a row.
const streamUserJSON = `{
	"id": 42, "shortUuid": "s3cr3tShortUuid", "username": "alice", "status": "ACTIVE",
	"trafficLimitBytes": 107374182400, "trafficLimitStrategy": "MONTH",
	"expireAt": "2026-12-01T00:00:00.000Z", "telegramId": 123456789, "email": null,
	"description": null, "tag": "VIP", "hwidDeviceLimit": 3, "externalSquadUuid": null,
	"trojanPassword": "pw", "vlessUuid": "6f1c9a2e-0000-4000-8000-000000000000", "ssPassword": "pw",
	"lastTriggeredThreshold": 0, "subRevokedAt": null, "lastTrafficResetAt": null,
	"createdAt": "2026-08-01T10:00:00.000Z", "updatedAt": "2026-10-01T10:00:00.000Z",
	"subscriptionUrl": "https://sub.example.com/s3cr3tShortUuid",
	"activeInternalSquads": [{"uuid": "a1b2c3d4-0000-4000-8000-000000000000", "name": "Premium"}],
	"userTraffic": {"usedTrafficBytes": 5368709120, "lifetimeUsedTrafficBytes": 53687091200,
		"onlineAt": "2026-10-06T12:00:00.000Z", "firstConnectedAt": null,
		"lastConnectedNodeUuid": "11111111-1111-4111-8111-111111111111"}
}`

func TestUserRowMapsStreamUser(t *testing.T) {
	var u streamUser
	if err := json.Unmarshal([]byte(streamUserJSON), &u); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	row := userRow(u, now)
	if len(row) != len(userColumns) {
		t.Fatalf("row has %d values, %d columns", len(row), len(userColumns))
	}
	got := map[string]any{}
	for i, col := range userColumns {
		got[col] = row[i]
	}
	epoch := time.Unix(0, 0).UTC()
	for col, want := range map[string]any{
		"user_id":                     uint64(42),
		"username":                    "alice",
		"tag":                         "VIP",
		"traffic_limit_strategy":      "MONTH",
		"hwid_device_limit":           int64(3),
		"telegram_id":                 int64(123456789),
		"used_traffic_bytes":          uint64(5368709120),
		"lifetime_used_traffic_bytes": uint64(53687091200),
		"created_at":                  time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		"online_at":                   time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC),
		// Null in the panel: stored as the epoch, which the schema reads as unset.
		"first_connected_at":       epoch,
		"sub_revoked_at":           epoch,
		"last_connected_node_uuid": "11111111-1111-4111-8111-111111111111",
		"updated_at":               now,
	} {
		if got[col] != want {
			t.Errorf("%s = %v, want %v", col, got[col], want)
		}
	}
	if squads, _ := got["internal_squads"].([]string); len(squads) != 1 || squads[0] != "Premium" {
		t.Errorf("internal_squads = %v, want [Premium]", got["internal_squads"])
	}
	for i, v := range row {
		if s, ok := v.(string); ok && strings.Contains(s, "s3cr3t") {
			t.Errorf("column %s carries the subscription secret: %q", userColumns[i], s)
		}
	}
}

func TestNodeRowCarriesProviderAndTags(t *testing.T) {
	var n apiNode
	if err := json.Unmarshal([]byte(`{"id": 7, "uuid": "u", "name": "de-1", "countryCode": "DE",
		"address": "de1.example.com", "tags": ["PREMIUM"], "provider": {"uuid": "p", "name": "Hetzner"},
		"ips": [{"ip": "203.0.113.7", "status": "OUTBOUND"}]}`), &n); err != nil {
		t.Fatal(err)
	}
	row := nodeRow(n, time.Unix(0, 0).UTC())
	if len(row) != len(nodeColumns) {
		t.Fatalf("row has %d values, %d columns", len(row), len(nodeColumns))
	}
	if row[4] != "de1.example.com" || row[5] != "Hetzner" {
		t.Errorf("address, provider = %v, %v", row[4], row[5])
	}
	if tags, _ := row[6].([]string); len(tags) != 1 || tags[0] != "PREMIUM" {
		t.Errorf("tags = %v", row[6])
	}

	// No provider and no tags must still produce insertable values.
	bare := nodeRow(apiNode{ID: 8}, time.Unix(0, 0).UTC())
	if bare[5] != "" {
		t.Errorf("provider = %v, want empty", bare[5])
	}
	if tags, ok := bare[6].([]string); !ok || tags == nil {
		t.Errorf("tags = %#v, want an empty, non-nil slice", bare[6])
	}
}

func TestNodeAddressesCollectsIPsAndResolvesHostnames(t *testing.T) {
	var nodes []apiNode
	if err := json.Unmarshal([]byte(`[
		{"id": 1, "address": "198.51.100.1", "ips": [{"ip": "203.0.113.7", "status": "OUTBOUND"},
			{"ip": "2001:db8::7", "status": "INBOUND"}]},
		{"id": 2, "address": "de1.example.com"},
		{"id": 3, "address": "gone.example.com"},
		{"id": 4, "address": ""}
	]`), &nodes); err != nil {
		t.Fatal(err)
	}
	lookup := func(_ context.Context, host string) ([]string, error) {
		if host == "de1.example.com" {
			return []string{"192.0.2.10"}, nil
		}
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}

	set := &IPSet{}
	set.replace(nodeAddresses(context.Background(), nodes, lookup))

	for _, ip := range []string{"198.51.100.1", "203.0.113.7", "2001:db8::7", "192.0.2.10", "::ffff:203.0.113.7"} {
		if !set.Contains(ip) {
			t.Errorf("%s is a node address but the set misses it", ip)
		}
	}
	for _, ip := range []string{"1.2.3.4", "not-an-ip", ""} {
		if set.Contains(ip) {
			t.Errorf("%q matched, want no match", ip)
		}
	}
	if set.Len() != 4 {
		t.Errorf("Len = %d, want 4", set.Len())
	}
}

// The decoders read the set from the moment they start, which is before the
// first node refresh has filled it.
func TestIPSetIsSafeBeforeTheFirstRefresh(t *testing.T) {
	var empty IPSet
	if empty.Contains("1.2.3.4") || empty.Len() != 0 {
		t.Error("an unfilled set must match nothing")
	}
	var nilSet *IPSet
	if nilSet.Contains("1.2.3.4") {
		t.Error("a nil set must match nothing")
	}
}

// hwidPanel serves total devices from /api/hwid/devices honouring start/size.
func hwidPanel(t *testing.T, total int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/hwid/devices" {
			http.NotFound(w, r)
			return
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("start"))
		size, _ := strconv.Atoi(r.URL.Query().Get("size"))
		var devices []string
		for i := start; i < min(start+size, total); i++ {
			devices = append(devices, fmt.Sprintf(`{"hwid":"hw-%d","userId":%d,"platform":"iOS","osVersion":"18.0",`+
				`"deviceModel":null,"userAgent":"Happ/1.0","requestIp":"1.2.3.4",`+
				`"createdAt":"2026-09-01T00:00:00.000Z","updatedAt":"2026-09-02T00:00:00.000Z"}`, i, i%7+1))
		}
		fmt.Fprintf(w, `{"response":{"devices":[%s],"total":%d}}`, strings.Join(devices, ","), total)
	}))
}

func TestFetchHwidDevicesPagesThroughTheList(t *testing.T) {
	srv := hwidPanel(t, 2*hwidPageSize+5)
	defer srv.Close()

	s, _ := newTestSyncer(srv.URL)
	devices, err := s.fetchHwidDevices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 2*hwidPageSize+5 {
		t.Fatalf("got %d devices, want %d", len(devices), 2*hwidPageSize+5)
	}
	seen := map[string]bool{}
	for _, d := range devices {
		if seen[d.HWID] {
			t.Fatalf("device %s fetched twice", d.HWID)
		}
		seen[d.HWID] = true
	}

	row := hwidRow(devices[0], time.Unix(0, 0).UTC())
	if len(row) != len(hwidColumns) {
		t.Fatalf("row has %d values, %d columns", len(row), len(hwidColumns))
	}
	if row[4] != "" {
		t.Errorf("device_model = %v, want empty for a null", row[4])
	}
}

// torrentPanel serves reports newest first, the panel's default order, one
// per minute going back from newest.
func torrentPanel(t *testing.T, total int, newest time.Time) (*httptest.Server, *int) {
	t.Helper()
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/node-plugins/torrent-blocker" {
			http.NotFound(w, r)
			return
		}
		requests++
		start, _ := strconv.Atoi(r.URL.Query().Get("start"))
		size, _ := strconv.Atoi(r.URL.Query().Get("size"))
		var records []string
		for i := start; i < min(start+size, total); i++ {
			created := newest.Add(-time.Duration(i) * time.Minute).Format(time.RFC3339Nano)
			records = append(records, fmt.Sprintf(`{"id":%d,"userId":42,"nodeId":7,
				"user":{"username":"alice"},"node":{"uuid":"u","name":"de-1","countryCode":"DE"},
				"report":{"actionReport":{"blocked":true,"ip":"203.0.113.9","blockDuration":3600,
					"willUnblockAt":%q,"userId":"42","processedAt":%q},
				"xrayReport":{"email":"42","level":0,"protocol":"bittorrent","network":"tcp",
					"source":"203.0.113.9:51413","destination":"tcp:198.51.100.20:6881",
					"routeTarget":null,"originalTarget":null,"inboundTag":"VLESS_REALITY",
					"inboundName":null,"inboundLocal":null,"outboundTag":"BLOCK","ts":1}},
				"createdAt":%q}`, total-i, created, created, created))
		}
		fmt.Fprintf(w, `{"response":{"records":[%s],"total":%d}}`, strings.Join(records, ","), total)
	}))
	return srv, &requests
}

func TestFetchTorrentReportsBackfillsEverythingOnFirstRun(t *testing.T) {
	newest := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	srv, _ := torrentPanel(t, torrentPageSize+10, newest)
	defer srv.Close()

	s, _ := newTestSyncer(srv.URL)
	reports, err := s.fetchTorrentReports(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != torrentPageSize+10 {
		t.Fatalf("got %d reports, want %d", len(reports), torrentPageSize+10)
	}

	row := torrentRow(reports[0], nil)
	if len(row) != len(torrentColumns) {
		t.Fatalf("row has %d values, %d columns", len(row), len(torrentColumns))
	}
	got := map[string]any{}
	for i, col := range torrentColumns {
		got[col] = row[i]
	}
	for col, want := range map[string]any{
		"ts":            newest,
		"user_id":       uint64(42),
		"node_id":       uint64(7),
		"ip":            "203.0.113.9",
		"blocked":       uint8(1),
		"block_seconds": uint32(3600),
		"destination":   "tcp:198.51.100.20:6881",
		"protocol":      "bittorrent",
		"inbound_tag":   "VLESS_REALITY",
		"outbound_tag":  "BLOCK",
	} {
		if got[col] != want {
			t.Errorf("%s = %v, want %v", col, got[col], want)
		}
	}
}

// With a cursor, paging stops at the first report older than the cursor minus
// the overlap, instead of walking the whole history every minute.
func TestFetchTorrentReportsStopsAtTheCursor(t *testing.T) {
	newest := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	srv, requests := torrentPanel(t, 5*torrentPageSize, newest)
	defer srv.Close()

	s, _ := newTestSyncer(srv.URL)
	since := newest.Add(-10 * time.Minute)
	reports, err := s.fetchTorrentReports(context.Background(), since)
	if err != nil {
		t.Fatal(err)
	}
	// Ten minutes of reports past the cursor plus the overlap, one a minute,
	// both ends inclusive.
	want := int((10*time.Minute+torrentOverlap)/time.Minute) + 1
	if len(reports) != want {
		t.Errorf("got %d reports, want %d", len(reports), want)
	}
	if *requests != 1 {
		t.Errorf("made %d requests, want 1", *requests)
	}
}

// HWID devices and torrent reports are optional: a token without their scope
// explains itself once at INFO, then stays at DEBUG.
func TestOptionalSourcesExplainAMissingScopeOnce(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	s, buf := newTestSyncer(srv.URL)
	for i := 0; i < 3; i++ {
		_, err := s.fetchTorrentReports(context.Background(), time.Time{})
		if !s.optionalUnavailable("torrent reports", "node-plugins:torrent-blocker-reports", err) {
			t.Fatalf("a 403 must count as unavailable, got %v", err)
		}
	}

	recs := logged(t, buf)
	if len(recs) != 3 {
		t.Fatalf("got %d log records, want 3: %v", len(recs), recs)
	}
	if recs[0]["level"] != "INFO" || recs[1]["level"] != "DEBUG" || recs[2]["level"] != "DEBUG" {
		t.Errorf("levels = %v, %v, %v; want INFO, DEBUG, DEBUG", recs[0]["level"], recs[1]["level"], recs[2]["level"])
	}
	if reason, _ := recs[0]["reason"].(string); !strings.Contains(reason, "node-plugins:torrent-blocker-reports") {
		t.Errorf("reason = %q, want it to name the scope", reason)
	}

	// A real failure is not swallowed.
	if s.optionalUnavailable("torrent reports", "x", &statusError{Code: http.StatusInternalServerError}) {
		t.Error("a 500 must not count as unavailable")
	}
	if s.optionalUnavailable("torrent reports", "x", fmt.Errorf("connection refused")) {
		t.Error("a transport error must not count as unavailable")
	}
}

// Every poll re-reads the overlap behind the cursor; reports this process
// already stored must not be written again, new ones must.
func TestTorrentReportsAreStoredOnce(t *testing.T) {
	newest := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	srv, _ := torrentPanel(t, 5, newest)
	defer srv.Close()

	s, _ := newTestSyncer(srv.URL)
	first, err := s.fetchTorrentReports(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.unseenTorrentReports(first); len(got) != 5 {
		t.Fatalf("first poll: %d unseen, want 5", len(got))
	}
	s.markTorrentReportsStored(first)
	if !s.torrentSince.Equal(newest) {
		t.Errorf("cursor = %v, want %v", s.torrentSince, newest)
	}

	again, err := s.fetchTorrentReports(context.Background(), s.torrentSince)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) == 0 {
		t.Fatal("the overlap was not re-read")
	}
	if got := s.unseenTorrentReports(again); len(got) != 0 {
		t.Errorf("second poll: %d unseen, want 0", len(got))
	}

	// A report the panel committed late, inside the overlap, still counts.
	late := again[0]
	late.ID, late.CreatedAt = 999, newest.Add(-30*time.Second).Format(time.RFC3339Nano)
	if got := s.unseenTorrentReports(append(again, late)); len(got) != 1 || got[0].ID != 999 {
		t.Errorf("late report: unseen = %v, want just id 999", got)
	}

	// Entries that fall behind the overlap are forgotten.
	s.markTorrentReportsStored([]torrentReport{{ID: 1000, CreatedAt: newest.Add(time.Hour).Format(time.RFC3339Nano)}})
	if len(s.torrentStored) != 1 {
		t.Errorf("remembered %d reports, want only the newest", len(s.torrentStored))
	}
}
