// Package e2e verifies the ClickHouse schema and every dashboard query against
// a real ClickHouse server.
//
// The test is opt-in:
//
//	CLICKHOUSE_TEST_ADDR=127.0.0.1:9000 go test ./internal/e2e/...
//
// It creates (and reuses) the database named by CLICKHOUSE_TEST_DB, default
// remnawave_e2e, so it never touches a production database unless you point it
// at one on purpose.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"remnanode-exporter/internal/consumer"
	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/model"
	"remnanode-exporter/internal/schema"
	"remnanode-exporter/internal/sink"
)

const dashboardDir = "../../dashboards"

func testDB() string {
	if v := os.Getenv("CLICKHOUSE_TEST_DB"); v != "" {
		return v
	}
	return "remnawave_e2e"
}

func connect(t *testing.T) (*sink.Writer, *slog.Logger) {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_TEST_ADDR")
	if addr == "" {
		t.Skip("set CLICKHOUSE_TEST_ADDR to run the ClickHouse end-to-end test")
	}
	opts := sink.Options{
		Addrs:    []string{addr},
		Database: testDB(),
		Username: envOr("CLICKHOUSE_TEST_USER", "default"),
		Password: os.Getenv("CLICKHOUSE_TEST_PASSWORD"),
	}
	ctx := context.Background()
	if err := sink.EnsureDatabase(ctx, opts); err != nil {
		t.Fatalf("ensure database: %v", err)
	}
	w, err := sink.Connect(ctx, opts)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := schema.Apply(ctx, w, log); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return w, log
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestSchemaAndPipeline pushes rows through the real decoders and checks that
// the rollup materialised views fill in.
func TestSchemaAndPipeline(t *testing.T) {
	w, log := connect(t)
	ctx := context.Background()
	seed(ctx, t, w, log)

	var usage uint64
	if err := w.Conn().QueryRow(ctx,
		fmt.Sprintf("SELECT sum(bytes) FROM %s.usage_5m", testDB())).Scan(&usage); err != nil {
		t.Fatalf("read usage_5m: %v", err)
	}
	if usage == 0 {
		t.Error("usage_5m rollup is empty; the materialised view did not fire")
	}

	var networks uint64
	if err := w.Conn().QueryRow(ctx, fmt.Sprintf(
		"SELECT uniqMerge(prefixes) FROM %s.user_conn_5m WHERE user_id = 1001", testDB())).Scan(&networks); err != nil {
		t.Fatalf("read user_conn_5m: %v", err)
	}
	if networks < 2 {
		t.Errorf("user 1001 should show at least 2 distinct networks, got %d", networks)
	}

	var scripted uint64
	if err := w.Conn().QueryRow(ctx, fmt.Sprintf(
		"SELECT sum(scripted) FROM %s.sub_req_5m", testDB())).Scan(&scripted); err != nil {
		t.Fatalf("read sub_req_5m: %v", err)
	}
	if scripted == 0 {
		t.Error("expected the curl subscription fetch to be counted as scripted")
	}
}

// TestAbuseViewRanksTheSharedAccountFirst checks the scoring actually works.
func TestAbuseViewRanksTheSharedAccountFirst(t *testing.T) {
	w, log := connect(t)
	ctx := context.Background()
	seed(ctx, t, w, log)

	rows, err := w.Conn().Query(ctx, fmt.Sprintf(`
SELECT user_id, score, countries, prefixes, hosting_ips
FROM %s.v_user_abuse(from = toDateTime(now() - 7200), to = toDateTime(now() + 600))
ORDER BY score DESC`, testDB()))
	if err != nil {
		t.Fatalf("query v_user_abuse: %v", err)
	}
	defer rows.Close()

	type row struct {
		userID                       uint64
		score                        float64
		countries, prefixes, hosting uint64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.userID, &r.score, &r.countries, &r.prefixes, &r.hosting); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("v_user_abuse returned nothing")
	}
	if got[0].userID != 1001 {
		t.Errorf("top scorer = %d, want 1001 (the shared account): %+v", got[0].userID, got)
	}
	if got[0].score <= 0 {
		t.Errorf("top score = %v, want > 0", got[0].score)
	}
}

// TestDashboardQueries executes every rawSql found in the provisioned
// dashboards, so a typo in a panel fails the build rather than the dashboard.
func TestDashboardQueries(t *testing.T) {
	w, log := connect(t)
	ctx := context.Background()
	seed(ctx, t, w, log)

	files, err := filepath.Glob(filepath.Join(dashboardDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no dashboards found in %s", dashboardDir)
	}

	total := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var dash struct {
			Title  string `json:"title"`
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					RawSQL string `json:"rawSql"`
				} `json:"targets"`
			} `json:"panels"`
			Templating struct {
				List []struct {
					Name string `json:"name"`
					// Query is an object for SQL-backed variables and a plain
					// string for the datasource picker, hence the deferred decode.
					Query json.RawMessage `json:"query"`
				} `json:"list"`
			} `json:"templating"`
		}
		if err := json.Unmarshal(raw, &dash); err != nil {
			t.Fatalf("%s: %v", file, err)
		}

		for _, v := range dash.Templating.List {
			var q struct {
				RawSQL string `json:"rawSql"`
			}
			if json.Unmarshal(v.Query, &q) != nil || q.RawSQL == "" {
				continue
			}
			total++
			runQuery(ctx, t, w, dash.Title+" / $"+v.Name, q.RawSQL)
		}
		for _, p := range dash.Panels {
			for _, tg := range p.Targets {
				if tg.RawSQL == "" {
					continue
				}
				total++
				runQuery(ctx, t, w, dash.Title+" / "+p.Title, tg.RawSQL)
			}
		}
	}
	t.Logf("executed %d dashboard queries", total)
}

func runQuery(ctx context.Context, t *testing.T, w *sink.Writer, name, rawSQL string) {
	t.Helper()
	query := expandMacros(rawSQL)
	rows, err := w.Conn().Query(ctx, query)
	if err != nil {
		t.Errorf("%s: %v\n--- query ---\n%s", name, err, query)
		return
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Errorf("%s: %v\n--- query ---\n%s", name, err, query)
	}
}

var (
	reTimeFilter   = regexp.MustCompile(`\$__timeFilter\(([^)]*)\)`)
	reTimeInterval = regexp.MustCompile(`\$__timeInterval\(([^)]*)\)`)
)

// expandMacros substitutes the Grafana ClickHouse datasource macros the same
// way the plugin does, so the queries can run outside Grafana.
func expandMacros(q string) string {
	const from = "toDateTime(now() - 86400)"
	const to = "toDateTime(now() + 600)"

	q = strings.ReplaceAll(q, "remnawave.", testDB()+".")
	q = reTimeInterval.ReplaceAllString(q, "toStartOfInterval(toDateTime($1), INTERVAL 300 SECOND)")
	q = reTimeFilter.ReplaceAllString(q, "$1 >= "+from+" AND $1 <= "+to)
	q = strings.ReplaceAll(q, "$__interval_s", "300")
	q = strings.ReplaceAll(q, "$__fromTime", from)
	q = strings.ReplaceAll(q, "$__toTime", to)
	// Grafana expands a multi-value variable to a comma separated list.
	q = strings.ReplaceAll(q, "${node:sqlstring}", "'node-1', 'node-2'")
	return q
}

// seed writes a small, deliberately shaped dataset:
//
//	user 1001 — one subscription, four networks in three countries, one of them
//	            a hosting ASN, plus scripted subscription fetches (the abuser)
//	user 1002 — a single network, one client (the normal subscriber)
func seed(ctx context.Context, t *testing.T, w *sink.Writer, log *slog.Logger) {
	t.Helper()
	geo := geoip.New(os.Getenv("GEOIP_CITY_DB"), os.Getenv("GEOIP_ASN_DB"), nil, log)
	t.Cleanup(geo.Close)

	now := time.Now().UTC().Truncate(5 * time.Minute)
	stamp := func(offset time.Duration) string {
		return now.Add(offset).Format(time.RFC3339Nano)
	}

	usage := consumer.UserUsageDecoder{}
	conns := consumer.NodeConnectionsDecoder{Geo: geo}
	subs := consumer.SubRequestDecoder{Geo: geo}

	insert := func(dec consumer.Decoder, fields model.Fields) {
		t.Helper()
		rows, err := dec.Decode(fields)
		if err != nil {
			t.Fatalf("decode %s: %v", dec.Stream(), err)
		}
		if err := w.Insert(ctx, dec.Table(), dec.Columns(), rows); err != nil {
			t.Fatalf("insert %s: %v", dec.Table(), err)
		}
	}

	for i := 0; i < 3; i++ {
		off := time.Duration(-i*5) * time.Minute
		insert(usage, model.Fields{
			"v": "1", "nodeId": "1", "ts": stamp(off),
			"records": "1001:53687091200;1002:1073741824",
		})
		insert(usage, model.Fields{
			"v": "1", "nodeId": "2", "ts": stamp(off),
			"records": "1001:10737418240",
		})

		insert(conns, model.Fields{
			"v": "1", "nodeId": "1", "ts": stamp(off),
			"users": fmt.Sprintf(`[
              {"userId":"1001","ips":[
                {"ip":"77.88.55.60","lastSeen":%q},
                {"ip":"8.8.8.8","lastSeen":%q},
                {"ip":"1.1.1.1","lastSeen":%q},
                {"ip":"2a02:6ea0:c700::1","lastSeen":%q}]},
              {"userId":"1002","ips":[{"ip":"77.88.55.70","lastSeen":%q}]}]`,
				stamp(off), stamp(off), stamp(off), stamp(off), stamp(off)),
		})
		insert(conns, model.Fields{
			"v": "1", "nodeId": "2", "ts": stamp(off),
			"users": fmt.Sprintf(`[{"userId":"1001","ips":[{"ip":"77.88.55.60","lastSeen":%q}]}]`, stamp(off)),
		})

		insert(subs, model.Fields{
			"v": "1", "userId": "1001", "requestAt": stamp(off),
			"requestIp": "8.8.8.8", "userAgent": "curl/8.5.0",
		})
		insert(subs, model.Fields{
			"v": "1", "userId": "1001", "requestAt": stamp(off + time.Minute),
			"requestIp": "1.1.1.1", "userAgent": "python-requests/2.31.0",
		})
		insert(subs, model.Fields{
			"v": "1", "userId": "1002", "requestAt": stamp(off),
			"requestIp": "77.88.55.70", "userAgent": "Happ/1.24.0",
		})
	}

	// Dimensions, so the name-resolving views return something readable.
	if err := w.Insert(ctx, "dim_users",
		[]string{"user_id", "username", "status", "tag", "traffic_limit_bytes", "hwid_device_limit", "expire_at", "updated_at"},
		[][]any{
			{uint64(1001), "sharer", "ACTIVE", "VIP", uint64(0), int64(2), now.Add(720 * time.Hour), now},
			{uint64(1002), "normal", "ACTIVE", "", uint64(0), int64(3), now.Add(720 * time.Hour), now},
		}); err != nil {
		t.Fatalf("insert dim_users: %v", err)
	}
	if err := w.Insert(ctx, "dim_nodes",
		[]string{"node_id", "uuid", "name", "country_code", "updated_at"},
		[][]any{
			{uint64(1), "11111111-1111-4111-8111-111111111111", "node-1", "DE", now},
			{uint64(2), "22222222-2222-4222-8222-222222222222", "node-2", "NL", now},
		}); err != nil {
		t.Fatalf("insert dim_nodes: %v", err)
	}
}
