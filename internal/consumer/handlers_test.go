package consumer

import (
	"io"
	"log/slog"
	"testing"

	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/model"
)

func testGeo() *geoip.Resolver {
	return geoip.New("", "", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestUserUsageDecoderRowShape(t *testing.T) {
	d := UserUsageDecoder{}
	rows, err := d.Decode(model.Fields{
		"v": "1", "nodeId": "2", "ts": "2026-07-16T11:59:30.000Z", "records": "10:100;11:200",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, row := range rows {
		if len(row) != len(d.Columns()) {
			t.Fatalf("row has %d values, columns %d", len(row), len(d.Columns()))
		}
	}
	if rows[0][1] != uint64(2) || rows[0][2] != uint64(10) || rows[0][3] != uint64(100) {
		t.Errorf("unexpected first row: %v", rows[0])
	}
}

func TestSubRequestDecoderRowShape(t *testing.T) {
	d := SubRequestDecoder{Geo: testGeo()}
	rows, err := d.Decode(model.Fields{
		"v": "1", "userId": "77", "requestAt": "2026-07-16T11:59:30.000Z",
		"requestIp": "1.2.3.4", "userAgent": "curl/8.5.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0]) != len(d.Columns()) {
		t.Fatalf("unexpected rows: %v", rows)
	}
	if rows[0][3] != "1.2.3.0/24" {
		t.Errorf("ip_prefix = %v, want 1.2.3.0/24", rows[0][3])
	}
	if rows[0][11] != "script" {
		t.Errorf("ua_kind = %v, want script", rows[0][11])
	}
}

func TestNodeConnectionsDecoderFlattensIPs(t *testing.T) {
	d := NodeConnectionsDecoder{Geo: testGeo()}
	rows, err := d.Decode(model.Fields{
		"v": "1", "nodeId": "5", "ts": "2026-07-16T12:00:00.000Z",
		"users": `[{"userId":"1","ips":[{"ip":"1.2.3.4","lastSeen":"2026-07-16T11:59:00.000Z"},` +
			`{"ip":"5.6.7.8","lastSeen":"2026-07-16T11:59:10.000Z"}]},` +
			`{"userId":"2","ips":[{"ip":"9.9.9.9","lastSeen":"2026-07-16T11:59:20.000Z"}]}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3 (one per user/IP pair)", len(rows))
	}
	for _, row := range rows {
		if len(row) != len(d.Columns()) {
			t.Fatalf("row has %d values, columns %d", len(row), len(d.Columns()))
		}
	}
}

// A user with no addresses in the snapshot must not produce a row.
func TestNodeConnectionsDecoderSkipsEmptyUsers(t *testing.T) {
	d := NodeConnectionsDecoder{Geo: testGeo()}
	rows, err := d.Decode(model.Fields{
		"v": "1", "nodeId": "5", "ts": "2026-07-16T12:00:00.000Z",
		"users": `[{"userId":"1","ips":[]}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("got %d rows, want 0", len(rows))
	}
}
