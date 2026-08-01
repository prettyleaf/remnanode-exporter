package model

import (
	"testing"
	"time"
)

func TestParseUserUsage(t *testing.T) {
	msg, err := ParseUserUsage(Fields{
		"v":       "1",
		"nodeId":  "7",
		"ts":      "2026-07-16T11:59:30.000Z",
		"records": "42:1024;43:2048;44:0",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.NodeID != 7 {
		t.Errorf("NodeID = %d, want 7", msg.NodeID)
	}
	if !msg.TS.Equal(time.Date(2026, 7, 16, 11, 59, 30, 0, time.UTC)) {
		t.Errorf("TS = %v", msg.TS)
	}
	want := []UsageRecord{{42, 1024}, {43, 2048}, {44, 0}}
	if len(msg.Records) != len(want) {
		t.Fatalf("got %d records, want %d", len(msg.Records), len(want))
	}
	for i := range want {
		if msg.Records[i] != want[i] {
			t.Errorf("record %d = %+v, want %+v", i, msg.Records[i], want[i])
		}
	}
}

func TestParseUserUsageRejectsBadInput(t *testing.T) {
	cases := map[string]Fields{
		"wrong version":  {"v": "2", "nodeId": "1", "ts": "2026-07-16T11:59:30.000Z", "records": "1:2"},
		"missing nodeId": {"v": "1", "ts": "2026-07-16T11:59:30.000Z", "records": "1:2"},
		"bad ts":         {"v": "1", "nodeId": "1", "ts": "yesterday", "records": "1:2"},
		"bad records":    {"v": "1", "nodeId": "1", "ts": "2026-07-16T11:59:30.000Z", "records": "1-2"},
		"empty records":  {"v": "1", "nodeId": "1", "ts": "2026-07-16T11:59:30.000Z", "records": ""},
	}
	for name, f := range cases {
		if _, err := ParseUserUsage(f); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestParseSubRequestOptionalFields(t *testing.T) {
	msg, err := ParseSubRequest(Fields{
		"v":         "1",
		"userId":    "9001",
		"requestAt": "2026-07-16T11:59:30.500Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.UserID != 9001 || msg.IP != "" || msg.UserAgent != "" {
		t.Errorf("unexpected message %+v", msg)
	}

	msg, err = ParseSubRequest(Fields{
		"v":         "1",
		"userId":    "9001",
		"requestAt": "2026-07-16T11:59:30.500Z",
		"requestIp": "1.2.3.4",
		"userAgent": "Happ/1.0",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.IP != "1.2.3.4" || msg.UserAgent != "Happ/1.0" {
		t.Errorf("unexpected message %+v", msg)
	}
}

func TestParseNodeConnections(t *testing.T) {
	msg, err := ParseNodeConnections(Fields{
		"v":      "1",
		"nodeId": "3",
		"ts":     "2026-07-16T12:00:00.000Z",
		"users": `[{"userId":"42","ips":[{"ip":"1.2.3.4","lastSeen":"2026-07-16T11:59:30.000Z"},` +
			`{"ip":"2a02:6ea0::1","lastSeen":"2026-07-16T11:58:00.000Z"}]},{"userId":"43","ips":[]}]`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.NodeID != 3 || len(msg.Users) != 2 {
		t.Fatalf("unexpected message %+v", msg)
	}
	if msg.Users[0].UserID != 42 || len(msg.Users[0].IPs) != 2 {
		t.Errorf("unexpected first user %+v", msg.Users[0])
	}
	if msg.Users[0].IPs[0].IP != "1.2.3.4" {
		t.Errorf("unexpected ip %q", msg.Users[0].IPs[0].IP)
	}
	if len(msg.Users[1].IPs) != 0 {
		t.Errorf("expected the second user to have no addresses")
	}
}

func TestParseNodeConnectionsRejectsBadJSON(t *testing.T) {
	_, err := ParseNodeConnections(Fields{
		"v": "1", "nodeId": "3", "ts": "2026-07-16T12:00:00.000Z", "users": "{not json",
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}
