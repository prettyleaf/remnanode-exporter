// Package model parses the raw Redis Stream payloads published by Remnawave 3.1
// when EXPORT_TO_STREAM_ENABLED=true.
//
// Schemas are taken verbatim from @remnawave/backend-contract@3.1.1
// (models/export-stream/export-stream.schema.ts):
//
//	ioraw:export:user_usage           RemnawaveUserUsageStreamMessageDto
//	ioraw:export:subscription_requests RemnawaveSubscriptionRequestStreamMessageDto
//	ioraw:export:node_connections      RemnawaveNodeConnectionsStreamMessageDto
package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Stream keys published by the panel.
const (
	StreamUserUsage       = "ioraw:export:user_usage"
	StreamSubRequests     = "ioraw:export:subscription_requests"
	StreamNodeConnections = "ioraw:export:node_connections"
)

// SchemaVersion is the only "v" value the contract currently emits.
const SchemaVersion = "1"

// UsageRecord is one "userId:totalBytes" pair of a user_usage message.
type UsageRecord struct {
	UserID uint64
	Bytes  uint64
}

// UserUsage is a decoded ioraw:export:user_usage message.
type UserUsage struct {
	TS      time.Time
	NodeID  uint64
	Records []UsageRecord
}

// SubRequest is a decoded ioraw:export:subscription_requests message.
type SubRequest struct {
	RequestAt time.Time
	UserID    uint64
	IP        string
	UserAgent string
	// ResponseType is what the Subscription Response Rules engine served:
	// a template name (XRAY_JSON, SINGBOX, …) or a refusal (BLOCK,
	// STATUS_CODE_404, STATUS_CODE_451, SOCKET_DROP). Empty on panels older
	// than 3.1.0.
	ResponseType string
	// RuleName is the response rule that matched, empty when none did.
	RuleName string
}

// ConnIP is one address a user was seen connecting from.
type ConnIP struct {
	IP       string
	LastSeen time.Time
}

// ConnUser groups the addresses of a single user inside a snapshot.
type ConnUser struct {
	UserID uint64
	IPs    []ConnIP
}

// NodeConnections is a decoded ioraw:export:node_connections message.
// The panel emits one snapshot per node roughly every 5 minutes.
type NodeConnections struct {
	TS     time.Time
	NodeID uint64
	Users  []ConnUser
}

// Fields is the flat field map of a single XREADGROUP entry.
type Fields map[string]any

func (f Fields) str(key string) (string, bool) {
	v, ok := f[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprint(v), true
	}
	return s, true
}

// firstStr returns the value of the first key that carries one.
func (f Fields) firstStr(keys ...string) string {
	for _, key := range keys {
		if s, ok := f.str(key); ok && s != "" {
			return s
		}
	}
	return ""
}

func (f Fields) required(key string) (string, error) {
	s, ok := f.str(key)
	if !ok || s == "" {
		return "", fmt.Errorf("missing field %q", key)
	}
	return s, nil
}

func (f Fields) checkVersion() error {
	v, err := f.required("v")
	if err != nil {
		return err
	}
	if v != SchemaVersion {
		return fmt.Errorf("unsupported schema version %q (want %q)", v, SchemaVersion)
	}
	return nil
}

func (f Fields) uint(key string) (uint64, error) {
	s, err := f.required(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", key, err)
	}
	return n, nil
}

func (f Fields) time(key string) (time.Time, error) {
	s, err := f.required(key)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("field %q: %w", key, err)
	}
	return t.UTC(), nil
}

// ParseUserUsage decodes a user_usage entry.
func ParseUserUsage(f Fields) (UserUsage, error) {
	var out UserUsage
	if err := f.checkVersion(); err != nil {
		return out, err
	}
	nodeID, err := f.uint("nodeId")
	if err != nil {
		return out, err
	}
	ts, err := f.time("ts")
	if err != nil {
		return out, err
	}
	raw, err := f.required("records")
	if err != nil {
		return out, err
	}
	recs, err := parseRecords(raw)
	if err != nil {
		return out, err
	}
	out.TS, out.NodeID, out.Records = ts, nodeID, recs
	return out, nil
}

// parseRecords splits the "userId:totalBytes;userId:totalBytes" packing.
func parseRecords(raw string) ([]UsageRecord, error) {
	pairs := strings.Split(raw, ";")
	out := make([]UsageRecord, 0, len(pairs))
	for _, pair := range pairs {
		if pair == "" {
			continue
		}
		userRaw, bytesRaw, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("malformed record %q", pair)
		}
		userID, err := strconv.ParseUint(userRaw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("record %q: bad userId: %w", pair, err)
		}
		total, err := strconv.ParseUint(bytesRaw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("record %q: bad totalBytes: %w", pair, err)
		}
		out = append(out, UsageRecord{UserID: userID, Bytes: total})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty records")
	}
	return out, nil
}

// ParseSubRequest decodes a subscription_requests entry.
// Everything but the version, the user and the timestamp is optional:
// requestIp and userAgent are omitted when unknown, and the SRR fields are
// absent altogether on panels older than 3.1.0.
func ParseSubRequest(f Fields) (SubRequest, error) {
	var out SubRequest
	if err := f.checkVersion(); err != nil {
		return out, err
	}
	userID, err := f.uint("userId")
	if err != nil {
		return out, err
	}
	at, err := f.time("requestAt")
	if err != nil {
		return out, err
	}
	ip, _ := f.str("requestIp")
	ua, _ := f.str("userAgent")
	rule, _ := f.str("srrRuleName")
	out = SubRequest{
		RequestAt: at,
		UserID:    userID,
		IP:        ip,
		UserAgent: ua,
		// The contract calls this srrResponseType, but the panel writes
		// ssrResponseType — a transposition in the queue processor that is
		// still there in backend 3.1.0. Prefer what actually lands on the
		// wire and accept the contract spelling once it gets fixed.
		ResponseType: f.firstStr("ssrResponseType", "srrResponseType"),
		RuleName:     rule,
	}
	return out, nil
}

type connUserJSON struct {
	UserID string `json:"userId"`
	IPs    []struct {
		IP       string `json:"ip"`
		LastSeen string `json:"lastSeen"`
	} `json:"ips"`
}

// ParseNodeConnections decodes a node_connections snapshot.
func ParseNodeConnections(f Fields) (NodeConnections, error) {
	var out NodeConnections
	if err := f.checkVersion(); err != nil {
		return out, err
	}
	nodeID, err := f.uint("nodeId")
	if err != nil {
		return out, err
	}
	ts, err := f.time("ts")
	if err != nil {
		return out, err
	}
	raw, err := f.required("users")
	if err != nil {
		return out, err
	}
	var decoded []connUserJSON
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return out, fmt.Errorf("field \"users\": %w", err)
	}
	users := make([]ConnUser, 0, len(decoded))
	for _, u := range decoded {
		userID, err := strconv.ParseUint(u.UserID, 10, 64)
		if err != nil {
			return out, fmt.Errorf("users[].userId %q: %w", u.UserID, err)
		}
		ips := make([]ConnIP, 0, len(u.IPs))
		for _, ip := range u.IPs {
			seen, err := time.Parse(time.RFC3339Nano, ip.LastSeen)
			if err != nil {
				return out, fmt.Errorf("users[].ips[].lastSeen %q: %w", ip.LastSeen, err)
			}
			ips = append(ips, ConnIP{IP: ip.IP, LastSeen: seen.UTC()})
		}
		users = append(users, ConnUser{UserID: userID, IPs: ips})
	}
	out.TS, out.NodeID, out.Users = ts, nodeID, users
	return out, nil
}
