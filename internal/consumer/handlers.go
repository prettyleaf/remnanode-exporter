package consumer

import (
	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/model"
	"remnanode-exporter/internal/ua"
)

// ClickHouse table names. They mirror the DDL in deploy/clickhouse/init.
const (
	TableUserUsage       = "user_usage"
	TableSubRequests     = "sub_requests"
	TableNodeConnections = "node_connections"
)

// UserUsageDecoder expands a traffic batch into one row per user.
type UserUsageDecoder struct{}

func (UserUsageDecoder) Stream() string { return model.StreamUserUsage }
func (UserUsageDecoder) Table() string  { return TableUserUsage }
func (UserUsageDecoder) Columns() []string {
	return []string{"ts", "node_id", "user_id", "bytes"}
}

func (UserUsageDecoder) Decode(f model.Fields) ([][]any, error) {
	msg, err := model.ParseUserUsage(f)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, 0, len(msg.Records))
	for _, r := range msg.Records {
		rows = append(rows, []any{msg.TS, msg.NodeID, r.UserID, r.Bytes})
	}
	return rows, nil
}

// SubRequestDecoder enriches a subscription fetch with geo and client data.
type SubRequestDecoder struct {
	Geo *geoip.Resolver
}

func (SubRequestDecoder) Stream() string { return model.StreamSubRequests }
func (SubRequestDecoder) Table() string  { return TableSubRequests }
func (SubRequestDecoder) Columns() []string {
	return []string{
		"ts", "user_id", "ip", "ip_prefix", "country", "city",
		"asn", "as_org", "is_hosting", "user_agent", "ua_family", "ua_kind",
		"srr_response_type", "srr_rule_name",
	}
}

func (d SubRequestDecoder) Decode(f model.Fields) ([][]any, error) {
	msg, err := model.ParseSubRequest(f)
	if err != nil {
		return nil, err
	}
	geo := d.Geo.Lookup(msg.IP)
	client := ua.Classify(msg.UserAgent)
	return [][]any{{
		msg.RequestAt,
		msg.UserID,
		msg.IP,
		geo.Prefix,
		geo.Country,
		geo.City,
		geo.ASN,
		geo.ASOrg,
		boolToUInt8(geo.IsHosting),
		msg.UserAgent,
		client.Family,
		string(client.Kind),
		msg.ResponseType,
		msg.RuleName,
	}}, nil
}

// NodeConnectionsDecoder flattens a snapshot into one row per user/IP pair.
type NodeConnectionsDecoder struct {
	Geo *geoip.Resolver
}

func (NodeConnectionsDecoder) Stream() string { return model.StreamNodeConnections }
func (NodeConnectionsDecoder) Table() string  { return TableNodeConnections }
func (NodeConnectionsDecoder) Columns() []string {
	return []string{
		"ts", "node_id", "user_id", "ip", "ip_prefix", "last_seen",
		"country", "city", "asn", "as_org", "is_hosting",
	}
}

func (d NodeConnectionsDecoder) Decode(f model.Fields) ([][]any, error) {
	msg, err := model.ParseNodeConnections(f)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, 0, len(msg.Users))
	for _, u := range msg.Users {
		for _, ip := range u.IPs {
			geo := d.Geo.Lookup(ip.IP)
			rows = append(rows, []any{
				msg.TS,
				msg.NodeID,
				u.UserID,
				ip.IP,
				geo.Prefix,
				ip.LastSeen,
				geo.Country,
				geo.City,
				geo.ASN,
				geo.ASOrg,
				boolToUInt8(geo.IsHosting),
			})
		}
	}
	return rows, nil
}

func boolToUInt8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
