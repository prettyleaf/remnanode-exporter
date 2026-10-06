// Package dict keeps ClickHouse in sync with what only the panel API knows.
//
// Dimensions, so dashboards can show usernames and node names instead of bare
// numeric ids: /api/users/stream and /api/nodes expose the same numeric ids
// the export streams carry. The node id was added in Remnawave 3.1.0; against
// an older panel the nodes stay unnamed. The node refresh also feeds the set
// of the panel's own addresses the connection decoder flags as infra.
//
// Two optional sources ride on the same token: every HWID device
// (/api/hwid/devices) and the Torrent Blocker plugin's reports
// (/api/node-plugins/torrent-blocker). A token without their scope simply
// leaves them empty.
//
// The same client also runs two one-shot preflights at startup:
// /api/system/metadata (Remnawave 3.0.0 and newer) reports which panel version
// is on the other end, and /api/system/configuration (3.2.0 and newer) reports
// how that panel is configured to export.
package dict

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/metrics"
	"remnanode-exporter/internal/sink"
)

// ClickHouse tables this package writes.
const (
	TableUsers          = "dim_users"
	TableNodes          = "dim_nodes"
	TableHwidDevices    = "dim_hwid_devices"
	TableTorrentReports = "torrent_reports"
)

// Options configures the syncer.
type Options struct {
	APIURL   string
	APIToken string
	// Interval paces the users, nodes and HWID devices refresh.
	Interval time.Duration
	// TorrentInterval paces the Torrent Blocker report poll.
	TorrentInterval time.Duration
	// Geo enriches the client address of every torrent report.
	Geo *geoip.Resolver
}

// Syncer refreshes the dimension tables on an interval.
type Syncer struct {
	opt   Options
	sink  *sink.Writer
	http  *http.Client
	log   *slog.Logger
	infra *IPSet

	// torrentSince is the newest report already stored, the poll cursor, and
	// torrentStored the reports in the overlap behind it this process wrote.
	torrentSince  time.Time
	torrentResume bool
	torrentStored map[torrentKey]time.Time
	// unavailable remembers which optional sources were already reported as
	// missing, so the explanation is logged once and not every refresh.
	unavailable map[string]bool
}

// New builds a Syncer.
func New(opt Options, w *sink.Writer, log *slog.Logger) *Syncer {
	return &Syncer{
		opt:           opt,
		sink:          w,
		http:          &http.Client{Timeout: 30 * time.Second},
		log:           log.With("component", "dict"),
		infra:         &IPSet{},
		torrentStored: map[torrentKey]time.Time{},
		unavailable:   map[string]bool{},
	}
}

// Infra is the set of the panel's own node addresses, refreshed with the nodes.
func (s *Syncer) Infra() *IPSet { return s.infra }

// Run syncs immediately and then on its intervals until ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	s.checkVersion(ctx)
	s.checkConfiguration(ctx)
	s.syncAll(ctx)
	s.pollTorrentReports(ctx)

	t := time.NewTicker(s.opt.Interval)
	defer t.Stop()
	torrents := time.NewTicker(s.opt.TorrentInterval)
	defer torrents.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.syncAll(ctx)
		case <-torrents.C:
			s.pollTorrentReports(ctx)
		}
	}
}

func (s *Syncer) hasCredentials() bool {
	return s.opt.APIURL != "" && s.opt.APIToken != ""
}

func (s *Syncer) syncAll(ctx context.Context) {
	if !s.hasCredentials() {
		return // no panel credentials: dashboards fall back to numeric ids
	}
	// Nodes first: it is one request, and it fills the own-address set the
	// connection decoder is already consulting, while the users can take
	// dozens of pages on a large panel.
	if err := s.syncNodes(ctx); err != nil {
		metrics.DictErrors.WithLabelValues(TableNodes).Inc()
		s.log.Warn("node dictionary refresh failed", "err", err)
	}
	if err := s.syncUsers(ctx); err != nil {
		metrics.DictErrors.WithLabelValues(TableUsers).Inc()
		s.log.Warn("user dictionary refresh failed", "err", err)
	}
	if err := s.syncHwidDevices(ctx); err != nil && !s.optionalUnavailable("HWID devices", "hwid-user-devices:list", err) {
		metrics.DictErrors.WithLabelValues(TableHwidDevices).Inc()
		s.log.Warn("HWID device refresh failed", "err", err)
	}
}

// optionalUnavailable reports whether err only means that an optional source
// is out of this token's or this panel's reach. The first time it says so at
// INFO, because the source stays empty until the token gains the scope, and
// at DEBUG after that.
func (s *Syncer) optionalUnavailable(source, scope string, err error) bool {
	var se *statusError
	if !errors.As(err, &se) {
		return false
	}
	var reason string
	switch se.Code {
	case http.StatusUnauthorized, http.StatusForbidden:
		reason = "the token lacks the " + scope + " scope"
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		reason = "the panel has no such endpoint"
	default:
		return false
	}
	if s.unavailable[source] {
		s.log.Debug(source+" skipped", "reason", reason)
	} else {
		s.unavailable[source] = true
		s.log.Info(source+" skipped", "reason", reason)
	}
	return true
}

// minPanelVersion is the oldest panel this exporter fully understands: 3.1.0
// is where /api/nodes started returning the numeric node id, and where the
// response-rule fields appeared in the subscription_requests stream.
var minPanelVersion = panelVersion{3, 1, 0}

// panelVersion is a major.minor.patch triple, which is all that is needed to
// tell two Remnawave releases apart.
type panelVersion struct{ major, minor, patch int }

func (v panelVersion) less(o panelVersion) bool {
	switch {
	case v.major != o.major:
		return v.major < o.major
	case v.minor != o.minor:
		return v.minor < o.minor
	default:
		return v.patch < o.patch
	}
}

// parsePanelVersion reads "3.3.2". Anything the panel appends to the patch
// number (a "-dev" tag on an unreleased build) is cut off rather than
// rejected: the three numbers are the whole comparison.
func parsePanelVersion(s string) (panelVersion, bool) {
	parts := strings.SplitN(strings.TrimPrefix(strings.TrimSpace(s), "v"), ".", 3)
	if len(parts) != 3 {
		return panelVersion{}, false
	}
	var out [3]int
	for i, part := range parts {
		n, ok := leadingInt(part)
		if !ok {
			return panelVersion{}, false
		}
		out[i] = n
	}
	return panelVersion{out[0], out[1], out[2]}, true
}

// leadingInt reads the digits that start s, ignoring whatever follows them.
func leadingInt(s string) (int, bool) {
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(s[:end])
	if err != nil {
		return 0, false
	}
	return n, true
}

// metadataResponse is the part of /api/system/metadata the exporter reads.
type metadataResponse struct {
	Response struct {
		Version string `json:"version"`
	} `json:"response"`
}

// checkVersion reports which panel the exporter is talking to and warns when
// that panel predates the data this exporter is built on. Like the
// configuration preflight it is advisory and never blocks startup.
func (s *Syncer) checkVersion(ctx context.Context) {
	if !s.hasCredentials() {
		return // no panel credentials: nothing to ask
	}

	var decoded metadataResponse
	if err := s.getJSON(ctx, "/api/system/metadata", &decoded); err != nil {
		s.logPreflightSkip("/api/system/metadata", "3.0.0", "system:metadata", err)
		return
	}

	raw := decoded.Response.Version
	v, ok := parsePanelVersion(raw)
	if !ok {
		s.log.Debug("panel reported an unreadable version", "version", raw)
		return
	}
	if v.less(minPanelVersion) {
		s.log.Warn("panel is older than 3.1.0: nodes stay unnamed and subscription "+
			"requests carry no response-rule fields", "panel_version", raw)
		return
	}
	s.log.Info("panel version", "panel_version", raw)
}

// logPreflightSkip explains at DEBUG why an advisory preflight got no answer.
// None of these are faults: the endpoint may postdate the panel, or the token
// may simply not carry the scope that endpoint sits behind.
func (s *Syncer) logPreflightSkip(path, since, scope string, err error) {
	code := 0
	var se *statusError
	if errors.As(err, &se) {
		code = se.Code
	}
	switch code {
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		s.log.Debug("panel has no " + path + ", it needs Remnawave " + since + " or newer")
	case http.StatusUnauthorized, http.StatusForbidden:
		s.log.Debug("preflight skipped: the token lacks the "+scope+" scope", "endpoint", path)
	default:
		s.log.Debug("preflight failed", "endpoint", path, "err", err)
	}
}

// configurationResponse is the part of /api/system/configuration that decides
// whether this exporter sees anything at all.
type configurationResponse struct {
	Response struct {
		Service struct {
			DisableUserUsageRecords bool `json:"disableUserUsageRecords"`
			DisableSrhRecords       bool `json:"disableSrhRecords"`
			ExportToRedisStream     bool `json:"exportToRedisStream"`
		} `json:"service"`
		Misc struct {
			UserUsageIgnoreBelowBytes float64 `json:"userUsageIgnoreBelowBytes"`
		} `json:"misc"`
	} `json:"response"`
}

// checkConfiguration reports once, at startup, how the panel is configured to
// export. Everything here is advisory and never blocks startup: the endpoint
// only exists on Remnawave 3.2.0 and newer, and only for tokens carrying the
// system:configuration scope, so both of those are ordinary setups rather than
// faults.
func (s *Syncer) checkConfiguration(ctx context.Context) {
	if !s.hasCredentials() {
		return // no panel credentials: nothing to ask
	}

	var decoded configurationResponse
	if err := s.getJSON(ctx, "/api/system/configuration", &decoded); err != nil {
		s.logPreflightSkip("/api/system/configuration", "3.2.0", "system:configuration", err)
		return
	}

	svc := decoded.Response.Service
	if !svc.ExportToRedisStream {
		s.log.Warn("panel is not publishing the export streams, so no data will ever arrive: " +
			"set EXPORT_TO_STREAM_ENABLED=true in the panel environment and restart it")
		return
	}

	// Neither disable* flag stops the streams; they only skip the panel's own
	// bookkeeping tables. userUsageIgnoreBelowBytes is the one that drops
	// traffic upstream of the stream, so it is what explains small deltas that
	// never reach ClickHouse.
	s.log.Info("panel export configuration",
		"user_usage_ignore_below_bytes", uint64(max(decoded.Response.Misc.UserUsageIgnoreBelowBytes, 0)),
		"user_usage_records_disabled", svc.DisableUserUsageRecords,
		"srh_records_disabled", svc.DisableSrhRecords)
}

// streamUser is the part of a /api/users/stream user the exporter keeps. The
// subscription secrets (shortUuid, subscriptionUrl, the proxy credentials)
// are deliberately left out: nothing here needs them.
type streamUser struct {
	ID                   int64   `json:"id"`
	Username             string  `json:"username"`
	Status               string  `json:"status"`
	Tag                  *string `json:"tag"`
	TrafficLimitBytes    float64 `json:"trafficLimitBytes"`
	TrafficLimitStrategy string  `json:"trafficLimitStrategy"`
	HwidDeviceLimit      *int64  `json:"hwidDeviceLimit"`
	TelegramID           *int64  `json:"telegramId"`
	ExpireAt             string  `json:"expireAt"`
	CreatedAt            string  `json:"createdAt"`
	SubRevokedAt         *string `json:"subRevokedAt"`
	ActiveInternalSquads []struct {
		Name string `json:"name"`
	} `json:"activeInternalSquads"`
	UserTraffic struct {
		UsedTrafficBytes         float64 `json:"usedTrafficBytes"`
		LifetimeUsedTrafficBytes float64 `json:"lifetimeUsedTrafficBytes"`
		OnlineAt                 *string `json:"onlineAt"`
		FirstConnectedAt         *string `json:"firstConnectedAt"`
		LastConnectedNodeUUID    *string `json:"lastConnectedNodeUuid"`
	} `json:"userTraffic"`
}

// userColumns matches userRow positionally.
var userColumns = []string{
	"user_id", "username", "status", "tag", "traffic_limit_bytes", "hwid_device_limit", "expire_at",
	"traffic_limit_strategy", "used_traffic_bytes", "lifetime_used_traffic_bytes", "telegram_id",
	"internal_squads", "created_at", "online_at", "first_connected_at", "sub_revoked_at",
	"last_connected_node_uuid", "updated_at",
}

func userRow(u streamUser, now time.Time) []any {
	squads := make([]string, 0, len(u.ActiveInternalSquads))
	for _, sq := range u.ActiveInternalSquads {
		squads = append(squads, sq.Name)
	}
	return []any{
		uint64(u.ID),
		u.Username,
		u.Status,
		derefString(u.Tag),
		uint64(max(u.TrafficLimitBytes, 0)),
		derefInt(u.HwidDeviceLimit),
		parseTime(u.ExpireAt),
		u.TrafficLimitStrategy,
		uint64(max(u.UserTraffic.UsedTrafficBytes, 0)),
		uint64(max(u.UserTraffic.LifetimeUsedTrafficBytes, 0)),
		derefInt(u.TelegramID),
		squads,
		parseTime(u.CreatedAt),
		parseTime(derefString(u.UserTraffic.OnlineAt)),
		parseTime(derefString(u.UserTraffic.FirstConnectedAt)),
		parseTime(derefString(u.SubRevokedAt)),
		derefString(u.UserTraffic.LastConnectedNodeUUID),
		now,
	}
}

// parseTime reads a panel timestamp. ClickHouse DateTime64 starts at 1900, so
// a missing or unreadable one becomes the epoch, which the schema treats as
// unset.
func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC()
	}
	return time.Unix(0, 0).UTC()
}

type usersStreamResponse struct {
	Response struct {
		Users      []streamUser `json:"users"`
		NextCursor *string      `json:"nextCursor"`
		HasMore    bool         `json:"hasMore"`
	} `json:"response"`
}

// syncUsers pages through /api/users/stream and rewrites dim_users.
func (s *Syncer) syncUsers(ctx context.Context) error {
	now := time.Now().UTC()
	var rows [][]any
	cursor := ""

	for page := 0; ; page++ {
		batch, next, err := s.fetchUsersPage(ctx, cursor)
		if err != nil {
			return err
		}
		for _, u := range batch {
			rows = append(rows, userRow(u, now))
		}
		if next == "" {
			break
		}
		cursor = next
		if page > 10000 { // hard stop against a server that never ends the cursor
			return fmt.Errorf("users stream did not terminate")
		}
	}

	if len(rows) == 0 {
		return nil
	}
	if err := s.sink.Insert(ctx, TableUsers, userColumns, rows); err != nil {
		return err
	}
	metrics.DictSize.WithLabelValues(TableUsers).Set(float64(len(rows)))
	s.log.Debug("user dictionary refreshed", "users", len(rows))
	return nil
}

func (s *Syncer) fetchUsersPage(ctx context.Context, cursor string) ([]streamUser, string, error) {
	path := "/api/users/stream?size=1000"
	if cursor != "" {
		path += "&cursor=" + cursor
	}
	var decoded usersStreamResponse
	if err := s.getJSON(ctx, path, &decoded); err != nil {
		return nil, "", err
	}
	next := ""
	if decoded.Response.HasMore && decoded.Response.NextCursor != nil {
		next = *decoded.Response.NextCursor
	}
	return decoded.Response.Users, next, nil
}

// statusError is a non-200 answer from the panel. It carries the code so
// callers can tell "this panel is too old" apart from a real failure.
type statusError struct {
	URL    string
	Status string
	Code   int
}

func (e *statusError) Error() string { return fmt.Sprintf("GET %s: %s", e.URL, e.Status) }

// getJSON performs an authenticated GET against the panel API and decodes the
// body into out.
func (s *Syncer) getJSON(ctx context.Context, path string, out any) error {
	url := s.opt.APIURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.opt.APIToken)
	req.Header.Set("Accept", "application/json")
	// Outside development mode the panel destroys the socket of any request
	// that did not come through a TLS-terminating reverse proxy, which reaches
	// a direct http://remnawave:3000 caller as a bare EOF. Over plain HTTP we
	// are talking to the panel's own port, so stand in for that proxy; over
	// HTTPS a real one is in front and sets these itself.
	if req.URL.Scheme == "http" {
		req.Header.Set("X-Forwarded-For", "127.0.0.1")
		req.Header.Set("X-Forwarded-Proto", "https")
	}

	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &statusError{URL: url, Status: resp.Status, Code: resp.StatusCode}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type apiNode struct {
	ID          int64    `json:"id"`
	UUID        string   `json:"uuid"`
	Name        string   `json:"name"`
	CountryCode string   `json:"countryCode"`
	Address     string   `json:"address"`
	Tags        []string `json:"tags"`
	Provider    *struct {
		Name string `json:"name"`
	} `json:"provider"`
	// IPs is the node's own address list, Remnawave 3.3.0 and newer.
	IPs []struct {
		IP     string `json:"ip"`
		Status string `json:"status"`
	} `json:"ips"`
}

type nodesResponse struct {
	Response []apiNode `json:"response"`
}

// nodeColumns matches nodeRow positionally.
var nodeColumns = []string{"node_id", "uuid", "name", "country_code", "address", "provider", "tags", "updated_at"}

func nodeRow(n apiNode, now time.Time) []any {
	provider := ""
	if n.Provider != nil {
		provider = n.Provider.Name
	}
	tags := n.Tags
	if tags == nil {
		tags = []string{}
	}
	return []any{uint64(n.ID), n.UUID, n.Name, n.CountryCode, n.Address, provider, tags, now}
}

// syncNodes rewrites dim_nodes from /api/nodes and refreshes the set of the
// panel's own addresses.
func (s *Syncer) syncNodes(ctx context.Context) error {
	now := time.Now().UTC()

	var decoded nodesResponse
	if err := s.getJSON(ctx, "/api/nodes", &decoded); err != nil {
		return err
	}

	s.infra.replace(nodeAddresses(ctx, decoded.Response, lookupHost))
	metrics.DictSize.WithLabelValues("infra_ips").Set(float64(s.infra.Len()))

	rows := make([][]any, 0, len(decoded.Response))
	for _, n := range decoded.Response {
		// Panels older than 3.1.0 do not expose the bigint id the export
		// streams carry, and it decodes as zero. Such a row would only
		// mislabel whichever node really is id 0.
		if n.ID <= 0 {
			continue
		}
		rows = append(rows, nodeRow(n, now))
	}

	if len(rows) == 0 {
		if len(decoded.Response) > 0 {
			return fmt.Errorf("none of the %d nodes carry a numeric id: node names need Remnawave 3.1.0 or newer",
				len(decoded.Response))
		}
		return nil
	}
	if err := s.sink.Insert(ctx, TableNodes, nodeColumns, rows); err != nil {
		return err
	}
	metrics.DictSize.WithLabelValues(TableNodes).Set(float64(len(rows)))
	s.log.Debug("node dictionary refreshed", "nodes", len(rows))
	return nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
