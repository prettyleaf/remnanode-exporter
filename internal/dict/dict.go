// Package dict keeps ClickHouse dimension tables in sync with the panel, so
// dashboards can show usernames and node names instead of bare numeric ids.
//
// Both dimensions come from the public API: /api/users/stream and /api/nodes
// expose the same numeric ids the export streams carry. The node id was added
// in Remnawave 3.1.0; against an older panel the nodes stay unnamed.
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

	"remnanode-exporter/internal/metrics"
	"remnanode-exporter/internal/sink"
)

// TableUsers and TableNodes are the ClickHouse dimension tables.
const (
	TableUsers = "dim_users"
	TableNodes = "dim_nodes"
)

// Options configures the syncer.
type Options struct {
	APIURL   string
	APIToken string
	Interval time.Duration
}

// Syncer refreshes the dimension tables on an interval.
type Syncer struct {
	opt  Options
	sink *sink.Writer
	http *http.Client
	log  *slog.Logger
}

// New builds a Syncer.
func New(opt Options, w *sink.Writer, log *slog.Logger) *Syncer {
	return &Syncer{
		opt:  opt,
		sink: w,
		http: &http.Client{Timeout: 30 * time.Second},
		log:  log.With("component", "dict"),
	}
}

// Run syncs immediately and then every Interval until ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	s.checkVersion(ctx)
	s.checkConfiguration(ctx)
	s.syncAll(ctx)

	t := time.NewTicker(s.opt.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.syncAll(ctx)
		}
	}
}

func (s *Syncer) syncAll(ctx context.Context) {
	if s.opt.APIURL == "" || s.opt.APIToken == "" {
		return // no panel credentials: dashboards fall back to numeric ids
	}
	if err := s.syncUsers(ctx); err != nil {
		metrics.DictErrors.WithLabelValues(TableUsers).Inc()
		s.log.Warn("user dictionary refresh failed", "err", err)
	}
	if err := s.syncNodes(ctx); err != nil {
		metrics.DictErrors.WithLabelValues(TableNodes).Inc()
		s.log.Warn("node dictionary refresh failed", "err", err)
	}
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
	if s.opt.APIURL == "" || s.opt.APIToken == "" {
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
	if s.opt.APIURL == "" || s.opt.APIToken == "" {
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

type streamUser struct {
	ID                int64   `json:"id"`
	Username          string  `json:"username"`
	Status            string  `json:"status"`
	Tag               *string `json:"tag"`
	TrafficLimitBytes float64 `json:"trafficLimitBytes"`
	HwidDeviceLimit   *int64  `json:"hwidDeviceLimit"`
	ExpireAt          string  `json:"expireAt"`
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
	const cols = "user_id, username, status, tag, traffic_limit_bytes, hwid_device_limit, expire_at, updated_at"
	now := time.Now().UTC()
	var rows [][]any
	cursor := ""

	for page := 0; ; page++ {
		batch, next, err := s.fetchUsersPage(ctx, cursor)
		if err != nil {
			return err
		}
		for _, u := range batch {
			// ClickHouse DateTime64 starts at 1900; use the epoch as "unset".
			expire := time.Unix(0, 0).UTC()
			if t, err := time.Parse(time.RFC3339Nano, u.ExpireAt); err == nil {
				expire = t.UTC()
			}
			rows = append(rows, []any{
				uint64(u.ID),
				u.Username,
				u.Status,
				derefString(u.Tag),
				uint64(max(u.TrafficLimitBytes, 0)),
				derefInt(u.HwidDeviceLimit),
				expire,
				now,
			})
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
	if err := s.sink.Insert(ctx, TableUsers, strings.Split(cols, ", "), rows); err != nil {
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
	ID          int64  `json:"id"`
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	CountryCode string `json:"countryCode"`
}

type nodesResponse struct {
	Response []apiNode `json:"response"`
}

// syncNodes rewrites dim_nodes from /api/nodes.
func (s *Syncer) syncNodes(ctx context.Context) error {
	cols := []string{"node_id", "uuid", "name", "country_code", "updated_at"}
	now := time.Now().UTC()

	var decoded nodesResponse
	if err := s.getJSON(ctx, "/api/nodes", &decoded); err != nil {
		return err
	}

	rows := make([][]any, 0, len(decoded.Response))
	for _, n := range decoded.Response {
		// Panels older than 3.1.0 do not expose the bigint id the export
		// streams carry, and it decodes as zero. Such a row would only
		// mislabel whichever node really is id 0.
		if n.ID <= 0 {
			continue
		}
		rows = append(rows, []any{uint64(n.ID), n.UUID, n.Name, n.CountryCode, now})
	}

	if len(rows) == 0 {
		if len(decoded.Response) > 0 {
			return fmt.Errorf("none of the %d nodes carry a numeric id: node names need Remnawave 3.1.0 or newer",
				len(decoded.Response))
		}
		return nil
	}
	if err := s.sink.Insert(ctx, TableNodes, cols, rows); err != nil {
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
