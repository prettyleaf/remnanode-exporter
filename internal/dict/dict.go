// Package dict keeps ClickHouse dimension tables in sync with the panel, so
// dashboards can show usernames and node names instead of bare numeric ids.
//
// Both dimensions come from the public API: /api/users/stream and /api/nodes
// expose the same numeric ids the export streams carry. The node id was added
// in Remnawave 3.1.0; against an older panel the nodes stay unnamed.
package dict

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
		return fmt.Errorf("GET %s: %s", url, resp.Status)
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
