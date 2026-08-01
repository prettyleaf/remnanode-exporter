// Package dict keeps ClickHouse dimension tables in sync with the panel, so
// dashboards can show usernames and node names instead of bare numeric ids.
//
// Users come from the public API (/api/users/stream exposes the same numeric
// id the streams carry). Node names are only available in the panel database:
// the REST API identifies nodes by uuid and never returns the bigint id that
// the export streams use.
package dict

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

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
	APIURL      string
	APIToken    string
	PostgresDSN string
	NodesSQL    string
	NodeNames   string
	Interval    time.Duration
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
	if s.opt.APIURL == "" || s.opt.APIToken == "" {
		return nil // dictionary disabled
	}

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
	url := s.opt.APIURL + "/api/users/stream?size=1000"
	if cursor != "" {
		url += "&cursor=" + cursor
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+s.opt.APIToken)
	req.Header.Set("Accept", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	var decoded usersStreamResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, "", err
	}
	next := ""
	if decoded.Response.HasMore && decoded.Response.NextCursor != nil {
		next = *decoded.Response.NextCursor
	}
	return decoded.Response.Users, next, nil
}

// syncNodes fills dim_nodes from the panel database, or from the static
// NODE_NAMES map when no DSN is configured.
func (s *Syncer) syncNodes(ctx context.Context) error {
	cols := []string{"node_id", "uuid", "name", "country_code", "updated_at"}
	now := time.Now().UTC()

	var rows [][]any
	switch {
	case s.opt.PostgresDSN != "":
		var err error
		rows, err = s.nodesFromPostgres(ctx, now)
		if err != nil {
			return err
		}
	case s.opt.NodeNames != "":
		rows = nodesFromStatic(s.opt.NodeNames, now)
	default:
		return nil
	}

	if len(rows) == 0 {
		return nil
	}
	if err := s.sink.Insert(ctx, TableNodes, cols, rows); err != nil {
		return err
	}
	metrics.DictSize.WithLabelValues(TableNodes).Set(float64(len(rows)))
	return nil
}

func (s *Syncer) nodesFromPostgres(ctx context.Context, now time.Time) ([][]any, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, s.opt.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("connect panel db: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	pgRows, err := conn.Query(ctx, s.opt.NodesSQL)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer pgRows.Close()

	var out [][]any
	for pgRows.Next() {
		var (
			id      int64
			uuid    string
			name    string
			country *string
		)
		if err := pgRows.Scan(&id, &uuid, &name, &country); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, []any{uint64(id), uuid, name, derefString(country), now})
	}
	return out, pgRows.Err()
}

// nodesFromStatic parses "1:Germany-1,2:NL-2".
func nodesFromStatic(spec string, now time.Time) [][]any {
	var out [][]any
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		idRaw, name, ok := strings.Cut(pair, ":")
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idRaw), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, []any{id, "", strings.TrimSpace(name), "", now})
	}
	return out
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
