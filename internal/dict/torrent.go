package dict

import (
	"context"
	"fmt"
	"time"

	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/metrics"
)

const (
	// torrentPageSize is the most the report list hands out per request.
	torrentPageSize = 1000
	// torrentMaxPages bounds one poll, which only matters for the first one
	// against a panel that has been collecting reports for a long time.
	torrentMaxPages = 100
	// torrentOverlap is re-read behind the cursor on every poll. Reports are
	// listed newest id first, and an id is taken before its row commits, so a
	// slow transaction can land a report just behind one already seen.
	torrentOverlap = 2 * time.Minute
)

// torrentReport is the part of a Torrent Blocker report the exporter keeps.
type torrentReport struct {
	ID     int64 `json:"id"`
	UserID int64 `json:"userId"`
	NodeID int64 `json:"nodeId"`
	Report struct {
		ActionReport struct {
			Blocked       bool    `json:"blocked"`
			IP            string  `json:"ip"`
			BlockDuration float64 `json:"blockDuration"`
		} `json:"actionReport"`
		XrayReport struct {
			Protocol    *string `json:"protocol"`
			Network     string  `json:"network"`
			Destination string  `json:"destination"`
			InboundTag  *string `json:"inboundTag"`
			OutboundTag *string `json:"outboundTag"`
		} `json:"xrayReport"`
	} `json:"report"`
	CreatedAt string `json:"createdAt"`
}

type torrentReportsResponse struct {
	Response struct {
		Records []torrentReport `json:"records"`
		Total   int             `json:"total"`
	} `json:"response"`
}

// torrentColumns matches torrentRow positionally.
var torrentColumns = []string{
	"ts", "report_id", "node_id", "user_id", "ip", "ip_prefix", "country", "city", "asn", "as_org",
	"is_hosting", "blocked", "block_seconds", "destination", "network", "protocol", "inbound_tag",
	"outbound_tag",
}

func torrentRow(r torrentReport, geo *geoip.Resolver) []any {
	act, xr := r.Report.ActionReport, r.Report.XrayReport
	var info geoip.Info
	if geo != nil {
		info = geo.Lookup(act.IP)
	}
	return []any{
		parseTime(r.CreatedAt),
		uint64(r.ID),
		uint64(r.NodeID),
		uint64(r.UserID),
		act.IP,
		info.Prefix,
		info.Country,
		info.City,
		info.ASN,
		info.ASOrg,
		boolToUInt8(info.IsHosting),
		boolToUInt8(act.Blocked),
		uint32(max(act.BlockDuration, 0)),
		xr.Destination,
		xr.Network,
		derefString(xr.Protocol),
		derefString(xr.InboundTag),
		derefString(xr.OutboundTag),
	}
}

// pollTorrentReports stores every report newer than the cursor. The cursor
// starts from the newest report already in ClickHouse, so a restart resumes
// where the last run stopped and the first run backfills.
func (s *Syncer) pollTorrentReports(ctx context.Context) {
	if !s.hasCredentials() {
		return
	}
	if !s.torrentResume {
		since, err := s.latestTorrentReport(ctx)
		if err != nil {
			metrics.DictErrors.WithLabelValues(TableTorrentReports).Inc()
			s.log.Warn("torrent report cursor unavailable", "err", err)
			return
		}
		s.torrentSince, s.torrentResume = since, true
	}

	reports, err := s.fetchTorrentReports(ctx, s.torrentSince)
	if err != nil {
		if !s.optionalUnavailable("torrent reports", "node-plugins:torrent-blocker-reports", err) {
			metrics.DictErrors.WithLabelValues(TableTorrentReports).Inc()
			s.log.Warn("torrent report poll failed", "err", err)
		}
		return
	}
	reports = s.unseenTorrentReports(reports)
	if len(reports) == 0 {
		return
	}

	rows := make([][]any, 0, len(reports))
	for _, r := range reports {
		rows = append(rows, torrentRow(r, s.opt.Geo))
	}
	if err := s.sink.Insert(ctx, TableTorrentReports, torrentColumns, rows); err != nil {
		metrics.DictErrors.WithLabelValues(TableTorrentReports).Inc()
		s.log.Warn("torrent report insert failed", "err", err)
		return
	}
	s.markTorrentReportsStored(reports)
	s.log.Debug("torrent reports stored", "reports", len(rows))
}

// torrentKey identifies a report across a panel-side truncate, which restarts
// the ids but not the clock.
type torrentKey struct {
	id        int64
	createdAt string
}

// unseenTorrentReports drops the reports this process already stored. Only
// the overlap behind the cursor can repeat, so that is all it has to
// remember; after a restart the overlap is written once more and
// v_torrent_reports collapses it.
func (s *Syncer) unseenTorrentReports(reports []torrentReport) []torrentReport {
	out := reports[:0:0]
	for _, r := range reports {
		if _, ok := s.torrentStored[torrentKey{r.ID, r.CreatedAt}]; !ok {
			out = append(out, r)
		}
	}
	return out
}

// markTorrentReportsStored advances the cursor past reports that reached
// ClickHouse and forgets the ones that fell out of the overlap.
func (s *Syncer) markTorrentReportsStored(reports []torrentReport) {
	for _, r := range reports {
		at := parseTime(r.CreatedAt)
		s.torrentStored[torrentKey{r.ID, r.CreatedAt}] = at
		if at.After(s.torrentSince) {
			s.torrentSince = at
		}
	}
	cutoff := s.torrentSince.Add(-torrentOverlap)
	for k, at := range s.torrentStored {
		if at.Before(cutoff) {
			delete(s.torrentStored, k)
		}
	}
}

// latestTorrentReport reads the poll cursor back from ClickHouse. An empty
// table yields the zero time, which means "fetch everything".
func (s *Syncer) latestTorrentReport(ctx context.Context) (time.Time, error) {
	var latest time.Time
	query := fmt.Sprintf("SELECT max(ts) FROM %s.%s", s.sink.DB(), TableTorrentReports)
	if err := s.sink.Conn().QueryRow(ctx, query).Scan(&latest); err != nil {
		return time.Time{}, err
	}
	if latest.Unix() <= 0 {
		return time.Time{}, nil
	}
	return latest.UTC(), nil
}

// fetchTorrentReports pages through the report list, newest first, until it
// reaches reports older than since minus the overlap. The panel's default
// order (id descending) is used on purpose: every panel version has it, and a
// truncate restarts the ids together with the table, so it stays newest first.
func (s *Syncer) fetchTorrentReports(ctx context.Context, since time.Time) ([]torrentReport, error) {
	cutoff := since.Add(-torrentOverlap)
	var out []torrentReport
	for page := 0; page < torrentMaxPages; page++ {
		var decoded torrentReportsResponse
		path := fmt.Sprintf("/api/node-plugins/torrent-blocker?start=%d&size=%d", page*torrentPageSize, torrentPageSize)
		if err := s.getJSON(ctx, path, &decoded); err != nil {
			return nil, err
		}
		batch := decoded.Response.Records
		for _, r := range batch {
			if !since.IsZero() && parseTime(r.CreatedAt).Before(cutoff) {
				return out, nil
			}
			out = append(out, r)
		}
		if len(batch) < torrentPageSize || (page+1)*torrentPageSize >= decoded.Response.Total {
			return out, nil
		}
	}
	s.log.Info("torrent report backfill capped, older reports are skipped", "reports", len(out))
	return out, nil
}

func boolToUInt8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
