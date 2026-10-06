package dict

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"remnanode-exporter/internal/metrics"
)

// hwidPageSize is the most /api/hwid/devices hands out per request.
const hwidPageSize = 1000

type hwidDevice struct {
	HWID        string  `json:"hwid"`
	UserID      int64   `json:"userId"`
	Platform    *string `json:"platform"`
	OSVersion   *string `json:"osVersion"`
	DeviceModel *string `json:"deviceModel"`
	UserAgent   *string `json:"userAgent"`
	RequestIP   *string `json:"requestIp"`
	CreatedAt   string  `json:"createdAt"`
	UpdatedAt   string  `json:"updatedAt"`
}

type hwidDevicesResponse struct {
	Response struct {
		Devices []hwidDevice `json:"devices"`
		Total   int          `json:"total"`
	} `json:"response"`
}

// hwidColumns matches hwidRow positionally.
var hwidColumns = []string{
	"user_id", "hwid", "platform", "os_version", "device_model", "user_agent", "request_ip",
	"created_at", "updated_at", "synced_at",
}

func hwidRow(d hwidDevice, syncedAt time.Time) []any {
	return []any{
		uint64(d.UserID),
		d.HWID,
		derefString(d.Platform),
		derefString(d.OSVersion),
		derefString(d.DeviceModel),
		derefString(d.UserAgent),
		derefString(d.RequestIP),
		parseTime(d.CreatedAt),
		parseTime(d.UpdatedAt),
		syncedAt,
	}
}

// syncHwidDevices rewrites dim_hwid_devices from /api/hwid/devices. Every row
// of one refresh carries the same synced_at, which is how v_hwid_devices tells
// the current device list from devices deleted since, so the whole list is
// written in one insert or not at all.
func (s *Syncer) syncHwidDevices(ctx context.Context) error {
	devices, err := s.fetchHwidDevices(ctx)
	if err != nil {
		return err
	}
	if len(devices) == 0 {
		return nil
	}
	now := time.Now().UTC()
	rows := make([][]any, 0, len(devices))
	for _, d := range devices {
		rows = append(rows, hwidRow(d, now))
	}
	if err := s.sink.Insert(ctx, TableHwidDevices, hwidColumns, rows); err != nil {
		return err
	}
	metrics.DictSize.WithLabelValues(TableHwidDevices).Set(float64(len(rows)))
	s.log.Debug("HWID devices refreshed", "devices", len(rows))
	return nil
}

// fetchHwidDevices pages through the offset-paginated device list.
func (s *Syncer) fetchHwidDevices(ctx context.Context) ([]hwidDevice, error) {
	var out []hwidDevice
	for page := 0; ; page++ {
		if page > 10000 { // hard stop against a server that never runs out
			return nil, fmt.Errorf("HWID device list did not terminate")
		}
		var decoded hwidDevicesResponse
		path := "/api/hwid/devices?start=" + strconv.Itoa(len(out)) + "&size=" + strconv.Itoa(hwidPageSize)
		if err := s.getJSON(ctx, path, &decoded); err != nil {
			return nil, err
		}
		batch := decoded.Response.Devices
		out = append(out, batch...)
		if len(batch) == 0 || len(out) >= decoded.Response.Total {
			return out, nil
		}
	}
}
