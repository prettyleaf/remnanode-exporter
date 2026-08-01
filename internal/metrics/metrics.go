// Package metrics exposes exporter self-telemetry on a Prometheus endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesRead counts stream entries pulled from Redis.
	MessagesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "remnanode_exporter_messages_read_total",
		Help: "Stream entries read from Redis, by stream.",
	}, []string{"stream"})

	// MessagesFailed counts entries that could not be decoded.
	MessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "remnanode_exporter_messages_failed_total",
		Help: "Stream entries that failed to decode, by stream.",
	}, []string{"stream"})

	// RowsInserted counts rows written to ClickHouse.
	RowsInserted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "remnanode_exporter_rows_inserted_total",
		Help: "Rows successfully inserted into ClickHouse, by table.",
	}, []string{"table"})

	// FlushErrors counts failed ClickHouse batches.
	FlushErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "remnanode_exporter_flush_errors_total",
		Help: "Failed ClickHouse batch inserts, by table.",
	}, []string{"table"})

	// FlushDuration tracks how long a batch insert takes.
	FlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "remnanode_exporter_flush_duration_seconds",
		Help:    "ClickHouse batch insert latency, by table.",
		Buckets: prometheus.DefBuckets,
	}, []string{"table"})

	// StreamLag reports the pending (read but unacked) entry count.
	StreamLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "remnanode_exporter_stream_pending",
		Help: "Entries delivered to the consumer group but not yet acknowledged.",
	}, []string{"stream"})

	// DictSize reports how many entries each lookup dictionary holds.
	DictSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "remnanode_exporter_dict_size",
		Help: "Number of entries in a resolved dictionary.",
	}, []string{"dict"})

	// DictErrors counts dictionary refresh failures.
	DictErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "remnanode_exporter_dict_refresh_errors_total",
		Help: "Dictionary refresh failures, by dictionary.",
	}, []string{"dict"})
)
