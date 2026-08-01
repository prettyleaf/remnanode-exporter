// Package sink writes decoded rows into ClickHouse in batches.
package sink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"remnanode-exporter/internal/metrics"
)

// Writer owns the ClickHouse connection pool.
type Writer struct {
	conn driver.Conn
	db   string
}

// Options configures the ClickHouse connection.
type Options struct {
	Addrs    []string
	Database string
	Username string
	Password string
}

// Connect dials ClickHouse and verifies the server answers.
func Connect(ctx context.Context, o Options) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: o.Addrs,
		Auth: clickhouse.Auth{
			Database: o.Database,
			Username: o.Username,
			Password: o.Password,
		},
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		MaxOpenConns:    8,
		MaxIdleConns:    4,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &Writer{conn: conn, db: o.Database}, nil
}

// EnsureDatabase creates the target database if it is missing, connecting to
// the server-level `default` database first.
func EnsureDatabase(ctx context.Context, o Options) error {
	bootstrap := o
	bootstrap.Database = "default"
	w, err := Connect(ctx, bootstrap)
	if err != nil {
		return err
	}
	defer w.Close()
	return w.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", o.Database))
}

// Close releases the pool.
func (w *Writer) Close() error { return w.conn.Close() }

// Conn exposes the raw connection for maintenance queries.
func (w *Writer) Conn() driver.Conn { return w.conn }

// DB returns the target database name.
func (w *Writer) DB() string { return w.db }

// Insert writes rows into db.table. Each row must match cols positionally.
func (w *Writer) Insert(ctx context.Context, table string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	stmt := fmt.Sprintf("INSERT INTO %s.%s (%s)", w.db, table, strings.Join(cols, ", "))

	batch, err := w.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		metrics.FlushErrors.WithLabelValues(table).Inc()
		return fmt.Errorf("prepare batch %s: %w", table, err)
	}
	for _, row := range rows {
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			metrics.FlushErrors.WithLabelValues(table).Inc()
			return fmt.Errorf("append row to %s: %w", table, err)
		}
	}
	if err := batch.Send(); err != nil {
		metrics.FlushErrors.WithLabelValues(table).Inc()
		return fmt.Errorf("send batch %s: %w", table, err)
	}

	metrics.FlushDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())
	metrics.RowsInserted.WithLabelValues(table).Add(float64(len(rows)))
	return nil
}

// Exec runs a statement (used for dimension table maintenance).
func (w *Writer) Exec(ctx context.Context, query string, args ...any) error {
	return w.conn.Exec(ctx, query, args...)
}
