// Package consumer reads Remnawave export streams with a Redis consumer group
// and forwards decoded rows to ClickHouse.
//
// Delivery is at-least-once: entries are acknowledged only after the batch they
// belong to has been committed to ClickHouse. A crash between the insert and
// the XACK re-delivers those entries, so the ClickHouse tables tolerate
// duplicates (see the deduplicating views in deploy/clickhouse).
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"remnanode-exporter/internal/metrics"
	"remnanode-exporter/internal/model"
	"remnanode-exporter/internal/sink"
)

// Decoder turns one stream entry into ClickHouse rows for a single table.
type Decoder interface {
	Stream() string
	Table() string
	Columns() []string
	Decode(model.Fields) ([][]any, error)
}

// Options tunes a worker's read and flush behaviour.
type Options struct {
	Group              string
	Consumer           string
	Block              time.Duration
	Count              int64
	StartFromBeginning bool
	ClaimMinIdle       time.Duration
	FlushInterval      time.Duration
	FlushMaxRows       int
}

// Worker consumes one stream.
type Worker struct {
	rdb  *redis.Client
	sink *sink.Writer
	dec  Decoder
	opt  Options
	log  *slog.Logger

	rows      [][]any
	pending   []string
	lastFlush time.Time
}

// NewWorker builds a worker for a single stream/table pair.
func NewWorker(rdb *redis.Client, w *sink.Writer, dec Decoder, opt Options, log *slog.Logger) *Worker {
	return &Worker{
		rdb:       rdb,
		sink:      w,
		dec:       dec,
		opt:       opt,
		log:       log.With("stream", dec.Stream(), "table", dec.Table()),
		lastFlush: time.Now(),
	}
}

// Run blocks until ctx is cancelled, draining whatever is already pending for
// this consumer group before switching to live reads.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}
	w.log.Info("consumer started", "group", w.opt.Group, "consumer", w.opt.Consumer)

	claimTicker := time.NewTicker(w.opt.ClaimMinIdle)
	defer claimTicker.Stop()

	// Anything this consumer read but never acked before a restart.
	w.recoverOwnPending(ctx)

	for {
		select {
		case <-ctx.Done():
			// Best effort: commit what we hold so a clean shutdown loses nothing.
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			err := w.flush(flushCtx)
			cancel()
			if err != nil {
				w.log.Error("final flush failed", "err", err)
			}
			return nil
		case <-claimTicker.C:
			w.claimStale(ctx)
		default:
		}

		n, err := w.readOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			w.log.Error("read failed", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if err := w.maybeFlush(ctx, n == 0); err != nil {
			w.log.Error("flush failed, entries will be redelivered", "err", err)
			w.dropBuffer()
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
		w.reportPending(ctx)
	}
}

func (w *Worker) ensureGroup(ctx context.Context) error {
	start := "$"
	if w.opt.StartFromBeginning {
		start = "0"
	}
	// MKSTREAM lets the exporter start before the panel has published anything.
	err := w.rdb.XGroupCreateMkStream(ctx, w.dec.Stream(), w.opt.Group, start).Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	return nil
}

// isBusyGroup reports whether the consumer group already exists.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// readOnce pulls up to Count new entries and buffers their rows.
func (w *Worker) readOnce(ctx context.Context) (int, error) {
	res, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.opt.Group,
		Consumer: w.opt.Consumer,
		Streams:  []string{w.dec.Stream(), ">"},
		Count:    w.opt.Count,
		Block:    w.opt.Block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil // block timeout, nothing new
	}
	if err != nil {
		return 0, err
	}
	n := 0
	for _, stream := range res {
		for _, msg := range stream.Messages {
			w.consume(msg)
			n++
		}
	}
	return n, nil
}

// recoverOwnPending re-reads entries this consumer name already owns, which
// happens when the process restarts mid-batch.
func (w *Worker) recoverOwnPending(ctx context.Context) {
	for {
		res, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.opt.Group,
			Consumer: w.opt.Consumer,
			Streams:  []string{w.dec.Stream(), "0"},
			Count:    w.opt.Count,
		}).Result()
		if err != nil || len(res) == 0 || len(res[0].Messages) == 0 {
			return
		}
		for _, msg := range res[0].Messages {
			w.consume(msg)
		}
		if err := w.flush(ctx); err != nil {
			w.log.Error("recovery flush failed", "err", err)
			w.dropBuffer()
			return
		}
	}
}

// claimStale takes over entries abandoned by a consumer that died.
func (w *Worker) claimStale(ctx context.Context) {
	msgs, _, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   w.dec.Stream(),
		Group:    w.opt.Group,
		Consumer: w.opt.Consumer,
		MinIdle:  w.opt.ClaimMinIdle,
		Start:    "0",
		Count:    w.opt.Count,
	}).Result()
	if err != nil || len(msgs) == 0 {
		return
	}
	w.log.Info("claimed stale entries", "count", len(msgs))
	for _, msg := range msgs {
		w.consume(msg)
	}
}

func (w *Worker) consume(msg redis.XMessage) {
	metrics.MessagesRead.WithLabelValues(w.dec.Stream()).Inc()
	rows, err := w.dec.Decode(model.Fields(msg.Values))
	if err != nil {
		// A malformed entry must not wedge the stream: log, ack, move on.
		metrics.MessagesFailed.WithLabelValues(w.dec.Stream()).Inc()
		w.log.Warn("decode failed, skipping entry", "id", msg.ID, "err", err)
		w.pending = append(w.pending, msg.ID)
		return
	}
	w.rows = append(w.rows, rows...)
	w.pending = append(w.pending, msg.ID)
}

func (w *Worker) maybeFlush(ctx context.Context, idle bool) error {
	if len(w.pending) == 0 {
		return nil
	}
	if len(w.rows) >= w.opt.FlushMaxRows || idle || time.Since(w.lastFlush) >= w.opt.FlushInterval {
		return w.flush(ctx)
	}
	return nil
}

func (w *Worker) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	if err := w.sink.Insert(ctx, w.dec.Table(), w.dec.Columns(), w.rows); err != nil {
		return err
	}
	if err := w.rdb.XAck(ctx, w.dec.Stream(), w.opt.Group, w.pending...).Err(); err != nil {
		// The data is in ClickHouse; a failed ack only means redelivery.
		w.log.Warn("xack failed", "err", err, "entries", len(w.pending))
	}
	w.dropBuffer()
	return nil
}

func (w *Worker) dropBuffer() {
	w.rows = w.rows[:0]
	w.pending = w.pending[:0]
	w.lastFlush = time.Now()
}

func (w *Worker) reportPending(ctx context.Context) {
	if time.Since(w.lastFlush) < time.Second {
		return
	}
	info, err := w.rdb.XPending(ctx, w.dec.Stream(), w.opt.Group).Result()
	if err != nil {
		return
	}
	metrics.StreamLag.WithLabelValues(w.dec.Stream()).Set(float64(info.Count))
}
