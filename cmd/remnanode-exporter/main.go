// Command remnanode-exporter ships the Remnawave Redis export streams into
// ClickHouse and enriches every address with MaxMind GeoLite2 data.
// Verified against panel 3.1.x and 3.2.0.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"remnanode-exporter/internal/config"
	"remnanode-exporter/internal/consumer"
	"remnanode-exporter/internal/dict"
	"remnanode-exporter/internal/geoip"
	"remnanode-exporter/internal/schema"
	"remnanode-exporter/internal/sink"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	chOpts := sink.Options{
		Addrs:    cfg.ClickHouseAddrs,
		Database: cfg.ClickHouseDB,
		Username: cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
	}
	if err := waitFor(ctx, "clickhouse", func(ctx context.Context) error {
		return sink.EnsureDatabase(ctx, chOpts)
	}, log); err != nil {
		return err
	}

	writer, err := sink.Connect(ctx, chOpts)
	if err != nil {
		return err
	}
	defer writer.Close()

	if err := schema.Apply(ctx, writer, log); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	log.Info("clickhouse schema ready", "database", cfg.ClickHouseDB)

	geo := geoip.New(cfg.GeoIPCityPath, cfg.GeoIPASNPath, nil, log)
	defer geo.Close()

	rdb, err := newRedis(cfg.RedisURL)
	if err != nil {
		return err
	}
	defer rdb.Close()

	if err := waitFor(ctx, "redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}, log); err != nil {
		return err
	}

	decoders := []consumer.Decoder{
		consumer.UserUsageDecoder{},
		consumer.SubRequestDecoder{Geo: geo},
		consumer.NodeConnectionsDecoder{Geo: geo},
	}
	workerOpts := consumer.Options{
		Group:              cfg.RedisGroup,
		Consumer:           cfg.RedisConsumer,
		Block:              cfg.RedisBlock,
		Count:              cfg.RedisBatch,
		StartFromBeginning: cfg.StartFromBeginning,
		ClaimMinIdle:       cfg.ClaimMinIdle,
		FlushInterval:      cfg.FlushInterval,
		FlushMaxRows:       cfg.FlushMaxRows,
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(decoders)+1)

	for _, dec := range decoders {
		w := consumer.NewWorker(rdb, writer, dec, workerOpts, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				errs <- err
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		geo.Watch(ctx, cfg.GeoIPReload)
	}()

	syncer := dict.New(dict.Options{
		APIURL:   cfg.APIURL,
		APIToken: cfg.APIToken,
		Interval: cfg.DictRefresh,
	}, writer, log)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := syncer.Run(ctx); err != nil {
			errs <- err
		}
	}()

	srv := serveMetrics(cfg.MetricsAddr, rdb, log)

	log.Info("remnanode-exporter running",
		"version", version,
		"redis", cfg.RedisURL,
		"clickhouse", strings.Join(cfg.ClickHouseAddrs, ","),
		"metrics", cfg.MetricsAddr)

	select {
	case <-ctx.Done():
	case err := <-errs:
		stop()
		wg.Wait()
		return err
	}

	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("stopped")
	return nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// newRedis accepts both TCP (redis://host:6379/0) and the unix socket the
// Remnawave compose file exposes (unix:///var/run/valkey/valkey.sock).
func newRedis(rawURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL %q: %w", rawURL, err)
	}
	return redis.NewClient(opts), nil
}

// waitFor retries a dependency check until it succeeds or ctx is cancelled.
func waitFor(ctx context.Context, name string, probe func(context.Context) error, log *slog.Logger) error {
	backoff := time.Second
	for {
		err := probe(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("dependency not ready", "dependency", name, "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

func serveMetrics(addr string, rdb *redis.Client, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()
	return srv
}
