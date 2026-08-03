// Package config loads exporter settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full runtime configuration.
type Config struct {
	// Redis / Valkey where Remnawave publishes the export streams.
	RedisURL      string
	RedisGroup    string
	RedisConsumer string
	RedisBlock    time.Duration
	RedisBatch    int64
	// StartFromBeginning creates the consumer group at id 0 instead of $,
	// replaying whatever is still retained in the stream.
	StartFromBeginning bool
	// ClaimMinIdle re-delivers entries another consumer read but never acked.
	ClaimMinIdle time.Duration

	ClickHouseAddrs    []string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string

	FlushInterval time.Duration
	FlushMaxRows  int

	GeoIPCityPath string
	GeoIPASNPath  string
	GeoIPReload   time.Duration

	// Remnawave panel API, used to resolve the numeric user and node ids the
	// streams carry into names, and to report the panel's export settings at
	// startup. Node ids need a panel on 3.1.0 or newer.
	APIURL      string
	APIToken    string
	DictRefresh time.Duration

	MetricsAddr string
	LogLevel    string
}

// Load reads the configuration, applying defaults and validating the result.
func Load() (Config, error) {
	host, _ := os.Hostname()
	if host == "" {
		host = "exporter"
	}

	c := Config{
		RedisURL:           env("REDIS_URL", "unix:///var/run/valkey/valkey.sock"),
		RedisGroup:         env("REDIS_GROUP", "remnanode-exporter"),
		RedisConsumer:      env("REDIS_CONSUMER", host),
		RedisBlock:         envDuration("REDIS_BLOCK", 5*time.Second),
		RedisBatch:         int64(envInt("REDIS_BATCH", 500)),
		StartFromBeginning: envBool("REDIS_START_FROM_BEGINNING", true),
		ClaimMinIdle:       envDuration("REDIS_CLAIM_MIN_IDLE", time.Minute),

		ClickHouseAddrs:    envList("CLICKHOUSE_ADDR", []string{"clickhouse:9000"}),
		ClickHouseDB:       env("CLICKHOUSE_DB", "remnawave"),
		ClickHouseUser:     env("CLICKHOUSE_USER", "default"),
		ClickHousePassword: env("CLICKHOUSE_PASSWORD", ""),

		FlushInterval: envDuration("FLUSH_INTERVAL", 5*time.Second),
		FlushMaxRows:  envInt("FLUSH_MAX_ROWS", 50000),

		GeoIPCityPath: env("GEOIP_CITY_DB", "/geoip/GeoLite2-City.mmdb"),
		GeoIPASNPath:  env("GEOIP_ASN_DB", "/geoip/GeoLite2-ASN.mmdb"),
		GeoIPReload:   envDuration("GEOIP_RELOAD_INTERVAL", 6*time.Hour),

		APIURL:      strings.TrimRight(env("REMNAWAVE_API_URL", ""), "/"),
		APIToken:    env("REMNAWAVE_API_TOKEN", ""),
		DictRefresh: envDuration("DICT_REFRESH_INTERVAL", 5*time.Minute),

		// 9101 is deliberately avoided: the common Remnawave monitoring setup
		// already runs cAdvisor there on every host.
		MetricsAddr: env("METRICS_ADDR", ":9102"),
		LogLevel:    env("LOG_LEVEL", "info"),
	}

	if c.RedisBatch <= 0 {
		return c, fmt.Errorf("REDIS_BATCH must be > 0")
	}
	if c.FlushMaxRows <= 0 {
		return c, fmt.Errorf("FLUSH_MAX_ROWS must be > 0")
	}
	if c.FlushInterval <= 0 {
		return c, fmt.Errorf("FLUSH_INTERVAL must be > 0")
	}
	if len(c.ClickHouseAddrs) == 0 {
		return c, fmt.Errorf("CLICKHOUSE_ADDR must not be empty")
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envList(key string, def []string) []string {
	raw := env(key, "")
	if raw == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(key string, def int) int {
	raw := env(key, "")
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	raw := env(key, "")
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return b
}

func envDuration(key string, def time.Duration) time.Duration {
	raw := env(key, "")
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return d
}
