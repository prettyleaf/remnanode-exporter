-- Raw facts written by the exporter, one table per Remnawave export stream.
-- Delivery is at-least-once, so every table may contain duplicate rows after a
-- crash; the rollups in 02_rollups.sql are additive and the abuse views use
-- uniq() over identities rather than raw counts wherever it matters.

CREATE TABLE IF NOT EXISTS {db}.user_usage
(
    ts      DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    node_id UInt64 CODEC(ZSTD(1)),
    user_id UInt64 CODEC(ZSTD(1)),
    bytes   UInt64 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (node_id, user_id, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS {db}.sub_requests
(
    ts         DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    user_id    UInt64 CODEC(ZSTD(1)),
    ip         String CODEC(ZSTD(1)),
    ip_prefix  String CODEC(ZSTD(1)),
    country    LowCardinality(String),
    city       String CODEC(ZSTD(1)),
    asn        UInt32 CODEC(ZSTD(1)),
    as_org     LowCardinality(String),
    is_hosting UInt8,
    user_agent String CODEC(ZSTD(1)),
    ua_family  LowCardinality(String),
    ua_kind    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (user_id, ts)
TTL toDateTime(ts) + INTERVAL 60 DAY;

-- One row per user/IP pair of a node snapshot. The panel emits a snapshot per
-- node roughly every 5 minutes, so this is the widest table by far.

CREATE TABLE IF NOT EXISTS {db}.node_connections
(
    ts         DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    node_id    UInt64 CODEC(ZSTD(1)),
    user_id    UInt64 CODEC(ZSTD(1)),
    ip         String CODEC(ZSTD(1)),
    ip_prefix  String CODEC(ZSTD(1)),
    last_seen  DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    country    LowCardinality(String),
    city       String CODEC(ZSTD(1)),
    asn        UInt32 CODEC(ZSTD(1)),
    as_org     LowCardinality(String),
    is_hosting UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (node_id, user_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- Dimensions. The exporter rewrites them wholesale on every refresh and
-- ReplacingMergeTree keeps the newest row per key.

CREATE TABLE IF NOT EXISTS {db}.dim_users
(
    user_id             UInt64,
    username            String,
    status              LowCardinality(String),
    tag                 LowCardinality(String),
    traffic_limit_bytes UInt64,
    hwid_device_limit   Int64,
    expire_at           DateTime64(3, 'UTC'),
    updated_at          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS {db}.dim_nodes
(
    node_id      UInt64,
    uuid         String,
    name         String,
    country_code LowCardinality(String),
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY node_id;
