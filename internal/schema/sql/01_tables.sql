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
    ua_kind    LowCardinality(String),
    -- What the Subscription Response Rules engine served (Remnawave 3.1+):
    -- a template name (XRAY_JSON, SINGBOX, …) or a refusal (BLOCK,
    -- STATUS_CODE_404, STATUS_CODE_451, SOCKET_DROP). Empty on older panels.
    srr_response_type LowCardinality(String),
    srr_rule_name     LowCardinality(String)
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
    is_hosting UInt8,
    -- The address belongs to one of the panel's own nodes (its address or an
    -- entry of its ips list, Remnawave 3.3+): a chained node connecting
    -- through a service account, not a subscriber.
    is_infra   UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (node_id, user_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- One row per Torrent Blocker report, polled from the panel API rather than a
-- stream. The poller re-reads a short overlap on every pass and a panel-side
-- truncate restarts report ids, so a row is identified by the whole sort key
-- and readers go through v_torrent_reports, which collapses the re-reads.

CREATE TABLE IF NOT EXISTS {db}.torrent_reports
(
    ts            DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    report_id     UInt64 CODEC(ZSTD(1)),
    node_id       UInt64 CODEC(ZSTD(1)),
    user_id       UInt64 CODEC(ZSTD(1)),
    ip            String CODEC(ZSTD(1)),
    ip_prefix     String CODEC(ZSTD(1)),
    country       LowCardinality(String),
    city          String CODEC(ZSTD(1)),
    asn           UInt32 CODEC(ZSTD(1)),
    as_org        LowCardinality(String),
    is_hosting    UInt8,
    blocked       UInt8,
    block_seconds UInt32,
    destination   String CODEC(ZSTD(1)),
    network       LowCardinality(String),
    protocol      LowCardinality(String),
    inbound_tag   LowCardinality(String),
    outbound_tag  LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (user_id, ts, report_id)
TTL toDateTime(ts) + INTERVAL 180 DAY;

-- Dimensions. The exporter rewrites them wholesale on every refresh and
-- ReplacingMergeTree keeps the newest row per key.

-- Timestamps the panel reports as null (never online, never revoked) are
-- stored as the epoch, since DateTime64 cannot hold a null without Nullable.

CREATE TABLE IF NOT EXISTS {db}.dim_users
(
    user_id                     UInt64,
    username                    String,
    status                      LowCardinality(String),
    tag                         LowCardinality(String),
    traffic_limit_bytes         UInt64,
    hwid_device_limit           Int64,
    expire_at                   DateTime64(3, 'UTC'),
    traffic_limit_strategy      LowCardinality(String),
    used_traffic_bytes          UInt64,
    lifetime_used_traffic_bytes UInt64,
    telegram_id                 Int64,
    internal_squads             Array(LowCardinality(String)),
    created_at                  DateTime64(3, 'UTC'),
    online_at                   DateTime64(3, 'UTC'),
    first_connected_at          DateTime64(3, 'UTC'),
    sub_revoked_at              DateTime64(3, 'UTC'),
    last_connected_node_uuid    String,
    updated_at                  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS {db}.dim_nodes
(
    node_id      UInt64,
    uuid         String,
    name         String,
    country_code LowCardinality(String),
    address      String,
    provider     LowCardinality(String),
    tags         Array(LowCardinality(String)),
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY node_id;

-- Every HWID device the panel knows. Each refresh writes the whole list under
-- one synced_at, so a device deleted in the panel is simply absent from the
-- newest sync: v_hwid_devices reads only that one, and the TTL clears the rest.

CREATE TABLE IF NOT EXISTS {db}.dim_hwid_devices
(
    user_id      UInt64,
    hwid         String,
    platform     LowCardinality(String),
    os_version   LowCardinality(String),
    device_model LowCardinality(String),
    user_agent   String,
    request_ip   String,
    created_at   DateTime64(3, 'UTC'),
    updated_at   DateTime64(3, 'UTC'),
    synced_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (user_id, hwid)
TTL toDateTime(synced_at) + INTERVAL 1 DAY;

-- Upgrades for databases created by an older exporter. CREATE TABLE IF NOT
-- EXISTS is a no-op on a table that already exists, so columns added later
-- have to be applied separately. They land before 02_rollups.sql, which reads
-- them from the materialised views.

ALTER TABLE {db}.sub_requests
    ADD COLUMN IF NOT EXISTS srr_response_type LowCardinality(String) AFTER ua_kind;

ALTER TABLE {db}.sub_requests
    ADD COLUMN IF NOT EXISTS srr_rule_name LowCardinality(String) AFTER srr_response_type;

ALTER TABLE {db}.node_connections
    ADD COLUMN IF NOT EXISTS is_infra UInt8 AFTER is_hosting;

ALTER TABLE {db}.dim_users
    ADD COLUMN IF NOT EXISTS traffic_limit_strategy LowCardinality(String) AFTER expire_at,
    ADD COLUMN IF NOT EXISTS used_traffic_bytes UInt64 AFTER traffic_limit_strategy,
    ADD COLUMN IF NOT EXISTS lifetime_used_traffic_bytes UInt64 AFTER used_traffic_bytes,
    ADD COLUMN IF NOT EXISTS telegram_id Int64 AFTER lifetime_used_traffic_bytes,
    ADD COLUMN IF NOT EXISTS internal_squads Array(LowCardinality(String)) AFTER telegram_id,
    ADD COLUMN IF NOT EXISTS created_at DateTime64(3, 'UTC') AFTER internal_squads,
    ADD COLUMN IF NOT EXISTS online_at DateTime64(3, 'UTC') AFTER created_at,
    ADD COLUMN IF NOT EXISTS first_connected_at DateTime64(3, 'UTC') AFTER online_at,
    ADD COLUMN IF NOT EXISTS sub_revoked_at DateTime64(3, 'UTC') AFTER first_connected_at,
    ADD COLUMN IF NOT EXISTS last_connected_node_uuid String AFTER sub_revoked_at;

ALTER TABLE {db}.dim_nodes
    ADD COLUMN IF NOT EXISTS address String AFTER country_code,
    ADD COLUMN IF NOT EXISTS provider LowCardinality(String) AFTER address,
    ADD COLUMN IF NOT EXISTS tags Array(LowCardinality(String)) AFTER provider;
