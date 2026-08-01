-- Five-minute rollups. Dashboards read these instead of the raw tables so that
-- a 30-day window stays interactive.

CREATE TABLE IF NOT EXISTS {db}.usage_5m
(
    ts5     DateTime('UTC'),
    node_id UInt64,
    user_id UInt64,
    bytes   UInt64
)
ENGINE = SummingMergeTree(bytes)
PARTITION BY toYYYYMM(ts5)
ORDER BY (node_id, user_id, ts5)
TTL ts5 + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.usage_5m_mv TO {db}.usage_5m AS
SELECT
    toStartOfFiveMinute(ts) AS ts5,
    node_id,
    user_id,
    sum(bytes) AS bytes
FROM {db}.user_usage
GROUP BY ts5, node_id, user_id;

-- Connection fan-out per user: how many addresses, networks, ASNs, countries
-- and nodes a single account was seen on inside one 5-minute bucket.
-- uniqState is used so buckets can be merged across arbitrary time ranges.

CREATE TABLE IF NOT EXISTS {db}.user_conn_5m
(
    ts5         DateTime('UTC'),
    user_id     UInt64,
    ips         AggregateFunction(uniq, String),
    prefixes    AggregateFunction(uniq, String),
    asns        AggregateFunction(uniq, UInt32),
    countries   AggregateFunction(uniq, String),
    nodes       AggregateFunction(uniq, UInt64),
    hosting_ips AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts5)
ORDER BY (user_id, ts5)
TTL ts5 + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.user_conn_5m_mv TO {db}.user_conn_5m AS
SELECT
    toStartOfFiveMinute(ts) AS ts5,
    user_id,
    uniqState(ip) AS ips,
    uniqState(ip_prefix) AS prefixes,
    uniqState(asn) AS asns,
    uniqStateIf(country, country != '') AS countries,
    uniqState(node_id) AS nodes,
    uniqStateIf(ip, is_hosting = 1) AS hosting_ips
FROM {db}.node_connections
GROUP BY ts5, user_id;

-- Subscription fetch rate per user. A human client refreshes a handful of
-- times an hour; a scraper or a shared link does far more, from more places.

CREATE TABLE IF NOT EXISTS {db}.sub_req_5m
(
    ts5       DateTime('UTC'),
    user_id   UInt64,
    requests  SimpleAggregateFunction(sum, UInt64),
    ips       AggregateFunction(uniq, String),
    prefixes  AggregateFunction(uniq, String),
    asns      AggregateFunction(uniq, UInt32),
    countries AggregateFunction(uniq, String),
    uas       AggregateFunction(uniq, String),
    scripted  SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts5)
ORDER BY (user_id, ts5)
TTL ts5 + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.sub_req_5m_mv TO {db}.sub_req_5m AS
SELECT
    toStartOfFiveMinute(ts) AS ts5,
    user_id,
    count() AS requests,
    uniqState(ip) AS ips,
    uniqState(ip_prefix) AS prefixes,
    uniqState(asn) AS asns,
    uniqStateIf(country, country != '') AS countries,
    uniqState(user_agent) AS uas,
    countIf(ua_kind IN ('script', 'bot')) AS scripted
FROM {db}.sub_requests
GROUP BY ts5, user_id;
