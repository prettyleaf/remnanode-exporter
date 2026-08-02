-- Dimension lookups. FINAL is cheap here: both tables hold at most a few
-- thousand rows.

CREATE OR REPLACE VIEW {db}.v_users AS
SELECT
    user_id,
    username,
    status,
    tag,
    traffic_limit_bytes,
    hwid_device_limit,
    expire_at
FROM {db}.dim_users FINAL;

CREATE OR REPLACE VIEW {db}.v_nodes AS
SELECT
    node_id,
    name AS node_name,
    country_code
FROM {db}.dim_nodes FINAL;

-- Fact views with names resolved. Dashboards use these so no panel has to
-- repeat the join, and nodes/users missing from the dimensions still render.

CREATE OR REPLACE VIEW {db}.v_usage AS
SELECT
    u.ts5 AS ts5,
    u.node_id AS node_id,
    if(n.node_name = '', concat('node-', toString(u.node_id)), n.node_name) AS node_name,
    u.user_id AS user_id,
    if(du.username = '', concat('user-', toString(u.user_id)), du.username) AS username,
    du.status AS status,
    du.tag AS tag,
    u.bytes AS bytes
FROM {db}.usage_5m AS u
LEFT JOIN {db}.v_nodes AS n ON n.node_id = u.node_id
LEFT JOIN {db}.v_users AS du ON du.user_id = u.user_id;

CREATE OR REPLACE VIEW {db}.v_connections AS
SELECT
    c.ts AS ts,
    c.node_id AS node_id,
    if(n.node_name = '', concat('node-', toString(c.node_id)), n.node_name) AS node_name,
    c.user_id AS user_id,
    if(du.username = '', concat('user-', toString(c.user_id)), du.username) AS username,
    du.status AS status,
    du.hwid_device_limit AS hwid_device_limit,
    c.ip AS ip,
    c.ip_prefix AS ip_prefix,
    c.last_seen AS last_seen,
    c.country AS country,
    c.city AS city,
    c.asn AS asn,
    c.as_org AS as_org,
    c.is_hosting AS is_hosting
FROM {db}.node_connections AS c
LEFT JOIN {db}.v_nodes AS n ON n.node_id = c.node_id
LEFT JOIN {db}.v_users AS du ON du.user_id = c.user_id;

CREATE OR REPLACE VIEW {db}.v_sub_requests AS
SELECT
    s.ts AS ts,
    s.user_id AS user_id,
    if(du.username = '', concat('user-', toString(s.user_id)), du.username) AS username,
    du.status AS status,
    s.ip AS ip,
    s.ip_prefix AS ip_prefix,
    s.country AS country,
    s.city AS city,
    s.asn AS asn,
    s.as_org AS as_org,
    s.is_hosting AS is_hosting,
    s.user_agent AS user_agent,
    s.ua_family AS ua_family,
    s.ua_kind AS ua_kind,
    s.srr_response_type AS srr_response_type,
    s.srr_rule_name AS srr_rule_name,
    -- The CAST is load-bearing: IN over a LowCardinality column yields
    -- LowCardinality(UInt8), which a view is not allowed to hold.
    CAST(s.srr_response_type IN ('BLOCK', 'STATUS_CODE_404', 'STATUS_CODE_451', 'SOCKET_DROP') AS UInt8) AS is_blocked
FROM {db}.sub_requests AS s
LEFT JOIN {db}.v_users AS du ON du.user_id = s.user_id;

-- The abuse view: one row per user over an arbitrary window, combining traffic,
-- connection fan-out and subscription-fetch behaviour into a single score.
--
-- Call it as a parameterised view, e.g. from Grafana:
--   SELECT * FROM remnawave.v_user_abuse(from = $__fromTime, to = $__toTime)
--   ORDER BY score DESC LIMIT 100
--
-- Score components, all additive and deliberately simple to reason about:
--   gigabytes moved in the window                             1 point per GB
--   distinct /24 (or /48) networks beyond the first two       2 points each
--   distinct countries beyond the first                       5 points each
--   distinct ASNs beyond the first two                        2 points each
--   addresses that belong to a hosting/datacenter ASN         4 points each
--   distinct User-Agents fetching the subscription beyond 2   2 points each
--   subscription fetches beyond 20                            0.2 points each
--   scripted subscription fetches (curl/python/bots)          0.5 points each
--   fetches refused by a response rule, beyond the first 5    0.5 points each

CREATE OR REPLACE VIEW {db}.v_user_abuse AS
WITH
    everyone AS (
        SELECT DISTINCT user_id
        FROM
        (
            SELECT user_id FROM {db}.usage_5m WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
            UNION ALL
            SELECT user_id FROM {db}.user_conn_5m WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
            UNION ALL
            SELECT user_id FROM {db}.sub_req_5m WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
        )
    ),
    traffic AS (
        SELECT
            user_id,
            sum(bytes) AS bytes,
            uniq(node_id) AS traffic_nodes
        FROM {db}.usage_5m
        WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
        GROUP BY user_id
    ),
    conns AS (
        SELECT
            user_id,
            uniqMerge(ips) AS ips,
            uniqMerge(prefixes) AS prefixes,
            uniqMerge(asns) AS asns,
            uniqMerge(countries) AS countries,
            uniqMerge(nodes) AS nodes,
            uniqMerge(hosting_ips) AS hosting_ips
        FROM {db}.user_conn_5m
        WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
        GROUP BY user_id
    ),
    subs AS (
        SELECT
            user_id,
            sum(requests) AS sub_requests,
            sum(scripted) AS sub_scripted,
            sum(blocked) AS sub_blocked,
            uniqMerge(ips) AS sub_ips,
            uniqMerge(uas) AS sub_uas
        FROM {db}.sub_req_5m
        WHERE ts5 >= {from:DateTime} AND ts5 <= {to:DateTime}
        GROUP BY user_id
    )
SELECT
    e.user_id AS user_id,
    if(du.username = '', concat('user-', toString(e.user_id)), du.username) AS username,
    du.status AS status,
    du.tag AS tag,
    du.hwid_device_limit AS hwid_device_limit,
    t.bytes AS bytes,
    round(t.bytes / 1073741824, 2) AS gigabytes,
    t.traffic_nodes AS traffic_nodes,
    c.ips AS ips,
    c.prefixes AS prefixes,
    c.asns AS asns,
    c.countries AS countries,
    c.nodes AS nodes,
    c.hosting_ips AS hosting_ips,
    s.sub_requests AS sub_requests,
    s.sub_scripted AS sub_scripted,
    s.sub_blocked AS sub_blocked,
    s.sub_ips AS sub_ips,
    s.sub_uas AS sub_uas,
    round(
        (t.bytes / 1073741824)
        + greatest(toInt64(c.prefixes) - 2, 0) * 2
        + greatest(toInt64(c.countries) - 1, 0) * 5
        + greatest(toInt64(c.asns) - 2, 0) * 2
        + c.hosting_ips * 4
        + greatest(toInt64(s.sub_uas) - 2, 0) * 2
        + greatest(toInt64(s.sub_requests) - 20, 0) * 0.2
        + s.sub_scripted * 0.5
        + greatest(toInt64(s.sub_blocked) - 5, 0) * 0.5
    , 1) AS score
FROM everyone AS e
LEFT JOIN traffic AS t ON t.user_id = e.user_id
LEFT JOIN conns AS c ON c.user_id = e.user_id
LEFT JOIN subs AS s ON s.user_id = e.user_id
LEFT JOIN {db}.v_users AS du ON du.user_id = e.user_id;
