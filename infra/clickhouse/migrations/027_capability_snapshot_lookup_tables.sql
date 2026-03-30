-- Migration 027: capability snapshot lookup tables for canonical refresh.
--
-- Canonical session attribution needs historical snapshot matches by
-- `(org, orch_address, snapshot_ts)` and `(org, orch_uri_norm, snapshot_ts)`.
-- The general canonical snapshot model is semantically correct but still too
-- broad for high-volume refresh lookups. These tables keep the same snapshot
-- history in physical orders that match the attribution joins directly.

CREATE TABLE IF NOT EXISTS naap.canonical_capability_snapshots_by_address
(
    snapshot_row_id String,
    snapshot_ts     DateTime64(3, 'UTC'),
    org             LowCardinality(String),
    orch_address    String,
    orch_uri        String,
    orch_uri_norm   String
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY (org, toYYYYMM(snapshot_ts))
ORDER BY (org, orch_address, snapshot_ts, snapshot_row_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_capability_snapshots_by_address
TO naap.canonical_capability_snapshots_by_address
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm
FROM naap.normalized_network_capabilities
WHERE orch_address != '';

CREATE TABLE IF NOT EXISTS naap.canonical_capability_snapshots_by_uri
(
    snapshot_row_id String,
    snapshot_ts     DateTime64(3, 'UTC'),
    org             LowCardinality(String),
    orch_address    String,
    orch_uri        String,
    orch_uri_norm   String
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY (org, toYYYYMM(snapshot_ts))
ORDER BY (org, orch_uri_norm, snapshot_ts, snapshot_row_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_capability_snapshots_by_uri
TO naap.canonical_capability_snapshots_by_uri
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm
FROM naap.normalized_network_capabilities
WHERE orch_uri_norm != '';
