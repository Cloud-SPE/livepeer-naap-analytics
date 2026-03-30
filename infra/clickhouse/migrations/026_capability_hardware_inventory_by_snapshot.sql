-- Migration 026: snapshot-keyed hardware inventory for canonical refresh.
--
-- `canonical-refresh` matches sessions to capability snapshots by `snapshot_row_id`.
-- The existing hardware inventory table is keyed for orchestrator-facing reads,
-- which forces broad scans when refresh joins by snapshot id. This table keeps
-- the same expanded hardware semantics but is physically keyed for snapshot
-- lookups so canonical session refresh can prune aggressively.

CREATE TABLE IF NOT EXISTS naap.canonical_capability_hardware_inventory_by_snapshot
(
    snapshot_row_id            String,
    snapshot_ts                DateTime64(3, 'UTC'),
    org                        LowCardinality(String),
    orch_address               String,
    orch_uri_norm              String,
    pipeline_id                String,
    model_id                   String,
    gpu_id                     String,
    gpu_model_name             Nullable(String),
    gpu_memory_bytes_total     Nullable(UInt64),
    runner_version             Nullable(String),
    cuda_version               Nullable(String)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY (org, toYYYYMM(snapshot_ts))
ORDER BY (snapshot_row_id, pipeline_id, model_id, gpu_id, org)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_capability_hardware_inventory_by_snapshot
TO naap.canonical_capability_hardware_inventory_by_snapshot
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
    JSONExtractString(hardware_json, 'model_id') AS model_id,
    ifNull(nullIf(JSONExtractString(gpu_info_raw, '0', 'id'), ''), '') AS gpu_id,
    nullIf(JSONExtractString(gpu_info_raw, '0', 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_info_raw, '0', 'memory_total'), 0) AS gpu_memory_bytes_total,
    cast(null as Nullable(String)) AS runner_version,
    cast(null as Nullable(String)) AS cuda_version
FROM (
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        hardware_json,
        JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
    FROM (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
        FROM naap.normalized_network_capabilities
        WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
    )
)
WHERE pipeline_id != '';
