-- Migration 028: compact latest orchestrator pipeline inventory.
--
-- API refreshes need the latest model/GPU inventory keyed by
-- (org, orch_address, pipeline_id, model_id). Querying that from the full
-- capability hardware history forces a broad regroup on every refresh. This
-- aggregate table keeps the same semantics but stores the latest values in a
-- compact keyed form for fast joins.

CREATE TABLE IF NOT EXISTS naap.canonical_latest_orchestrator_pipeline_inventory_agg
(
    org                               LowCardinality(String),
    orch_address                      String,
    pipeline_id                       String,
    model_id                          String,
    gpu_id_state                      AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    gpu_model_name_state              AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    gpu_memory_bytes_total_state      AggregateFunction(argMaxIf, UInt64, DateTime64(3, 'UTC'), UInt8),
    runner_version_state              AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    cuda_version_state                AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    last_seen_state                   AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, orch_address, pipeline_id, model_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_latest_orchestrator_pipeline_inventory_agg
TO naap.canonical_latest_orchestrator_pipeline_inventory_agg
AS
SELECT
    org,
    orch_address,
    pipeline_id,
    model_id,
    argMaxIfState(gpu_id, snapshot_ts, toUInt8(gpu_id != '')) AS gpu_id_state,
    argMaxIfState(ifNull(gpu_model_name, ''), snapshot_ts, toUInt8(ifNull(gpu_model_name, '') != '')) AS gpu_model_name_state,
    argMaxIfState(ifNull(gpu_memory_bytes_total, toUInt64(0)), snapshot_ts, toUInt8(ifNull(gpu_memory_bytes_total, toUInt64(0)) > 0)) AS gpu_memory_bytes_total_state,
    argMaxIfState(ifNull(runner_version, ''), snapshot_ts, toUInt8(ifNull(runner_version, '') != '')) AS runner_version_state,
    argMaxIfState(ifNull(cuda_version, ''), snapshot_ts, toUInt8(ifNull(cuda_version, '') != '')) AS cuda_version_state,
    maxState(snapshot_ts) AS last_seen_state
FROM naap.canonical_capability_hardware_inventory
WHERE pipeline_id != ''
GROUP BY org, orch_address, pipeline_id, model_id;
