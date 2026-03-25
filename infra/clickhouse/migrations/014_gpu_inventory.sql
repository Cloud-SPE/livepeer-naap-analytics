-- Migration 014: GPU inventory table (dashboard support)
--
-- Populated by the API enrichment worker (not a Materialized View) because
-- gpu_info is a JSON object with dynamic integer slot keys ("0", "1", ...)
-- that are straightforward to iterate in Go but awkward in ClickHouse SQL.
--
-- The worker reads naap.agg_orch_state FINAL, parses raw_capabilities JSON
-- using the existing gpuInfoEntry/hardwareEntry structs, and batch-inserts
-- into this table on each enrichment cycle.
--
-- ReplacingMergeTree(last_seen) deduplicates on (orch_address, gpu_id) and
-- keeps the most recent record. TTL of 7 days drops stale GPU slots.

CREATE TABLE IF NOT EXISTS naap.agg_gpu_inventory
(
    gpu_id       String,
    orch_address String,
    org          LowCardinality(String),
    gpu_model    String,
    memory_bytes UInt64,
    pipeline     LowCardinality(String),
    model_id     LowCardinality(String),
    last_seen    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (orch_address, gpu_id)
TTL toDateTime(last_seen) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;
