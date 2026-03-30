-- Migration 033: stage changed attribution rows once per refresh chunk.
--
-- The repair worker currently computes changed attribution rows and then
-- re-executes the same heavy query multiple times for audit/current/delta
-- publication. This table lets the worker materialize the changed set once and
-- reuse it for the remaining publication steps.

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_changed_attribution
(
    run_id                     String,
    canonical_session_key      String,
    org                        LowCardinality(String),
    decision_anchor_ts         DateTime64(3, 'UTC'),
    canonical_model            Nullable(String),
    has_ambiguous_identity     UInt8,
    has_snapshot_match         UInt8,
    is_hardware_less           UInt8,
    is_stale                   UInt8,
    attribution_reason         LowCardinality(String),
    attribution_status         LowCardinality(String),
    attributed_orch_address    Nullable(String),
    attributed_orch_uri        Nullable(String),
    attribution_snapshot_row_id Nullable(String),
    attribution_snapshot_ts    Nullable(DateTime64(3, 'UTC')),
    attribution_method         LowCardinality(String),
    selection_confidence       LowCardinality(String),
    attribution_input_hash     String,
    inserted_at                DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, canonical_session_key)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
