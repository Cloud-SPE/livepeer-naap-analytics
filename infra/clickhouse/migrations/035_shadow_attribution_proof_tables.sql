-- Migration 035: shadow proof tables for Go attribution comparison.
--
-- The SQL attribution path remains the published truth until the Go resolver
-- proves semantic parity and better runtime on bounded chunks.

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_shadow_comparisons
(
    run_id                String,
    stage_run_id          String,
    mode                  LowCardinality(String),
    input_rows            UInt64,
    uri_candidate_rows    UInt64,
    address_candidate_rows UInt64,
    model_lookup_rows     UInt64,
    snapshot_summary_rows UInt64,
    compared_rows         UInt64,
    parity_rows           UInt64,
    mismatch_rows         UInt64,
    go_runtime_ms         UInt64,
    sql_runtime_ms        UInt64,
    compared_at           DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(compared_at)
ORDER BY (run_id, stage_run_id, compared_at)
TTL compared_at + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_shadow_mismatches
(
    run_id                          String,
    stage_run_id                    String,
    canonical_session_key           String,
    org                             LowCardinality(String),
    mismatch_fields                 Array(String),
    sql_attribution_status          LowCardinality(String),
    go_attribution_status           LowCardinality(String),
    sql_attribution_reason          LowCardinality(String),
    go_attribution_reason           LowCardinality(String),
    sql_canonical_model             Nullable(String),
    go_canonical_model              Nullable(String),
    sql_attributed_orch_address     Nullable(String),
    go_attributed_orch_address      Nullable(String),
    sql_attributed_orch_uri         Nullable(String),
    go_attributed_orch_uri          Nullable(String),
    sql_attribution_snapshot_row_id Nullable(String),
    go_attribution_snapshot_row_id  Nullable(String),
    sql_attribution_snapshot_ts     Nullable(DateTime64(3, 'UTC')),
    go_attribution_snapshot_ts      Nullable(DateTime64(3, 'UTC')),
    sql_attribution_method          LowCardinality(String),
    go_attribution_method           LowCardinality(String),
    sql_selection_confidence        LowCardinality(String),
    go_selection_confidence         LowCardinality(String),
    compared_at                     DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(compared_at)
ORDER BY (run_id, stage_run_id, canonical_session_key, compared_at)
TTL compared_at + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
