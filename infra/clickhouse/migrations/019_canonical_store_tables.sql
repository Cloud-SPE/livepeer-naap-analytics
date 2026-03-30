-- Migration 019: physical backing tables for canonical session/state surfaces.
--
-- These tables are append-only refresh stores. The canonical dbt models remain
-- the semantic contract and select the latest version per business key from the
-- store, while the refresh worker populates the store from compiled dbt SQL.

CREATE TABLE IF NOT EXISTS naap.canonical_session_latest_store
(
    canonical_session_key         String,
    org                           LowCardinality(String),
    stream_id                     String,
    request_id                    String,
    canonical_pipeline            String,
    canonical_model               Nullable(String),
    started_at                    Nullable(DateTime64(3, 'UTC')),
    last_seen                     DateTime64(3, 'UTC'),
    started                       UInt8,
    playable_seen                 UInt8,
    no_orch                       UInt8,
    completed                     UInt8,
    swap_count                    UInt64,
    restart_seen                  UInt8,
    error_seen                    UInt8,
    degraded_input_seen           UInt8,
    degraded_inference_seen       UInt8,
    status_sample_count           UInt64,
    status_error_sample_count     UInt64,
    loading_only_session          UInt8,
    zero_output_fps_session       UInt8,
    health_signal_count           UInt64,
    health_expected_signal_count  UInt64,
    health_signal_coverage_ratio  Float64,
    startup_outcome               LowCardinality(String),
    has_ambiguous_identity        UInt8,
    has_snapshot_match            UInt8,
    is_hardware_less              UInt8,
    is_stale                      UInt8,
    attribution_reason            LowCardinality(String),
    attribution_status            LowCardinality(String),
    attributed_orch_address       Nullable(String),
    attributed_orch_uri           Nullable(String),
    attribution_snapshot_ts       Nullable(DateTime64(3, 'UTC')),
    refresh_run_id                String,
    artifact_checksum             String DEFAULT '',
    refreshed_at                  DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(last_seen))
ORDER BY (canonical_session_key, refreshed_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_status_hours_store
(
    canonical_session_key      String,
    org                        LowCardinality(String),
    hour                       DateTime('UTC'),
    stream_id                  String,
    request_id                 String,
    canonical_pipeline         String,
    canonical_model            Nullable(String),
    orch_address               Nullable(String),
    attribution_status         LowCardinality(String),
    attribution_reason         LowCardinality(String),
    started_at                 Nullable(DateTime64(3, 'UTC')),
    session_last_seen          DateTime64(3, 'UTC'),
    status_samples             UInt64,
    fps_positive_samples       UInt64,
    running_state_samples      UInt64,
    degraded_input_samples     UInt64,
    degraded_inference_samples UInt64,
    error_samples              UInt64,
    avg_output_fps             Float64,
    avg_input_fps              Float64,
    avg_e2e_latency_ms         Nullable(Float64),
    is_terminal_tail_artifact  UInt8,
    refresh_run_id             String,
    artifact_checksum          String DEFAULT '',
    refreshed_at               DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(hour))
ORDER BY (canonical_session_key, hour, refreshed_at)
SETTINGS index_granularity = 8192;
