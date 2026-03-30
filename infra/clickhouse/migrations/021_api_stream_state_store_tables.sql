-- Migration 021: physical backing tables for hot sample/latest-session API surfaces.
--
-- `api_status_samples_store` stores refreshed per-event serving samples keyed by
-- event_id. `api_active_stream_state_store` stores the latest known session
-- state keyed by canonical_session_key; the dbt api_* model applies the active
-- filter on top of the latest row.

CREATE TABLE IF NOT EXISTS naap.api_status_samples_store
(
    canonical_session_key String,
    event_id              String,
    sample_ts             DateTime64(3, 'UTC'),
    org                   LowCardinality(String),
    stream_id             String,
    request_id            String,
    gateway               String,
    orch_address          Nullable(String),
    pipeline              String,
    model_id              Nullable(String),
    attribution_status    String,
    attribution_reason    String,
    state                 String,
    output_fps            Float64,
    input_fps             Float64,
    e2e_latency_ms        Nullable(Float64),
    is_attributed         UInt8,
    refresh_run_id        String,
    artifact_checksum     String DEFAULT '',
    refreshed_at          DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(sample_ts))
ORDER BY (event_id, refreshed_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.api_active_stream_state_store
(
    canonical_session_key String,
    event_id              String,
    sample_ts             DateTime64(3, 'UTC'),
    org                   LowCardinality(String),
    stream_id             String,
    request_id            String,
    gateway               String,
    pipeline              String,
    model_id              Nullable(String),
    orch_address          Nullable(String),
    attribution_status    String,
    attribution_reason    String,
    state                 String,
    output_fps            Float64,
    input_fps             Float64,
    e2e_latency_ms        Nullable(Float64),
    started_at            Nullable(DateTime64(3, 'UTC')),
    last_seen             DateTime64(3, 'UTC'),
    completed             UInt8,
    refresh_run_id        String,
    artifact_checksum     String DEFAULT '',
    refreshed_at          DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(sample_ts))
ORDER BY (canonical_session_key, refreshed_at)
SETTINGS index_granularity = 8192;
