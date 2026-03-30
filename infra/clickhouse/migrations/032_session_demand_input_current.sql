-- Migration 032: session-grain demand input current table for lean API refresh.
--
-- Network demand publication should aggregate affected windows from a narrow
-- session-grain current table instead of rescanning canonical session/sample
-- stores for each refresh chunk.

CREATE TABLE IF NOT EXISTS naap.canonical_session_demand_input_current
(
    canonical_session_key        String,
    window_start                 DateTime('UTC'),
    org                          LowCardinality(String),
    gateway                      String,
    region                       Nullable(String),
    pipeline_id                  String,
    model_id                     Nullable(String),
    startup_outcome              LowCardinality(String),
    swap_count                   UInt64,
    error_seen                   UInt8,
    status_error_sample_count    UInt64,
    health_signal_coverage_ratio Float64,
    avg_output_fps               Float64,
    total_minutes                Float64,
    ticket_face_value_eth        Float64,
    refresh_run_id               String,
    artifact_checksum            String DEFAULT '',
    refreshed_at                 DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(refreshed_at)
PARTITION BY (org, toYYYYMM(window_start))
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;
