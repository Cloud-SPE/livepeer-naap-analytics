-- Migration 020: physical backing tables for hot api_* org-window rollups.
--
-- These tables are append-only slice stores. Each refresh run recomputes all
-- rows for an affected (org, window_start) slice and tags them with the
-- refresh_run_id. The dbt api_* models read only the latest refresh for each
-- slice so stale grouped rows disappear when a slice is recomputed.

CREATE TABLE IF NOT EXISTS naap.api_network_demand_by_org_store
(
    window_start                 DateTime('UTC'),
    org                          LowCardinality(String),
    gateway                      String,
    region                       Nullable(String),
    pipeline_id                  String,
    model_id                     Nullable(String),
    sessions_count               UInt64,
    avg_output_fps               Float64,
    total_minutes                Float64,
    known_sessions_count         UInt64,
    served_sessions              UInt64,
    unserved_sessions            UInt64,
    total_demand_sessions        UInt64,
    startup_unexcused_sessions   UInt64,
    confirmed_swapped_sessions   UInt64,
    inferred_swap_sessions       UInt64,
    total_swapped_sessions       UInt64,
    sessions_ending_in_error     UInt64,
    error_status_samples         UInt64,
    health_signal_coverage_ratio Float64,
    startup_success_rate         Float64,
    effective_success_rate       Float64,
    ticket_face_value_eth        Float64,
    refresh_run_id               String,
    artifact_checksum            String DEFAULT '',
    refreshed_at                 DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(window_start))
ORDER BY (org, window_start, gateway, pipeline_id, ifNull(model_id, ''), refreshed_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.api_sla_compliance_by_org_store
(
    window_start                 DateTime('UTC'),
    org                          LowCardinality(String),
    orchestrator_address         String,
    pipeline_id                  String,
    model_id                     Nullable(String),
    gpu_id                       Nullable(String),
    region                       Nullable(String),
    known_sessions_count         UInt64,
    startup_success_sessions     UInt64,
    startup_excused_sessions     UInt64,
    startup_unexcused_sessions   UInt64,
    confirmed_swapped_sessions   UInt64,
    inferred_swap_sessions       UInt64,
    total_swapped_sessions       UInt64,
    sessions_ending_in_error     UInt64,
    error_status_samples         UInt64,
    health_signal_coverage_ratio Float64,
    startup_success_rate         Nullable(Float64),
    effective_success_rate       Nullable(Float64),
    no_swap_rate                 Nullable(Float64),
    sla_score                    Nullable(Float64),
    refresh_run_id               String,
    artifact_checksum            String DEFAULT '',
    refreshed_at                 DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(window_start))
ORDER BY (org, window_start, orchestrator_address, pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), refreshed_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.api_gpu_metrics_by_org_store
(
    window_start                           DateTime('UTC'),
    org                                    LowCardinality(String),
    orchestrator_address                   String,
    pipeline_id                            String,
    model_id                               Nullable(String),
    gpu_id                                 Nullable(String),
    region                                 Nullable(String),
    avg_output_fps                         Float64,
    p95_output_fps                         Float64,
    fps_jitter_coefficient                 Nullable(Float64),
    status_samples                         UInt64,
    error_status_samples                   UInt64,
    health_signal_coverage_ratio           Float64,
    gpu_model_name                         Nullable(String),
    gpu_memory_bytes_total                 Nullable(UInt64),
    runner_version                         Nullable(String),
    cuda_version                           Nullable(String),
    avg_prompt_to_first_frame_ms           Nullable(Float64),
    avg_startup_latency_ms                 Nullable(Float64),
    avg_e2e_latency_ms                     Nullable(Float64),
    p95_prompt_to_first_frame_latency_ms   Nullable(Float64),
    p95_startup_latency_ms                 Nullable(Float64),
    p95_e2e_latency_ms                     Nullable(Float64),
    prompt_to_first_frame_sample_count     UInt64,
    startup_latency_sample_count           UInt64,
    e2e_latency_sample_count               UInt64,
    known_sessions_count                   UInt64,
    startup_success_sessions               UInt64,
    startup_excused_sessions               UInt64,
    startup_unexcused_sessions             UInt64,
    confirmed_swapped_sessions             UInt64,
    inferred_swap_sessions                 UInt64,
    total_swapped_sessions                 UInt64,
    sessions_ending_in_error               UInt64,
    startup_unexcused_rate                 Float64,
    swap_rate                              Float64,
    refresh_run_id                         String,
    artifact_checksum                      String DEFAULT '',
    refreshed_at                           DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(window_start))
ORDER BY (org, window_start, orchestrator_address, pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), refreshed_at)
SETTINGS index_granularity = 8192;
