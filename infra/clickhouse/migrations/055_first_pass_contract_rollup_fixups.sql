-- Migration 055: first-pass contracted rollup and serving fixups.

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS loading_only_session UInt8 DEFAULT 0
    AFTER excusal_reason;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS zero_output_fps_session UInt8 DEFAULT 0
    AFTER loading_only_session;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS health_signal_count UInt64 DEFAULT 0
    AFTER zero_output_fps_session;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS health_expected_signal_count UInt64 DEFAULT 0
    AFTER health_signal_count;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS loading_only_sessions UInt64 DEFAULT 0
    AFTER startup_failed_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS zero_output_fps_sessions UInt64 DEFAULT 0
    AFTER loading_only_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS effective_failed_sessions UInt64 DEFAULT 0
    AFTER zero_output_fps_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS health_signal_count UInt64 DEFAULT 0
    AFTER error_status_samples;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS health_expected_signal_count UInt64 DEFAULT 0
    AFTER health_signal_count;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS requested_sessions UInt64 DEFAULT 0
    AFTER known_sessions_count;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS loading_only_sessions UInt64 DEFAULT 0
    AFTER startup_failed_sessions;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS zero_output_fps_sessions UInt64 DEFAULT 0
    AFTER loading_only_sessions;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS effective_failed_sessions UInt64 DEFAULT 0
    AFTER zero_output_fps_sessions;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS health_signal_count UInt64 DEFAULT 0
    AFTER error_status_samples;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS health_expected_signal_count UInt64 DEFAULT 0
    AFTER health_signal_count;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS output_viability_rate Nullable(Float64) DEFAULT cast(null as Nullable(Float64))
    AFTER no_swap_rate;

CREATE TABLE IF NOT EXISTS naap.api_gpu_network_demand_by_org_store
(
    window_start                 DateTime('UTC'),
    org                          LowCardinality(String),
    gateway                      String,
    orchestrator_address         String,
    region                       Nullable(String),
    pipeline_id                  String,
    model_id                     Nullable(String),
    gpu_id                       Nullable(String),
    gpu_identity_status          LowCardinality(String),
    sessions_count               UInt64,
    avg_output_fps               Float64,
    total_minutes                Float64,
    known_sessions_count         UInt64,
    requested_sessions           UInt64,
    startup_success_sessions     UInt64,
    no_orch_sessions             UInt64,
    startup_excused_sessions     UInt64,
    startup_failed_sessions      UInt64,
    loading_only_sessions        UInt64,
    zero_output_fps_sessions     UInt64,
    effective_failed_sessions    UInt64,
    confirmed_swapped_sessions   UInt64,
    inferred_swap_sessions       UInt64,
    total_swapped_sessions       UInt64,
    sessions_ending_in_error     UInt64,
    error_status_samples         UInt64,
    health_signal_count          UInt64,
    health_expected_signal_count UInt64,
    health_signal_coverage_ratio Float64,
    startup_success_rate         Nullable(Float64),
    excused_failure_rate         Nullable(Float64),
    effective_success_rate       Nullable(Float64),
    ticket_face_value_eth        Float64,
    refresh_run_id               String,
    artifact_checksum            String DEFAULT '',
    refreshed_at                 DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(window_start))
ORDER BY (
    org,
    window_start,
    gateway,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, ''),
    ifNull(gpu_id, ''),
    gpu_identity_status,
    refreshed_at
)
SETTINGS index_granularity = 8192;
