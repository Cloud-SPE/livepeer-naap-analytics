-- Migration 049: rollup store lifecycle cutover.

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS requested_sessions UInt64 DEFAULT 0
    AFTER known_sessions_count;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS startup_success_sessions UInt64 DEFAULT 0
    AFTER requested_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS no_orch_sessions UInt64 DEFAULT 0
    AFTER startup_success_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS startup_excused_sessions UInt64 DEFAULT 0
    AFTER no_orch_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS startup_failed_sessions UInt64 DEFAULT 0
    AFTER startup_excused_sessions;

ALTER TABLE naap.api_network_demand_by_org_store
    ADD COLUMN IF NOT EXISTS excused_failure_rate Float64 DEFAULT 0
    AFTER startup_success_rate;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS no_orch_sessions UInt64 DEFAULT 0
    AFTER startup_success_sessions;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS startup_failed_sessions UInt64 DEFAULT 0
    AFTER startup_excused_sessions;

ALTER TABLE naap.api_sla_compliance_by_org_store
    ADD COLUMN IF NOT EXISTS excused_failure_rate Nullable(Float64) DEFAULT cast(null as Nullable(Float64))
    AFTER startup_success_rate;

ALTER TABLE naap.api_gpu_metrics_by_org_store
    ADD COLUMN IF NOT EXISTS no_orch_sessions UInt64 DEFAULT 0
    AFTER startup_success_sessions;

ALTER TABLE naap.api_gpu_metrics_by_org_store
    ADD COLUMN IF NOT EXISTS startup_failed_sessions UInt64 DEFAULT 0
    AFTER startup_excused_sessions;

ALTER TABLE naap.api_gpu_metrics_by_org_store
    ADD COLUMN IF NOT EXISTS startup_failed_rate Float64 DEFAULT 0
    AFTER sessions_ending_in_error;
