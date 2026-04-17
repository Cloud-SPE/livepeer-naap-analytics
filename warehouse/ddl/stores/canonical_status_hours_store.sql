-- Canonical DDL for naap.canonical_status_hours_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.

CREATE TABLE IF NOT EXISTS naap.canonical_status_hours_store (`canonical_session_key` String, `org` LowCardinality(String), `hour` DateTime('UTC'), `stream_id` String, `request_id` String, `canonical_pipeline` String, `canonical_model` Nullable(String), `orch_address` Nullable(String), `attribution_status` LowCardinality(String), `attribution_reason` LowCardinality(String), `started_at` Nullable(DateTime64(3, 'UTC')), `session_last_seen` DateTime64(3, 'UTC'), `startup_latency_ms` Nullable(Float64), `status_samples` UInt64, `fps_positive_samples` UInt64, `running_state_samples` UInt64, `degraded_input_samples` UInt64, `degraded_inference_samples` UInt64, `error_samples` UInt64, `avg_output_fps` Float64, `avg_input_fps` Float64, `avg_e2e_latency_ms` Nullable(Float64), `prompt_to_playable_latency_ms` Nullable(Float64), `is_terminal_tail_artifact` UInt8, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(hour)) ORDER BY (canonical_session_key, hour, refreshed_at) SETTINGS index_granularity = 8192;
