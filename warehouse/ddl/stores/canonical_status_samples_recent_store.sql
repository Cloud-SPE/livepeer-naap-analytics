-- Canonical DDL for naap.canonical_status_samples_recent_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.

CREATE TABLE IF NOT EXISTS naap.canonical_status_samples_recent_store (`canonical_session_key` String, `event_id` String, `sample_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String), `stream_id` String, `request_id` String, `gateway` String, `orch_address` Nullable(String), `pipeline` String, `model_id` Nullable(String), `attribution_status` String, `attribution_reason` String, `state` String, `output_fps` Float64, `input_fps` Float64, `e2e_latency_ms` Nullable(Float64), `is_attributed` UInt8, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64(), PROJECTION by_refresh_run_session (SELECT refresh_run_id, canonical_session_key, sample_ts, gateway, output_fps, org, stream_id, request_id, orch_address, pipeline, model_id, attribution_status, attribution_reason, state, input_fps, e2e_latency_ms, event_id ORDER BY refresh_run_id, canonical_session_key, sample_ts)) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(sample_ts)) ORDER BY (event_id, refreshed_at) SETTINGS index_granularity = 8192;
