-- Canonical DDL for naap.canonical_active_stream_state_latest_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.

CREATE TABLE IF NOT EXISTS naap.canonical_active_stream_state_latest_store (`canonical_session_key` String, `event_id` String, `sample_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String), `stream_id` String, `request_id` String, `gateway` String, `pipeline` String, `model_id` Nullable(String), `orch_address` Nullable(String), `attribution_status` String, `attribution_reason` String, `state` String, `output_fps` Float64, `input_fps` Float64, `e2e_latency_ms` Nullable(Float64), `started_at` Nullable(DateTime64(3, 'UTC')), `last_seen` DateTime64(3, 'UTC'), `completed` UInt8, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(sample_ts)) ORDER BY (canonical_session_key, refreshed_at) SETTINGS index_granularity = 8192;
