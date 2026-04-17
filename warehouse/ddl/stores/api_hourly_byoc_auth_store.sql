-- Canonical DDL for naap.api_hourly_byoc_auth_store.
-- Source of truth: this file. The resolver writes rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.
--
-- Phase 6.2 of serving-layer-v2: promotes api_hourly_byoc_auth from an
-- on-demand view that rescanned canonical_byoc_auth every request to a
-- pre-aggregated store the resolver writes per window slice. One row per
-- (org, window_start, capability_name, refresh_run_id).

CREATE TABLE IF NOT EXISTS naap.api_hourly_byoc_auth_store (`window_start` DateTime('UTC'), `org` LowCardinality(String), `capability_name` String, `total_events` UInt64 DEFAULT 0, `success_count` UInt64 DEFAULT 0, `failure_count` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(window_start)) ORDER BY (org, window_start, capability_name, refreshed_at) SETTINGS index_granularity = 8192;
