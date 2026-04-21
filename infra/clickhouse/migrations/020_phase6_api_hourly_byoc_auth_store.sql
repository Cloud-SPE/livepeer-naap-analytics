-- Phase 6.2 of serving-layer-v2: promote api_hourly_byoc_auth from an
-- on-demand view over canonical_byoc_auth to a resolver-written store,
-- matching the _hourly family convention (Phase 6.1 did the same for
-- request_demand). Read path becomes a primary-key lookup + latest-slice
-- argMax; the resolver writes per window slice.
--
-- Safe to run on fresh stacks (bootstrap has this table too); safe to
-- re-run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS naap.api_hourly_byoc_auth_store (`window_start` DateTime('UTC'), `org` LowCardinality(String), `capability_name` String, `total_events` UInt64 DEFAULT 0, `success_count` UInt64 DEFAULT 0, `failure_count` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(window_start)) ORDER BY (org, window_start, capability_name, refreshed_at) SETTINGS index_granularity = 8192;
