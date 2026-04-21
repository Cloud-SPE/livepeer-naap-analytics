-- Phase 2 of serving-layer-v2: create canonical_sla_benchmark_daily_store.
-- This is the store that replaces api_base_sla_quality_cohort_daily_state —
-- scalar p50/p90/p10 values per (pipeline_id, model_id, cohort_date) so
-- the resolver's scoring pass reads O(7) benchmark rows per input row
-- instead of re-aggregating the entire input surface via arrayJoin +
-- quantileTDigestIfMerge.
--
-- Safe to run on fresh stacks (bootstrap has this table too); safe to
-- re-run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS naap.canonical_sla_benchmark_daily_store (`cohort_date` Date, `pipeline_id` String, `model_id` Nullable(String), `ptff_p50` Nullable(Float64), `ptff_p90` Nullable(Float64), `ptff_row_count` UInt64 DEFAULT 0, `e2e_p50` Nullable(Float64), `e2e_p90` Nullable(Float64), `e2e_row_count` UInt64 DEFAULT 0, `fps_p10` Nullable(Float64), `fps_p50` Nullable(Float64), `fps_row_count` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = ReplacingMergeTree(refreshed_at) PARTITION BY toYYYYMM(cohort_date) ORDER BY (pipeline_id, ifNull(model_id, ''), cohort_date, refresh_run_id) SETTINGS index_granularity = 8192;
