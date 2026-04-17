-- Canonical DDL for naap.canonical_sla_benchmark_daily_store.
-- Source of truth: this file. The resolver writes rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.
--
-- Pre-computed daily SLA benchmark cohort: one row per (pipeline_id,
-- model_id, cohort_date, refresh_run_id) with scalar p50/p90/p10
-- percentiles over the day's avg_prompt_to_first_frame_ms,
-- avg_e2e_latency_ms, and avg_output_fps observations.
--
-- Phase 2 of serving-layer-v2 — replaces the view-based
-- api_base_sla_quality_cohort_daily_state whose AggregateFunction states
-- forced a 7-day arrayJoin + quantileTDigestIfMerge on every scoring
-- pass. With this store, the resolver's SLA scoring reads one row per
-- cohort day (7 per scored grain) instead of re-aggregating the whole
-- input surface every time.

CREATE TABLE IF NOT EXISTS naap.canonical_sla_benchmark_daily_store (`cohort_date` Date, `pipeline_id` String, `model_id` Nullable(String), `ptff_p50` Nullable(Float64), `ptff_p90` Nullable(Float64), `ptff_row_count` UInt64 DEFAULT 0, `e2e_p50` Nullable(Float64), `e2e_p90` Nullable(Float64), `e2e_row_count` UInt64 DEFAULT 0, `fps_p10` Nullable(Float64), `fps_p50` Nullable(Float64), `fps_row_count` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = ReplacingMergeTree(refreshed_at) PARTITION BY toYYYYMM(cohort_date) ORDER BY (pipeline_id, ifNull(model_id, ''), cohort_date, refresh_run_id) SETTINGS index_granularity = 8192;
