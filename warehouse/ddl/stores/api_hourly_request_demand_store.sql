-- Canonical DDL for naap.api_hourly_request_demand_store.
-- Source of truth: this file. The resolver writes rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- warehouse/tests/test_store_ddl_drift.sql.
--
-- Phase 6.1 of serving-layer-v2: promotes api_hourly_request_demand from
-- an on-demand view that rescanned canonical_ai_batch_jobs +
-- canonical_byoc_jobs every request to a pre-aggregated store the resolver
-- writes per window slice. Read path is now one MergeTree primary-key
-- lookup plus a latest-slice argMax, mirroring the Phase 1 SLA promotion.

CREATE TABLE IF NOT EXISTS naap.api_hourly_request_demand_store (`window_start` DateTime('UTC'), `org` LowCardinality(String), `gateway` String, `execution_mode` LowCardinality(String) DEFAULT 'request', `capability_family` LowCardinality(String), `capability_name` String, `capability_id` Nullable(UInt16), `canonical_pipeline` Nullable(String), `pipeline_id` String, `canonical_model` Nullable(String), `orchestrator_address` String DEFAULT '', `orchestrator_uri` String DEFAULT '', `job_count` UInt64 DEFAULT 0, `selected_count` UInt64 DEFAULT 0, `no_orch_count` UInt64 DEFAULT 0, `success_count` UInt64 DEFAULT 0, `duration_ms_sum` Int64 DEFAULT 0, `price_sum` Float64 DEFAULT 0, `llm_request_count` UInt64 DEFAULT 0, `llm_success_count` UInt64 DEFAULT 0, `llm_total_tokens_sum` Int64 DEFAULT 0, `llm_total_tokens_sample_count` UInt64 DEFAULT 0, `llm_tokens_per_second_sum` Float64 DEFAULT 0, `llm_tokens_per_second_sample_count` UInt64 DEFAULT 0, `llm_ttft_ms_sum` Float64 DEFAULT 0, `llm_ttft_ms_sample_count` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(window_start)) ORDER BY (org, window_start, gateway, execution_mode, capability_family, capability_name, ifNull(canonical_model, ''), orchestrator_address, orchestrator_uri, refreshed_at) SETTINGS index_granularity = 8192;
