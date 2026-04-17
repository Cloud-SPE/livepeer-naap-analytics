-- Phase 6.3 of serving-layer-v2: new denormalized store carrying one
-- row per orch_address (org-agnostic; orchestrators are physical nodes
-- shared across orgs) with identity + capability membership + GPU count +
-- latest 24h of SLA/reliability, so the three orchestrator-listing
-- endpoints (/v1/streaming/orchestrators, /v1/requests/orchestrators,
-- dashboard-orchestrators) can each do a single MergeTree scan instead
-- of a 3–4 query fan-out rejoined in Go.
--
-- Safe to run on fresh stacks (bootstrap has this table too); safe to
-- re-run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS naap.api_current_orchestrator_store (`orch_address` String, `orchestrator_uri` String DEFAULT '', `orch_name` String DEFAULT '', `orch_label` String DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `gpu_count` UInt64 DEFAULT 0, `streaming_models` Array(String) DEFAULT [], `request_capability_pairs` Array(Tuple(String, String)) DEFAULT [], `pipelines` Array(String) DEFAULT [], `pipeline_model_pairs` Array(Tuple(String, String)) DEFAULT [], `known_sessions_count` UInt64 DEFAULT 0, `success_sessions` UInt64 DEFAULT 0, `requested_sessions` UInt64 DEFAULT 0, `effective_success_rate` Nullable(Float64), `no_swap_rate` Nullable(Float64), `latest_sla_score` Nullable(Float64), `latest_sla_window_start` Nullable(DateTime('UTC')), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (orch_address, refreshed_at) SETTINGS index_granularity = 8192;
