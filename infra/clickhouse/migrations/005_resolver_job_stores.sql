-- Migration 005: Resolver-owned job attribution stores for AI batch and BYOC.
--
-- These tables are the single source of truth for attributed job records.
-- The Go resolver writes to them; dbt models read from them as thin views.
-- Canonical read layers must collapse them deterministically to one latest row
-- per logical job key before any downstream rollup reads them.
--
-- Both tables use ReplacingMergeTree(materialized_at) so that a later resolver
-- run for the same job key (re-attribution after new capability data arrives)
-- automatically supersedes the prior row on background merge or with FINAL.

CREATE TABLE IF NOT EXISTS naap.canonical_ai_batch_job_store
(
    -- Job identity
    `request_id`              String,
    `org`                     LowCardinality(String),
    `gateway`                 String,
    `pipeline`                LowCardinality(String),
    `model_id`                String,

    -- Job lifecycle timestamps
    `received_at`             Nullable(DateTime64(3, 'UTC')),
    `completed_at`            DateTime64(3, 'UTC'),

    -- Job outcome
    `success`                 Nullable(UInt8),
    `tries`                   UInt16,
    `duration_ms`             Int64,

    -- Orchestrator identity (always from completed event)
    `orch_url`                String,
    `orch_url_norm`           String,

    -- Pricing and quality
    `latency_score`           Float64,
    `price_per_unit`          Float64,
    `error_type`              String,
    `error`                   String,

    -- Attribution outputs
    `attribution_status`      LowCardinality(String),
    `attribution_reason`      LowCardinality(String),
    `attribution_method`      LowCardinality(String),
    `attribution_confidence`  LowCardinality(String),
    `attributed_orch_uri`     Nullable(String),
    `capability_version_id`   Nullable(String),
    `attribution_snapshot_ts` Nullable(DateTime64(3, 'UTC')),

    -- Hardware (null when hardware_less or unresolved)
    `gpu_id`                  Nullable(String),
    `gpu_model_name`          Nullable(String),
    `gpu_memory_bytes_total`  Nullable(UInt64),
    `attributed_model`        Nullable(String),

    -- Resolver bookkeeping
    `resolver_run_id`         String       DEFAULT '',
    `materialized_at`         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(materialized_at)
PARTITION BY (org, toYYYYMM(completed_at))
ORDER BY (org, request_id, completed_at)
TTL toDateTime(completed_at) + toIntervalDay(90)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS naap.canonical_byoc_job_store
(
    -- Job identity (event_id is the stable key; request_id is always empty for BYOC)
    `event_id`                String,
    `org`                     LowCardinality(String),
    `gateway`                 String,
    `capability`              String,

    -- Job lifecycle
    `completed_at`            DateTime64(3, 'UTC'),

    -- Job outcome
    `success`                 Nullable(UInt8),
    `duration_ms`             Int64,
    `http_status`             UInt16,

    -- Orchestrator identity
    `orch_address`            String,
    `orch_url`                String,
    `orch_url_norm`           String,
    `worker_url`              String,
    `charged_compute`         UInt8,
    `error`                   String,

    -- Model resolved from worker_lifecycle (primary) or CI canonical_model (fallback)
    `model`                   Nullable(String),
    `price_per_unit`          Float64,

    -- Attribution outputs
    `attribution_status`      LowCardinality(String),
    `attribution_reason`      LowCardinality(String),
    `attribution_method`      LowCardinality(String),
    `attribution_confidence`  LowCardinality(String),
    `attributed_orch_uri`     Nullable(String),
    `capability_version_id`   Nullable(String),
    `attribution_snapshot_ts` Nullable(DateTime64(3, 'UTC')),

    -- Hardware (null when hardware_less or unresolved)
    `gpu_id`                  Nullable(String),
    `gpu_model_name`          Nullable(String),
    `gpu_memory_bytes_total`  Nullable(UInt64),

    -- Resolver bookkeeping
    `resolver_run_id`         String       DEFAULT '',
    `materialized_at`         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(materialized_at)
PARTITION BY (org, toYYYYMM(completed_at))
ORDER BY (org, event_id, completed_at)
TTL toDateTime(completed_at) + toIntervalDay(90)
SETTINGS index_granularity = 8192;
