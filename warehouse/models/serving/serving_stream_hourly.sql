{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=["ifNull(org, '')", 'hour', "ifNull(pipeline, '')"],
    partition_by='toYYYYMM(hour)'
) }}

select
    toStartOfHour(coalesce(f.started_at, f.last_seen)) as hour,
    f.org,
    f.canonical_pipeline as pipeline,
    ifNull(f.gateway, '') as gateway,
    countIf(f.started = 1) as started,
    countIf(f.startup_outcome = 'success') as completed,
    countIf(f.no_orch = 1) as no_orch,
    countIf(f.swap_count > 0) as orch_swap,
    toFloat64(avgIf(f.startup_latency_ms, f.startup_latency_ms > 0)) as avg_startup_latency_ms,
    toFloat64(quantileTDigestIf(0.95)(f.startup_latency_ms, f.startup_latency_ms > 0)) as p95_startup_latency_ms
from {{ ref('fact_workflow_sessions') }} f
where f.canonical_session_key != ''
  and toYear(coalesce(f.started_at, f.last_seen)) > 2000
group by hour, f.org, pipeline, gateway
