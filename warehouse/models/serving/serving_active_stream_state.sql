{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['org', 'state', 'sample_ts']
) }}

-- Materialized table: rebuilt on dbt incremental schedule (~5 min cadence).
-- Bound to 24h by sample_ts to cap table size. Recency filtering (e.g. last 30s)
-- is the responsibility of each API consumer, not this model.
with latest_sample as (
    select
        canonical_session_key,
        argMax(event_id, event_ts) as event_id,
        argMax(event_ts, event_ts) as sample_ts,
        argMax(org, event_ts) as org,
        argMax(stream_id, event_ts) as stream_id,
        argMax(request_id, event_ts) as request_id,
        argMax(gateway, event_ts) as gateway,
        argMax(state, event_ts) as state,
        argMax(output_fps, event_ts) as output_fps,
        argMax(input_fps, event_ts) as input_fps,
        argMax(e2e_latency_ms, event_ts) as e2e_latency_ms
    from {{ ref('stg_ai_stream_status') }}
    where canonical_session_key != ''
    group by canonical_session_key
)
select
    s.event_id,
    s.sample_ts,
    s.org,
    coalesce(fs.stream_id, s.stream_id) as stream_id,
    coalesce(fs.request_id, s.request_id) as request_id,
    s.gateway,
    fs.canonical_pipeline as pipeline,
    fs.canonical_model as model_id,
    fs.attributed_orch_address as orch_address,
    fs.attributed_orch_uri as orch_uri,
    fs.attribution_status,
    fs.attribution_reason,
    s.state,
    s.output_fps,
    s.input_fps,
    s.e2e_latency_ms,
    fs.started_at,
    fs.last_seen,
    fs.completed
from latest_sample s
left join {{ ref('fact_workflow_sessions') }} fs on s.canonical_session_key = fs.canonical_session_key
where s.sample_ts > now() - interval 24 hour
  and fs.completed = 0
