-- Unified view of all three job types: streaming sessions, AI-batch jobs, BYOC jobs.
-- Provides a common schema for cross-type analytics endpoints.

with stream_gateway as (
    select
        canonical_session_key,
        anyIf(gateway, gateway != '') as gateway
    from {{ ref('stg_stream_trace') }}
    where canonical_session_key != ''
    group by canonical_session_key
),

streams as (
    select
        s.canonical_session_key                                   as job_id,
        'stream'                                                  as job_type,
        s.org                                                     as org,
        ifNull(sg.gateway, '')                                    as gateway,
        s.canonical_pipeline                                      as pipeline,
        s.canonical_model                                         as model,
        ifNull(s.attributed_orch_address, '')                     as orch_address,
        ifNull(s.attributed_orch_uri, '')                         as orch_url_norm,
        ifNull(s.gpu_id, '')                                      as gpu_id,
        ''                                                        as gpu_model_name,
        if(s.startup_outcome = 'success', toUInt8(1), toUInt8(0)) as success,
        toInt64(coalesce(s.e2e_latency_ms, 0))                   as duration_ms,
        coalesce(s.started_at, s.last_seen)                      as event_ts,
        toFloat64(0)                                              as price_per_unit
    from {{ ref('canonical_session_current') }} s
    left join stream_gateway sg
        on s.canonical_session_key = sg.canonical_session_key
    where s.canonical_session_key != ''
      and s.canonical_pipeline    != ''
),

ai_batch as (
    select
        request_id                                                as job_id,
        'ai-batch'                                               as job_type,
        org                                                       as org,
        ifNull(gateway, '')                                       as gateway,
        pipeline                                                  as pipeline,
        model_id                                                  as model,
        ifNull(orch_url_norm, '')                                 as orch_address,
        ifNull(orch_url_norm, '')                                 as orch_url_norm,
        ifNull(gpu_id, '')                                        as gpu_id,
        ifNull(gpu_model_name, '')                                as gpu_model_name,
        toUInt8(coalesce(success, 0))                             as success,
        toInt64(coalesce(duration_ms, 0))                         as duration_ms,
        coalesce(completed_at, received_at)                       as event_ts,
        toFloat64(coalesce(price_per_unit, 0))                   as price_per_unit
    from {{ ref('canonical_ai_batch_jobs') }}
    where request_id != ''
      and pipeline   != ''
),

byoc as (
    select
        request_id                                                as job_id,
        'byoc'                                                    as job_type,
        org                                                       as org,
        ifNull(gateway, '')                                       as gateway,
        capability                                                as pipeline,
        ifNull(model, '')                                         as model,
        ifNull(orch_address, '')                                  as orch_address,
        ifNull(orch_url_norm, '')                                 as orch_url_norm,
        ifNull(gpu_id, '')                                        as gpu_id,
        ifNull(gpu_model_name, '')                                as gpu_model_name,
        toUInt8(coalesce(success, 0))                             as success,
        toInt64(coalesce(duration_ms, 0))                         as duration_ms,
        coalesce(completed_at, submitted_at)                      as event_ts,
        toFloat64(coalesce(price_per_unit, 0))                   as price_per_unit
    from {{ ref('canonical_byoc_jobs') }}
    where capability != ''
)

select * from streams
union all
select * from ai_batch
union all
select * from byoc
