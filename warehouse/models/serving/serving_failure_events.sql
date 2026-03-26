select
    s.event_ts as failure_ts,
    s.org,
    s.gateway,
    s.stream_id,
    s.request_id,
    -- Prefer canonical attributed address; fall back to raw hot-wallet as provenance only
    coalesce(nullIf(fs.attributed_orch_address, ''), coalesce(s.orch_raw_address, '')) as orch_address,
    fs.attributed_orch_uri as orch_uri,
    coalesce(nullIf(fs.canonical_pipeline, ''), s.raw_pipeline_hint) as pipeline,
    fs.canonical_model as model_id,
    s.trace_type as failure_type,
    s.data as detail
from {{ ref('stg_stream_trace') }} s
left join {{ ref('fact_workflow_sessions') }} fs on s.canonical_session_key = fs.canonical_session_key
where s.trace_type in ('gateway_no_orchestrators_available', 'orchestrator_swap')

union all

select
    s.event_ts as failure_ts,
    s.org,
    s.gateway,
    s.stream_id,
    s.request_id,
    coalesce(nullIf(fs.attributed_orch_address, ''), coalesce(s.orch_raw_address, '')) as orch_address,
    fs.attributed_orch_uri as orch_uri,
    coalesce(nullIf(fs.canonical_pipeline, ''), s.raw_pipeline_hint) as pipeline,
    fs.canonical_model as model_id,
    if(s.restart_count > 0, 'inference_restart', 'inference_error') as failure_type,
    s.data as detail
from {{ ref('stg_ai_stream_status') }} s
left join {{ ref('fact_workflow_sessions') }} fs on s.canonical_session_key = fs.canonical_session_key
where s.restart_count > 0
   or s.last_error not in ('', 'null')
