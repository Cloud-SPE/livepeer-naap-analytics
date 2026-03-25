select
    s.canonical_session_key,
    s.event_id,
    s.event_ts as sample_ts,
    s.org,
    coalesce(fs.stream_id, s.stream_id) as stream_id,
    coalesce(fs.request_id, s.request_id) as request_id,
    s.gateway,
    fs.attributed_orch_address as orch_address,
    fs.canonical_pipeline as pipeline,
    fs.canonical_model as model_id,
    fs.attribution_status,
    fs.attribution_reason,
    s.state,
    s.output_fps,
    s.input_fps,
    s.e2e_latency_ms,
    fs.attribution_status = 'resolved' as is_attributed
from {{ ref('stg_ai_stream_status') }} s
left join {{ ref('fact_workflow_sessions') }} fs on s.canonical_session_key = fs.canonical_session_key
