select
    event_ts as failure_ts,
    org,
    gateway,
    stream_id,
    request_id,
    coalesce(orch_raw_address, '') as orch_address,
    raw_pipeline_hint as pipeline,
    trace_type as failure_type,
    data as detail
from {{ ref('stg_stream_trace') }}
where trace_type in ('gateway_no_orchestrators_available', 'orchestrator_swap')

union all

select
    event_ts as failure_ts,
    org,
    gateway,
    stream_id,
    request_id,
    coalesce(orch_raw_address, '') as orch_address,
    raw_pipeline_hint as pipeline,
    if(restart_count > 0, 'inference_restart', 'inference_error') as failure_type,
    data as detail
from {{ ref('stg_ai_stream_status') }}
where restart_count > 0
   or last_error not in ('', 'null')
