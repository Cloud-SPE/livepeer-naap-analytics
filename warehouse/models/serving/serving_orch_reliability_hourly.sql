select
    hour,
    org,
    ifNull(orch_address, '') as orch_address,
    canonical_pipeline as pipeline,
    uniqExact(canonical_session_key) as ai_stream_count,
    uniqExactIf(canonical_session_key, degraded_input_samples > 0 or degraded_inference_samples > 0) as degraded_count,
    uniqExactIf(canonical_session_key, degraded_input_samples > 0) as degraded_input_count,
    uniqExactIf(canonical_session_key, degraded_inference_samples > 0) as degraded_inference_count,
    uniqExactIf(canonical_session_key, fs.restart_seen = 1) as restart_count,
    uniqExactIf(canonical_session_key, error_samples > 0) as error_count,
    uniqExactIf(canonical_session_key, attribution_status = 'resolved') as resolved_session_count,
    uniqExactIf(canonical_session_key, attribution_status = 'hardware_less') as hardware_less_session_count,
    uniqExactIf(canonical_session_key, attribution_status = 'stale') as stale_session_count,
    uniqExactIf(canonical_session_key, attribution_status = 'ambiguous') as ambiguous_session_count,
    uniqExactIf(canonical_session_key, attribution_status = 'unresolved') as unresolved_session_count,
    sum(error_samples) as error_status_samples
from {{ ref('fact_workflow_status_hours') }}
left join {{ ref('fact_workflow_sessions') }} fs using (canonical_session_key)
where is_terminal_tail_artifact = 0
group by hour, org, orch_address, pipeline
