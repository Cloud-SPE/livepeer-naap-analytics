{{ config(
    engine='MergeTree()',
    order_by=['org', 'hour', 'canonical_session_key'],
    partition_by='toYYYYMM(hour)'
) }}

with attributed_status as (
    select
        s.canonical_session_key,
        s.org,
        s.event_ts,
        toStartOfHour(s.event_ts) as hour,
        coalesce(fs.stream_id, s.stream_id) as stream_id,
        coalesce(fs.request_id, s.request_id) as request_id,
        fs.canonical_pipeline,
        fs.canonical_model,
        fs.started_at,
        fs.last_seen as session_last_seen,
        fs.attributed_orch_address as orch_address,
        fs.attributed_orch_uri as orch_uri,
        fs.attribution_status,
        fs.attribution_reason,
        s.state,
        s.output_fps,
        s.input_fps,
        s.e2e_latency_ms,
        s.restart_count,
        s.last_error
    from {{ ref('stg_ai_stream_status') }} s
    inner join {{ ref('fact_workflow_sessions') }} fs on s.canonical_session_key = fs.canonical_session_key
    where s.canonical_session_key != ''
),
base_hours as (
    select
        canonical_session_key,
        org,
        hour,
        any(stream_id) as stream_id,
        any(request_id) as request_id,
        any(canonical_pipeline) as canonical_pipeline,
        any(canonical_model) as canonical_model,
        any(orch_address) as orch_address,
        any(orch_uri) as orch_uri,
        any(attribution_status) as attribution_status,
        any(attribution_reason) as attribution_reason,
        any(started_at) as started_at,
        any(session_last_seen) as session_last_seen,
        count() as status_samples,
        countIf(output_fps > 0) as fps_positive_samples,
        countIf({{ running_state_expr("state") }}) as running_state_samples,
        countIf(state = 'DEGRADED_INPUT') as degraded_input_samples,
        countIf(state = 'DEGRADED_INFERENCE') as degraded_inference_samples,
        countIf(last_error not in ('', 'null')) as error_samples,
        avg(output_fps) as avg_output_fps,
        avg(input_fps) as avg_input_fps,
        avgIf(e2e_latency_ms, e2e_latency_ms > 0) as avg_e2e_latency_ms
    from attributed_status
    group by canonical_session_key, org, hour
),
with_prev as (
    select
        b.*,
        ifNull(p.status_samples, 0) as prev_status_samples,
        ifNull(p.fps_positive_samples, 0) as prev_fps_positive_samples,
        ifNull(p.running_state_samples, 0) as prev_running_state_samples
    from base_hours b
    left join base_hours p
      on b.canonical_session_key = p.canonical_session_key
     and p.hour = b.hour - interval 1 hour
)
select
    *,
    toUInt8(
        started_at < hour
        and session_last_seen >= hour
        and session_last_seen < hour + interval 1 hour
        and fps_positive_samples = 0
        and running_state_samples = 0
        and status_samples < 3
        and prev_fps_positive_samples = 0
        and prev_running_state_samples = 0
        and prev_status_samples < 3
    ) as is_terminal_tail_artifact
from with_prev
