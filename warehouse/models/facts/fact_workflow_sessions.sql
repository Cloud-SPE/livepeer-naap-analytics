with trace_rollup as (
    select
        canonical_session_key,
        any(org) as org,
        argMaxIf(stream_id, event_ts, stream_id != '') as stream_id,
        argMaxIf(request_id, event_ts, request_id != '') as request_id,
        argMaxIf(pipeline_id, event_ts, pipeline_id != '') as pipeline_id,
        argMaxIf(raw_pipeline_hint, event_ts, raw_pipeline_hint != '') as raw_pipeline_hint,
        minIf(event_ts, trace_type = 'gateway_receive_stream_request') as started_at,
        maxIf(1, trace_type = 'gateway_receive_stream_request') as started,
        maxIf(1, trace_type = 'gateway_receive_few_processed_segments') as playable_seen,
        maxIf(1, trace_type = 'gateway_no_orchestrators_available') as no_orch,
        maxIf(1, trace_type = 'gateway_ingest_stream_closed') as completed,
        countIf(trace_type = 'orchestrator_swap') as swap_count,
        max(event_ts) as trace_last_seen
    from {{ ref('stg_stream_trace') }}
    where canonical_session_key != ''
    group by canonical_session_key
),
status_rollup as (
    select
        canonical_session_key,
        any(org) as org,
        argMaxIf(stream_id, event_ts, stream_id != '') as stream_id,
        argMaxIf(request_id, event_ts, request_id != '') as request_id,
        argMaxIf(raw_pipeline_hint, event_ts, raw_pipeline_hint != '') as canonical_pipeline,
        maxIf(1, restart_count > 0) as restart_seen,
        maxIf(1, last_error not in ('', 'null')) as error_seen,
        maxIf(1, state = 'DEGRADED_INPUT') as degraded_input_seen,
        maxIf(1, state = 'DEGRADED_INFERENCE') as degraded_inference_seen,
        count() as status_sample_count,
        countIf(last_error not in ('', 'null')) as status_error_sample_count,
        maxIf(1, state = 'ONLINE') as online_seen,
        maxIf(1, output_fps > 0) as positive_output_seen,
        countIf(state in ('ONLINE', 'DEGRADED_INPUT', 'DEGRADED_INFERENCE')) as running_state_samples,
        max(event_ts) as status_last_seen
    from {{ ref('stg_ai_stream_status') }}
    where canonical_session_key != ''
    group by canonical_session_key
),
orch_observations as (
    select canonical_session_key, event_ts, orch_raw_address as orch_address, orch_url
    from {{ ref('stg_stream_trace') }}
    where canonical_session_key != ''
      and (orch_raw_address != '' or orch_url != '')
    union all
    select canonical_session_key, event_ts, orch_raw_address as orch_address, orch_url
    from {{ ref('stg_ai_stream_status') }}
    where canonical_session_key != ''
      and (orch_raw_address != '' or orch_url != '')
    union all
    select canonical_session_key, event_ts, orch_raw_address as orch_address, orch_url
    from {{ ref('stg_ai_stream_events') }}
    where canonical_session_key != ''
      and (orch_raw_address != '' or orch_url != '')
),
orch_rollup as (
    select
        canonical_session_key,
        argMaxIf(orch_address, event_ts, orch_address != '') as observed_orch_address,
        argMaxIf(orch_url, event_ts, orch_url != '') as observed_orch_url,
        uniqExactIf(orch_address, orch_address != '') as observed_orch_address_count,
        uniqExactIf(lower(orch_url), orch_url != '') as observed_orch_url_count
    from orch_observations
    group by canonical_session_key
),
event_rollup as (
    select
        canonical_session_key,
        argMaxIf(raw_pipeline_hint, event_ts, raw_pipeline_hint != '') as event_pipeline_hint,
        maxIf(1, message != '') as any_event_message,
        max(event_ts) as event_last_seen
    from {{ ref('stg_ai_stream_events') }}
    where canonical_session_key != ''
    group by canonical_session_key
),
base as (
    select
        coalesce(nullIf(t.canonical_session_key, ''), nullIf(s.canonical_session_key, ''), nullIf(e.canonical_session_key, '')) as canonical_session_key,
        coalesce(nullIf(t.org, ''), nullIf(s.org, '')) as org,
        coalesce(nullIf(t.stream_id, ''), nullIf(s.stream_id, '')) as stream_id,
        coalesce(nullIf(t.request_id, ''), nullIf(s.request_id, '')) as request_id,
        coalesce(nullIf(s.canonical_pipeline, ''), nullIf(e.event_pipeline_hint, ''), nullIf(t.pipeline_id, ''), nullIf(t.raw_pipeline_hint, '')) as canonical_pipeline,
        nullIf(o.observed_orch_address, '') as observed_orch_address,
        nullIf(o.observed_orch_url, '') as observed_orch_url,
        lower(nullIf(o.observed_orch_url, '')) as observed_orch_url_norm,
        toUInt64(ifNull(o.observed_orch_address_count, 0)) as observed_orch_address_count,
        toUInt64(ifNull(o.observed_orch_url_count, 0)) as observed_orch_url_count,
        t.started_at,
        greatest(ifNull(t.trace_last_seen, toDateTime64(0, 3, 'UTC')), ifNull(s.status_last_seen, toDateTime64(0, 3, 'UTC')), ifNull(e.event_last_seen, toDateTime64(0, 3, 'UTC'))) as last_seen,
        toUInt8(ifNull(t.started, 0)) as started,
        toUInt8(ifNull(t.playable_seen, 0)) as playable_seen,
        toUInt8(ifNull(t.no_orch, 0)) as no_orch,
        toUInt8(ifNull(t.completed, 0)) as completed,
        toUInt64(ifNull(t.swap_count, 0)) as swap_count,
        toUInt8(ifNull(s.restart_seen, 0)) as restart_seen,
        toUInt8(ifNull(s.error_seen, 0)) as error_seen,
        toUInt8(ifNull(s.degraded_input_seen, 0)) as degraded_input_seen,
        toUInt8(ifNull(s.degraded_inference_seen, 0)) as degraded_inference_seen,
        toUInt64(ifNull(s.status_sample_count, 0)) as status_sample_count,
        toUInt64(ifNull(s.status_error_sample_count, 0)) as status_error_sample_count,
        toUInt8(ifNull(s.status_sample_count, 0) > 0 and ifNull(s.running_state_samples, 0) = 0 and ifNull(s.online_seen, 0) = 0) as loading_only_session,
        toUInt8(ifNull(s.status_sample_count, 0) > 0 and ifNull(s.positive_output_seen, 0) = 0) as zero_output_fps_session
    from trace_rollup t
    full outer join status_rollup s on t.canonical_session_key = s.canonical_session_key
    full outer join event_rollup e on coalesce(nullIf(t.canonical_session_key, ''), nullIf(s.canonical_session_key, '')) = e.canonical_session_key
    left join orch_rollup o on coalesce(nullIf(t.canonical_session_key, ''), nullIf(s.canonical_session_key, ''), nullIf(e.canonical_session_key, '')) = o.canonical_session_key
),
capability_snapshots_norm as (
    select
        snapshot_ts,
        orch_address,
        orch_uri,
        lower(orch_uri) as orch_uri_norm,
        raw_capabilities
    from {{ ref('capability_snapshots') }}
),
capability_match_by_address as (
    select
        b.canonical_session_key as canonical_session_key,
        argMaxIf(
            c.orch_address,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_orch_address,
        argMaxIf(
            c.orch_uri,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_orch_uri,
        argMaxIf(
            c.raw_capabilities,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_raw_capabilities,
        nullIf(
            maxIf(
                c.snapshot_ts,
                c.snapshot_ts <= b.last_seen + interval 60 second
                and c.snapshot_ts >= b.last_seen - interval 15 minute
            ),
            toDateTime64(0, 3, 'UTC')
        ) as matched_snapshot_ts
    from base b
    left join capability_snapshots_norm c
      on b.observed_orch_address is not null
     and b.observed_orch_address != ''
     and c.orch_address = b.observed_orch_address
    group by b.canonical_session_key
),
capability_match_by_url as (
    select
        b.canonical_session_key as canonical_session_key,
        argMaxIf(
            c.orch_address,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_orch_address,
        argMaxIf(
            c.orch_uri,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_orch_uri,
        argMaxIf(
            c.raw_capabilities,
            c.snapshot_ts,
            c.snapshot_ts <= b.last_seen + interval 60 second
            and c.snapshot_ts >= b.last_seen - interval 15 minute
        ) as matched_raw_capabilities,
        nullIf(
            maxIf(
                c.snapshot_ts,
                c.snapshot_ts <= b.last_seen + interval 60 second
                and c.snapshot_ts >= b.last_seen - interval 15 minute
            ),
            toDateTime64(0, 3, 'UTC')
        ) as matched_snapshot_ts
    from base b
    left join capability_snapshots_norm c
      on (b.observed_orch_address is null or b.observed_orch_address = '')
     and b.observed_orch_url_norm != ''
     and c.orch_uri_norm = b.observed_orch_url_norm
    group by b.canonical_session_key
),
capability_match as (
    select
        b.canonical_session_key as canonical_session_key,
        coalesce(a.matched_orch_address, u.matched_orch_address) as matched_orch_address,
        coalesce(a.matched_orch_uri, u.matched_orch_uri) as matched_orch_uri,
        coalesce(a.matched_raw_capabilities, u.matched_raw_capabilities) as matched_raw_capabilities,
        coalesce(a.matched_snapshot_ts, u.matched_snapshot_ts) as matched_snapshot_ts
    from base b
    left join capability_match_by_address a on b.canonical_session_key = a.canonical_session_key
    left join capability_match_by_url u on b.canonical_session_key = u.canonical_session_key
),
stale_capability_match_by_address as (
    select
        b.canonical_session_key as canonical_session_key,
        nullIf(max(c.snapshot_ts), toDateTime64(0, 3, 'UTC')) as stale_snapshot_ts
    from base b
    left join capability_snapshots_norm c
      on b.observed_orch_address is not null
     and b.observed_orch_address != ''
     and c.orch_address = b.observed_orch_address
    group by b.canonical_session_key
),
stale_capability_match_by_url as (
    select
        b.canonical_session_key as canonical_session_key,
        nullIf(max(c.snapshot_ts), toDateTime64(0, 3, 'UTC')) as stale_snapshot_ts
    from base b
    left join capability_snapshots_norm c
      on (b.observed_orch_address is null or b.observed_orch_address = '')
     and b.observed_orch_url_norm != ''
     and c.orch_uri_norm = b.observed_orch_url_norm
    group by b.canonical_session_key
),
stale_capability_match as (
    select
        b.canonical_session_key as canonical_session_key,
        coalesce(a.stale_snapshot_ts, u.stale_snapshot_ts) as stale_snapshot_ts
    from base b
    left join stale_capability_match_by_address a on b.canonical_session_key = a.canonical_session_key
    left join stale_capability_match_by_url u on b.canonical_session_key = u.canonical_session_key
)
select
    b.canonical_session_key as canonical_session_key,
    b.org,
    b.stream_id,
    b.request_id,
    b.canonical_pipeline,
    cast(null as Nullable(String)) as canonical_model,
    b.started_at,
    b.last_seen,
    b.started,
    b.playable_seen,
    b.no_orch,
    b.completed,
    b.swap_count,
    b.restart_seen,
    b.error_seen,
    b.degraded_input_seen,
    b.degraded_inference_seen,
    b.status_sample_count,
    b.status_error_sample_count,
    b.loading_only_session,
    b.zero_output_fps_session,
    toUInt64(
        (b.status_sample_count > 0) +
        b.playable_seen +
        toUInt8(b.status_sample_count > 0 and (b.loading_only_session = 0 or b.zero_output_fps_session = 0))
    ) as health_signal_count,
    toUInt64(if(b.started = 1, 3, 0)) as health_expected_signal_count,
    if(b.started = 1, toFloat64(
        (b.status_sample_count > 0) +
        b.playable_seen +
        toUInt8(b.status_sample_count > 0 and (b.loading_only_session = 0 or b.zero_output_fps_session = 0))
    ) / 3.0, 1.0) as health_signal_coverage_ratio,
    {{ canonical_startup_outcome("b.playable_seen", "b.no_orch", "b.started") }} as startup_outcome,
    toUInt8(
        greatest(b.observed_orch_address_count, b.observed_orch_url_count) > 1
        and b.swap_count = 0
    ) as has_ambiguous_identity,
    toUInt8(cm.matched_snapshot_ts is not null) as has_snapshot_match,
    toUInt8(
        cm.matched_snapshot_ts is not null
        and length(JSONExtractArrayRaw(ifNull(cm.matched_raw_capabilities, '{}'), 'hardware')) = 0
    ) as is_hardware_less,
    toUInt8(
        cm.matched_snapshot_ts is null
        and sm.stale_snapshot_ts is not null
        and coalesce(b.observed_orch_address, b.observed_orch_url, '') != ''
    ) as is_stale,
    {{ attribution_reason_expr(
        "toUInt8(cm.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(cm.matched_raw_capabilities, '{}'), 'hardware')) > 0 and greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1)",
        "toUInt8(greatest(b.observed_orch_address_count, b.observed_orch_url_count) > 1 and b.swap_count = 0)",
        "toUInt8(cm.matched_snapshot_ts is null and sm.stale_snapshot_ts is not null and coalesce(b.observed_orch_address, b.observed_orch_url, '') != '')",
        "toUInt8(cm.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(cm.matched_raw_capabilities, '{}'), 'hardware')) = 0 and greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1)"
    ) }} as attribution_reason,
    {{ attribution_status_expr(
        "toUInt8(cm.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(cm.matched_raw_capabilities, '{}'), 'hardware')) > 0 and greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1)",
        "toUInt8(greatest(b.observed_orch_address_count, b.observed_orch_url_count) > 1 and b.swap_count = 0)",
        "toUInt8(cm.matched_snapshot_ts is null and sm.stale_snapshot_ts is not null and coalesce(b.observed_orch_address, b.observed_orch_url, '') != '')",
        "toUInt8(cm.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(cm.matched_raw_capabilities, '{}'), 'hardware')) = 0 and greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1)"
    ) }} as attribution_status,
    coalesce(nullIf(cm.matched_orch_address, ''), b.observed_orch_address) as attributed_orch_address,
    nullIf(cm.matched_orch_uri, '') as attributed_orch_uri,
    cm.matched_snapshot_ts as attribution_snapshot_ts
from base b
left join capability_match cm on b.canonical_session_key = cm.canonical_session_key
left join stale_capability_match sm on b.canonical_session_key = sm.canonical_session_key
where b.canonical_session_key != ''
