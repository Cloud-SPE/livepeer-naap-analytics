{{ config(
    engine='MergeTree()',
    order_by=["ifNull(org, '')", "ifNull(canonical_session_key, '')"]
) }}

with trace_rollup as (
    select
        canonical_session_key,
        any(org) as org,
        argMaxIf(stream_id, event_ts, stream_id != '') as stream_id,
        argMaxIf(request_id, event_ts, request_id != '') as request_id,
        argMaxIf(pipeline_id, event_ts, pipeline_id != '') as pipeline_id,
        argMaxIf(raw_pipeline_hint, event_ts, raw_pipeline_hint != '') as raw_pipeline_hint,
        anyIf(gateway, gateway != '') as gateway,
        minIf(event_ts, trace_type = 'gateway_receive_stream_request') as started_at,
        -- Exact moment the gateway sent the first ingest segment to the selected orch.
        -- This is when orch selection occurred, making it the authoritative anchor for
        -- point-in-time capability enrichment. Null when no trace event present.
        nullIf(
            minIf(event_ts, trace_type = 'gateway_send_first_ingest_segment'),
            toDateTime64(0, 3, 'UTC')
        ) as orch_selected_at,
        nullIf(
            minIf(event_ts, trace_type = 'gateway_receive_first_processed_segment'),
            toDateTime64(0, 3, 'UTC')
        ) as first_segment_at,
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
        nullIf(t.gateway, '') as gateway,
        nullIf(o.observed_orch_address, '') as observed_orch_address,
        nullIf(o.observed_orch_url, '') as observed_orch_url,
        lower(nullIf(o.observed_orch_url, '')) as observed_orch_url_norm,
        toUInt64(ifNull(o.observed_orch_address_count, 0)) as observed_orch_address_count,
        toUInt64(ifNull(o.observed_orch_url_count, 0)) as observed_orch_url_count,
        t.started_at as started_at,
        t.orch_selected_at as orch_selected_at,
        t.first_segment_at as first_segment_at,
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
-- Existence check: which orch addresses appear in capability_snapshots at all.
-- Used by cap_stale to distinguish "no snapshot before selection" from "orch not in snapshot system".
orch_in_snapshots as (
    select orch_address
    from {{ ref('capability_snapshots') }}
    group by orch_address
),
-- Point-in-time capability join via ASOF LEFT JOIN.
-- Finds the last snapshot at or before orch selection time — the snapshot the gateway
-- acted on when selecting this orch. Falls back to last_seen when orch_selected_at is
-- unavailable (sessions with no gateway trace data).
-- capability_snapshots ORDER BY (orch_address, snapshot_ts) satisfies ASOF JOIN requirements.
-- URL-only fallback omitted: all observed events carry orch_address; URL-only path is dead code.
-- ClickHouse ASOF LEFT JOIN miss fills String columns with '' — guard on cs.orch_address != ''.
cap_match as (
    select
        b.canonical_session_key,
        b.canonical_pipeline as raw_hint_pipeline,
        b.playable_seen,
        b.started,
        b.no_orch,
        b.observed_orch_address,
        b.observed_orch_url,
        b.orch_selected_at,
        if(cs.orch_address != '', cs.orch_address,      null) as matched_orch_address,
        if(cs.orch_address != '', cs.orch_uri,          null) as matched_orch_uri,
        if(cs.orch_address != '', cs.raw_capabilities,  null) as matched_raw_capabilities,
        if(cs.orch_address != '', cs.snapshot_ts,       null) as matched_snapshot_ts
    from base b
    asof left join {{ ref('capability_snapshots') }} cs
        on cs.orch_address = coalesce(b.observed_orch_address, '')
        and cs.snapshot_ts <= coalesce(b.orch_selected_at, b.last_seen)
),
-- Stale detection requires a regular LEFT JOIN against orch_in_snapshots.
-- Kept as a separate CTE to avoid mixing ASOF and regular JOINs in the same FROM clause.
-- is_stale = 1 when: orch is observed, ASOF JOIN missed (no snapshot before selection),
-- but the orch does exist in the snapshot system (registered capabilities at some point).
cap_stale as (
    select
        cm.*,
        toUInt8(
            cm.observed_orch_address is not null
            and cm.matched_snapshot_ts is null
            and length(ois.orch_address) > 0
        ) as is_stale
    from cap_match cm
    left join orch_in_snapshots ois
        on coalesce(cm.observed_orch_address, '') = ois.orch_address
),
-- Resolve canonical pipeline and model from the matched point-in-time capability snapshot.
-- Uses ClickHouse alias-in-SELECT chaining (hw_entries → cap_pipeline_names → canonical_pipeline)
-- to avoid extra CTEs. References cap_stale only — base is not re-joined here.
cap_resolved as (
    select
        canonical_session_key,
        observed_orch_address,
        observed_orch_url,
        orch_selected_at,
        matched_orch_address,
        matched_orch_uri,
        matched_raw_capabilities,
        matched_snapshot_ts,
        is_stale,
        JSONExtractArrayRaw(ifNull(matched_raw_capabilities, '{}'), 'hardware') as hw_entries,
        arrayMap(x -> JSONExtractString(x, 'pipeline'), hw_entries) as cap_pipeline_names,
        multiIf(
            matched_snapshot_ts is not null
            and length(hw_entries) > 0
            and has(cap_pipeline_names, raw_hint_pipeline),
            raw_hint_pipeline,
            matched_snapshot_ts is not null and length(hw_entries) = 1,
            cap_pipeline_names[1],
            raw_hint_pipeline
        ) as canonical_pipeline,
        nullIf(
            JSONExtractString(
                arrayElement(
                    arrayFilter(x -> JSONExtractString(x, 'pipeline') = canonical_pipeline, hw_entries),
                    1
                ),
                'model_id'
            ),
            ''
        ) as canonical_model
    from cap_stale
)
select
    b.canonical_session_key as canonical_session_key,
    b.org,
    b.stream_id,
    b.request_id,
    r.canonical_pipeline as canonical_pipeline,
    b.gateway,
    r.canonical_model,
    b.started_at,
    r.orch_selected_at as orch_selected_at,
    b.first_segment_at,
    -- Startup latency: gateway_receive_stream_request → gateway_receive_first_processed_segment.
    -- Null when either timestamp is missing or the delta is implausible (> 5 min).
    nullIf(
        if(
            b.started_at is not null
            and b.first_segment_at is not null
            and b.first_segment_at > b.started_at
            and dateDiff('millisecond', b.started_at, b.first_segment_at) < 300000,
            toFloat64(dateDiff('millisecond', b.started_at, b.first_segment_at)),
            0.0
        ),
        0.0
    ) as startup_latency_ms,
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
    toUInt8(r.matched_snapshot_ts is not null) as has_snapshot_match,
    toUInt8(
        r.matched_snapshot_ts is not null
        and length(JSONExtractArrayRaw(ifNull(r.matched_raw_capabilities, '{}'), 'hardware')) = 0
    ) as is_hardware_less,
    r.is_stale,
    -- resolved/hardware_less allow orch_count > 1 when swap_count > 0: an explicit swap event
    -- explains multiple observed orches. ambiguous is only when orch_count > 1 with no swap.
    {{ attribution_reason_expr(
        "toUInt8(r.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(r.matched_raw_capabilities, '{}'), 'hardware')) > 0 and (greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1 or b.swap_count > 0))",
        "toUInt8(greatest(b.observed_orch_address_count, b.observed_orch_url_count) > 1 and b.swap_count = 0)",
        "r.is_stale",
        "toUInt8(r.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(r.matched_raw_capabilities, '{}'), 'hardware')) = 0 and (greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1 or b.swap_count > 0))"
    ) }} as attribution_reason,
    {{ attribution_status_expr(
        "toUInt8(r.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(r.matched_raw_capabilities, '{}'), 'hardware')) > 0 and (greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1 or b.swap_count > 0))",
        "toUInt8(greatest(b.observed_orch_address_count, b.observed_orch_url_count) > 1 and b.swap_count = 0)",
        "r.is_stale",
        "toUInt8(r.matched_snapshot_ts is not null and length(JSONExtractArrayRaw(ifNull(r.matched_raw_capabilities, '{}'), 'hardware')) = 0 and (greatest(b.observed_orch_address_count, b.observed_orch_url_count) <= 1 or b.swap_count > 0))"
    ) }} as attribution_status,
    coalesce(nullIf(r.matched_orch_address, ''), b.observed_orch_address) as attributed_orch_address,
    nullIf(r.matched_orch_uri, '') as attributed_orch_uri,
    r.matched_snapshot_ts as attribution_snapshot_ts
from base b
left join cap_resolved r on b.canonical_session_key = r.canonical_session_key
where b.canonical_session_key != ''
