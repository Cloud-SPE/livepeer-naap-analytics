with ai_batch as (
    select
        toStartOfHour(coalesce(completed_at, received_at)) as window_start,
        org,
        ifNull(gateway, '') as gateway,
        'request' as execution_mode,
        'builtin' as capability_family,
        pipeline as capability_name,
        cast(null, 'Nullable(UInt16)') as capability_id,
        cast(pipeline, 'Nullable(String)') as canonical_pipeline,
        cast(model_id, 'Nullable(String)') as canonical_model,
        cast('', 'String') as orchestrator_address,
        ifNull(orch_url_norm, '') as orchestrator_uri,
        toUInt64(count()) as job_count,
        toUInt64(countIf(selection_outcome = 'selected')) as selected_count,
        toUInt64(countIf(selection_outcome = 'no_orch')) as no_orch_count,
        toUInt64(countIf(success = 1)) as success_count,
        toInt64(sum(toInt64(coalesce(duration_ms, 0)))) as duration_ms_sum,
        toFloat64(sum(toFloat64(coalesce(price_per_unit, 0)))) as price_sum
    from {{ ref('canonical_ai_batch_jobs') }}
    where request_id != ''
      and pipeline != ''
    group by window_start, org, gateway, execution_mode, capability_family, capability_name, canonical_pipeline, canonical_model, orchestrator_address, orchestrator_uri
),
byoc as (
    select
        toStartOfHour(coalesce(completed_at, submitted_at)) as window_start,
        org,
        ifNull(gateway, '') as gateway,
        'request' as execution_mode,
        'byoc' as capability_family,
        capability as capability_name,
        cast(37, 'Nullable(UInt16)') as capability_id,
        cast(null, 'Nullable(String)') as canonical_pipeline,
        cast(model, 'Nullable(String)') as canonical_model,
        ifNull(orch_address, '') as orchestrator_address,
        ifNull(orch_url_norm, '') as orchestrator_uri,
        toUInt64(count()) as job_count,
        toUInt64(countIf(selection_outcome = 'selected')) as selected_count,
        toUInt64(countIf(selection_outcome = 'no_orch')) as no_orch_count,
        toUInt64(countIf(success = 1)) as success_count,
        toInt64(sum(toInt64(coalesce(duration_ms, 0)))) as duration_ms_sum,
        toFloat64(sum(toFloat64(coalesce(price_per_unit, 0)))) as price_sum
    from {{ ref('canonical_byoc_jobs') }}
    where request_id != ''
      and capability != ''
    group by window_start, org, gateway, execution_mode, capability_family, capability_name, capability_id, canonical_pipeline, canonical_model, orchestrator_address, orchestrator_uri
)
select * from ai_batch
union all
select * from byoc
