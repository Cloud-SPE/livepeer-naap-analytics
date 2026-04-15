with expected as (
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
        count() as expected_job_count,
        countIf(selection_outcome = 'selected') as expected_selected_count,
        countIf(selection_outcome = 'no_orch') as expected_no_orch_count,
        countIf(success = 1) as expected_success_count,
        sum(toInt64(coalesce(duration_ms, 0))) as expected_duration_ms_sum,
        sum(toFloat64(coalesce(price_per_unit, 0))) as expected_price_sum
    from {{ ref('canonical_ai_batch_jobs') }}
    where request_id != ''
      and pipeline != ''
    group by window_start, org, gateway, execution_mode, capability_family, capability_name, capability_id, canonical_pipeline, canonical_model, orchestrator_address, orchestrator_uri

    union all

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
        count() as expected_job_count,
        countIf(selection_outcome = 'selected') as expected_selected_count,
        countIf(selection_outcome = 'no_orch') as expected_no_orch_count,
        countIf(success = 1) as expected_success_count,
        sum(toInt64(coalesce(duration_ms, 0))) as expected_duration_ms_sum,
        sum(toFloat64(coalesce(price_per_unit, 0))) as expected_price_sum
    from {{ ref('canonical_byoc_jobs') }}
    where request_id != ''
      and capability != ''
    group by window_start, org, gateway, execution_mode, capability_family, capability_name, capability_id, canonical_pipeline, canonical_model, orchestrator_address, orchestrator_uri
),
actual as (
    select
        window_start,
        org,
        gateway,
        execution_mode,
        capability_family,
        capability_name,
        capability_id,
        canonical_pipeline,
        canonical_model,
        orchestrator_address,
        orchestrator_uri,
        job_count,
        selected_count,
        no_orch_count,
        success_count,
        duration_ms_sum,
        price_sum
    from {{ ref('api_hourly_request_demand') }}
)
select
    coalesce(e.window_start, a.window_start) as window_start,
    coalesce(e.org, a.org) as org,
    coalesce(e.gateway, a.gateway) as gateway,
    coalesce(e.execution_mode, a.execution_mode) as execution_mode,
    coalesce(e.capability_family, a.capability_family) as capability_family,
    coalesce(e.capability_name, a.capability_name) as capability_name,
    coalesce(e.capability_id, a.capability_id) as capability_id,
    coalesce(e.canonical_pipeline, a.canonical_pipeline) as canonical_pipeline,
    coalesce(e.canonical_model, a.canonical_model) as canonical_model,
    coalesce(e.orchestrator_address, a.orchestrator_address) as orchestrator_address,
    coalesce(e.orchestrator_uri, a.orchestrator_uri) as orchestrator_uri,
    e.expected_job_count,
    a.job_count,
    e.expected_selected_count,
    a.selected_count,
    e.expected_no_orch_count,
    a.no_orch_count,
    e.expected_success_count,
    a.success_count,
    e.expected_duration_ms_sum,
    a.duration_ms_sum,
    e.expected_price_sum,
    a.price_sum
from expected e
full outer join actual a
    on e.window_start = a.window_start
   and e.org = a.org
   and e.gateway = a.gateway
   and e.execution_mode = a.execution_mode
   and e.capability_family = a.capability_family
   and e.capability_name = a.capability_name
   and ifNull(e.capability_id, 0) = ifNull(a.capability_id, 0)
   and ifNull(e.canonical_pipeline, '') = ifNull(a.canonical_pipeline, '')
   and ifNull(e.canonical_model, '') = ifNull(a.canonical_model, '')
   and e.orchestrator_address = a.orchestrator_address
   and e.orchestrator_uri = a.orchestrator_uri
where ifNull(e.expected_job_count, 0) != ifNull(a.job_count, 0)
   or ifNull(e.expected_selected_count, 0) != ifNull(a.selected_count, 0)
   or ifNull(e.expected_no_orch_count, 0) != ifNull(a.no_orch_count, 0)
   or ifNull(e.expected_success_count, 0) != ifNull(a.success_count, 0)
   or ifNull(e.expected_duration_ms_sum, 0) != ifNull(a.duration_ms_sum, 0)
   or abs(ifNull(e.expected_price_sum, 0.0) - ifNull(a.price_sum, 0.0)) > 1e-9
