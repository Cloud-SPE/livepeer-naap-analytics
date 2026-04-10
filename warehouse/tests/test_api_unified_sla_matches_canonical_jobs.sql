with expected as (
    select
        toStartOfHour(event_ts) as window_start,
        org,
        orchestrator_uri,
        pipeline as pipeline_id,
        model as model_id,
        gpu_id,
        job_type,
        count() as expected_job_count,
        countIf(success = 1) as expected_success_count
    from {{ ref('canonical_unified_jobs') }}
    where job_type != 'stream'
      and event_ts is not null
    group by window_start, org, orchestrator_uri, pipeline_id, model_id, gpu_id, job_type
),
actual as (
    select
        window_start,
        org,
        orchestrator_uri,
        pipeline_id,
        model_id,
        gpu_id,
        job_type,
        job_count,
        success_count
    from {{ ref('api_unified_sla') }}
)
select
    coalesce(e.window_start, a.window_start) as window_start,
    coalesce(e.org, a.org) as org,
    coalesce(e.orchestrator_uri, a.orchestrator_uri) as orchestrator_uri,
    coalesce(e.pipeline_id, a.pipeline_id) as pipeline_id,
    coalesce(e.model_id, a.model_id) as model_id,
    coalesce(e.gpu_id, a.gpu_id) as gpu_id,
    coalesce(e.job_type, a.job_type) as job_type,
    e.expected_job_count,
    a.job_count,
    e.expected_success_count,
    a.success_count
from expected e
full outer join actual a
    on e.window_start = a.window_start
   and e.org = a.org
   and e.orchestrator_uri = a.orchestrator_uri
   and e.pipeline_id = a.pipeline_id
   and ifNull(e.model_id, '') = ifNull(a.model_id, '')
   and ifNull(e.gpu_id, '') = ifNull(a.gpu_id, '')
   and e.job_type = a.job_type
where ifNull(e.expected_job_count, 0) != ifNull(a.job_count, 0)
   or ifNull(e.expected_success_count, 0) != ifNull(a.success_count, 0)
