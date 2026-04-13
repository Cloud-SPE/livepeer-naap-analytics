-- Hourly SLA by orchestrator for non-streaming job types (ai-batch and byoc).
-- Streaming SLA is already covered by api_sla_compliance.

select
    toStartOfHour(event_ts)          as window_start,
    org,
    orchestrator_uri                 as orchestrator_uri,
    pipeline                         as pipeline_id,
    model                            as model_id,
    gpu_id,
    job_type,
    count()                          as job_count,
    countIf(success = 1)             as success_count,
    countIf(success = 1) / nullIf(toFloat64(count()), 0) as success_rate,
    avg(toFloat64(duration_ms))      as avg_duration_ms,
    (countIf(success = 1) / nullIf(toFloat64(count()), 0)) * 100.0 as sla_score
from {{ ref('canonical_requests_jobs') }}
where job_type != 'stream'
  and event_ts  is not null
group by
    window_start,
    org,
    orchestrator_uri,
    pipeline_id,
    model_id,
    gpu_id,
    job_type
