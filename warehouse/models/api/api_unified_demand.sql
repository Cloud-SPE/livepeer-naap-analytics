-- Hourly demand aggregation for non-streaming job types (ai-batch and byoc).
-- Streaming demand is already covered by api_network_demand.

select
    toStartOfHour(event_ts)          as window_start,
    org,
    gateway,
    pipeline                         as pipeline_id,
    model                            as model_id,
    job_type,
    count()                          as job_count,
    countIf(success = 1)             as success_count,
    countIf(success = 1) / nullIf(toFloat64(count()), 0) as success_rate,
    avg(toFloat64(duration_ms))      as avg_duration_ms,
    sum(toFloat64(duration_ms)) / 60000.0 as total_minutes
from {{ ref('canonical_unified_jobs') }}
where job_type != 'stream'
  and event_ts  is not null
group by
    window_start,
    org,
    gateway,
    pipeline_id,
    model_id,
    job_type
