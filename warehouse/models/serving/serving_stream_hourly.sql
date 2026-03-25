select
    toStartOfHour(coalesce(started_at, last_seen)) as hour,
    org,
    canonical_pipeline as pipeline,
    countIf(started = 1) as started,
    countIf(startup_outcome = 'success') as completed,
    countIf(no_orch = 1) as no_orch,
    countIf(swap_count > 0) as orch_swap
from {{ ref('fact_workflow_sessions') }}
where canonical_session_key != ''
group by hour, org, pipeline
