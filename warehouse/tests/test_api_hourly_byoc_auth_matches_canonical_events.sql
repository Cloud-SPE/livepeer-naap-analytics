with expected as (
    select
        toStartOfHour(event_ts) as window_start,
        org,
        capability as capability_name,
        count() as expected_total_events,
        countIf(success = 1) as expected_success_count,
        countIf(success = 0) as expected_failure_count
    from {{ ref('canonical_byoc_auth') }}
    where capability != ''
    group by window_start, org, capability_name
),
actual as (
    select
        window_start,
        org,
        capability_name,
        total_events,
        success_count,
        failure_count
    from {{ ref('api_hourly_byoc_auth') }}
)
select
    coalesce(e.window_start, a.window_start) as window_start,
    coalesce(e.org, a.org) as org,
    coalesce(e.capability_name, a.capability_name) as capability_name,
    e.expected_total_events,
    a.total_events,
    e.expected_success_count,
    a.success_count,
    e.expected_failure_count,
    a.failure_count
from expected e
full outer join actual a
    on e.window_start = a.window_start
   and e.org = a.org
   and e.capability_name = a.capability_name
where ifNull(e.expected_total_events, 0) != ifNull(a.total_events, 0)
   or ifNull(e.expected_success_count, 0) != ifNull(a.success_count, 0)
   or ifNull(e.expected_failure_count, 0) != ifNull(a.failure_count, 0)
