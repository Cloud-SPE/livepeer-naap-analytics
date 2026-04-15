select
    toStartOfHour(event_ts) as window_start,
    org,
    capability as capability_name,
    toUInt64(count()) as total_events,
    toUInt64(countIf(success = 1)) as success_count,
    toUInt64(countIf(success = 0)) as failure_count
from {{ ref('canonical_byoc_auth') }}
where capability != ''
group by window_start, org, capability_name
