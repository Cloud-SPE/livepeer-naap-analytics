-- Uniqueness test on api_hourly_byoc_auth_store. After Phase 6.2 the resolver
-- writes one row per (org, window_start, capability_name, refresh_run_id);
-- duplicates at that grain indicate a bug in insertHourlyBYOCAuth's GROUP BY.

select
    org,
    window_start,
    capability_name,
    refresh_run_id,
    count(*) as row_count
from naap.api_hourly_byoc_auth_store
group by 1, 2, 3, 4
having count(*) > 1
