-- Phase 6.2: read pre-aggregated rows from api_hourly_byoc_auth_store.
-- The resolver writes one row per (org, window_start, capability_name) on
-- every refresh run; this view picks the latest slice via argMax, so callers
-- see idempotent output even across partial re-runs.

{{ config(materialized='view') }}

with latest_slices as (
    select
        org,
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_hourly_byoc_auth_store
    group by org, window_start
)
select
    s.window_start,
    s.org,
    s.capability_name,
    s.total_events,
    s.success_count,
    s.failure_count
from naap.api_hourly_byoc_auth_store as s
inner join latest_slices as l
    on  s.org = l.org
    and s.window_start = l.window_start
    and s.refresh_run_id = l.refresh_run_id
