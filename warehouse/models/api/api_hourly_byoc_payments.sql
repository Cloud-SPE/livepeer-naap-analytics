-- Reads pre-aggregated rows from api_hourly_byoc_payments_store. The
-- resolver writes one row per (org, window_start, capability, payment_type)
-- on every refresh run; this view picks the latest slice via argMax so
-- callers see idempotent output even across partial re-runs. Backs the
-- public Jobs dashboard's exact settled-payment panels.

{{ config(materialized='view') }}

with latest_slices as (
    select
        org,
        window_start,
        capability,
        payment_type,
        argMax(refresh_run_id, (refreshed_at, refresh_run_id)) as refresh_run_id
    from naap.api_hourly_byoc_payments_store
    group by org, window_start, capability, payment_type
)
select
    s.window_start,
    s.org,
    s.capability,
    s.payment_type,
    s.payment_count,
    s.total_amount,
    s.currency,
    s.unique_orchs
from naap.api_hourly_byoc_payments_store as s
inner join latest_slices as l
    on  s.org = l.org
    and s.window_start = l.window_start
    and s.capability = l.capability
    and s.payment_type = l.payment_type
    and s.refresh_run_id = l.refresh_run_id
