with latest_slices as (
    select
        org,
        hour,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_payment_hourly_store
    group by org, hour
)
select
    s.hour,
    s.org,
    s.pipeline,
    s.orch_address,
    s.total_wei,
    s.event_count,
    s.avg_price_wei_per_pixel
from naap.api_payment_hourly_store s
inner join latest_slices l
    on s.org = l.org
   and s.hour = l.hour
   and s.refresh_run_id = l.refresh_run_id
