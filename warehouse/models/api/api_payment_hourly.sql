with {{ latest_value_cte('latest_slices', 'naap.api_payment_hourly_store', ['org', 'hour'], 'refresh_run_id') }}
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
    on {{ join_on_columns('s', 'l', ['org', 'hour']) }}
   and s.refresh_run_id = l.refresh_run_id
