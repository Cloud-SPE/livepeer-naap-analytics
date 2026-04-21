with expected as (
    select
        toStartOfHour(event_ts) as window_start,
        org,
        capability,
        payment_type,
        count() as expected_payment_count,
        sum(toFloat64(amount)) as expected_total_amount,
        argMax(currency, (event_ts, event_id)) as expected_currency,
        uniqExact(orch_address) as expected_unique_orchs
    from {{ ref('canonical_byoc_payments') }}
    where capability != ''
    group by window_start, org, capability, payment_type
),
actual as (
    select
        window_start,
        org,
        capability,
        payment_type,
        payment_count,
        total_amount,
        currency,
        unique_orchs
    from {{ ref('api_hourly_byoc_payments') }}
)
select
    coalesce(e.window_start, a.window_start) as window_start,
    coalesce(e.org, a.org) as org,
    coalesce(e.capability, a.capability) as capability,
    coalesce(e.payment_type, a.payment_type) as payment_type,
    e.expected_payment_count,
    a.payment_count,
    e.expected_total_amount,
    a.total_amount,
    e.expected_currency,
    a.currency,
    e.expected_unique_orchs,
    a.unique_orchs
from expected e
full outer join actual a
    on e.window_start = a.window_start
   and e.org = a.org
   and e.capability = a.capability
   and e.payment_type = a.payment_type
where ifNull(e.expected_payment_count, 0) != ifNull(a.payment_count, 0)
   or abs(ifNull(e.expected_total_amount, 0.0) - ifNull(a.total_amount, 0.0)) > 1e-9
   or ifNull(e.expected_currency, '') != ifNull(a.currency, '')
   or ifNull(e.expected_unique_orchs, 0) != ifNull(a.unique_orchs, 0)
