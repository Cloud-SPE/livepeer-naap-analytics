-- One row per BYOC payment event.
-- Stores the typed fields extracted from job_payment events.
-- No hardware attribution — payments are financial records, not job records.

select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    capability,
    orch_address,
    amount,
    currency,
    payment_type
from {{ ref('stg_byoc_payments') }}
where event_id != ''
