-- One row per BYOC auth event (job_auth family).
-- Canonical source for per-capability auth success/failure rates.

select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    capability,
    subtype,
    orch_address,
    orch_url,
    orch_url_norm,
    success,
    error
from {{ ref('stg_byoc_auth') }}
where capability != ''
