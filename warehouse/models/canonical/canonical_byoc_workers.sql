-- One row per (org, capability, orch_address, event_ts) worker registration.
-- Used as the canonical source for BYOC worker inventory lookups.
-- argMax-based joins over this model collapse to the latest known worker/model
-- state per capability+orch_address pair (see canonical_byoc_jobs.sql).

select
    event_id,
    event_ts,
    org,
    gateway,
    capability,
    orch_address,
    orch_url,
    orch_url_norm,
    worker_url,
    price_per_unit,
    model,
    worker_options_raw
from {{ ref('stg_worker_lifecycle') }}
where capability != ''
  and orch_address != ''
