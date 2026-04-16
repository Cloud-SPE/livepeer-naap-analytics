select
    event_id as observation_event_id,
    event_ts as last_seen,
    org,
    capability as capability_name,
    orch_address,
    orch_url as orchestrator_url,
    orch_url_norm as orchestrator_uri,
    worker_url,
    price_per_unit,
    model,
    worker_options_raw
from {{ ref('canonical_byoc_workers') }}
