select
    org,
    capability as capability_name,
    orch_address,
    orch_url_norm as orchestrator_uri,
    worker_url,
    argMax(model, event_ts) as model,
    argMax(price_per_unit, event_ts) as price_per_unit,
    max(event_ts) as last_seen
from {{ ref('canonical_byoc_workers') }}
group by org, capability, orch_address, orchestrator_uri, worker_url
