select
    orch_address,
    name,
    name as orch_name,
    uri as orchestrator_uri,
    version,
    org,
    last_seen,
    raw_capabilities
from {{ ref('canonical_latest_orchestrator_state') }}
