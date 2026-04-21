select
    capability_version_id,
    snapshot_event_id,
    orch_address,
    name,
    orch_name,
    orchestrator_uri,
    orch_uri,
    orch_uri_norm,
    version,
    org,
    last_seen,
    raw_capabilities
from {{ ref('canonical_capability_orchestrator_inventory') }}

