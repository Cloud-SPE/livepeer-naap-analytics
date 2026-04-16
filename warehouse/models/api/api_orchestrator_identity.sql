select
    capability_version_id,
    snapshot_event_id,
    orch_address,
    name,
    orch_name,
    orch_label,
    orchestrator_uri,
    orch_uri_norm,
    version,
    org,
    last_seen
from {{ ref('canonical_capability_orchestrator_identity_latest') }}
