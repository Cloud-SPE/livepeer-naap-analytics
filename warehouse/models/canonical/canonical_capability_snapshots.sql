select
    snapshot_row_id,
    source_event_id,
    snapshot_ts,
    org,
    orch_address,
    orch_name,
    orch_uri,
    orch_uri_norm,
    version,
    raw_capabilities
from naap.canonical_capability_snapshots_store
