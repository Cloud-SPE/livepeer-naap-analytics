select
    row_id as snapshot_row_id,
    event_id as source_event_id,
    event_ts as snapshot_ts,
    org,
    orch_address,
    orch_name,
    orch_uri,
    version,
    raw_capabilities
from naap.normalized_network_capabilities
where row_id != ''
