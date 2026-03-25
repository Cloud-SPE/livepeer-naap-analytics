select
    orch_address,
    argMax(orch_name, snapshot_ts) as name,
    argMax(orch_uri, snapshot_ts) as uri,
    argMax(version, snapshot_ts) as version,
    argMax(org, snapshot_ts) as org,
    max(snapshot_ts) as last_seen,
    argMax(raw_capabilities, snapshot_ts) as raw_capabilities
from {{ ref('capability_snapshots') }}
group by orch_address
