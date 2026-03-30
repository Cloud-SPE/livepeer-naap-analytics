with ens as (
    select eth_address, argMax(name, updated_at) as ens_name
    from naap.orch_metadata final
    group by eth_address
),
caps as (
    select
        orch_address,
        argMaxIfMerge(orch_name) as orch_name,
        argMaxIfMerge(orch_uri) as uri,
        argMaxIfMerge(version) as version,
        any(org)              as org,
        maxMerge(snapshot_ts) as last_seen,
        argMaxIfMerge(raw_capabilities) as raw_capabilities
    from naap.canonical_capability_snapshot_latest
    group by orch_address
)
select
    c.orch_address,
    coalesce(nullIf(e.ens_name, ''), c.orch_name) as name,
    c.uri,
    c.version,
    c.org,
    c.last_seen,
    c.raw_capabilities
from caps c
left join ens e on lower(c.orch_address) = lower(e.eth_address)
