with ens as (
    select eth_address, argMax(name, updated_at) as ens_name
    from naap.orch_metadata final
    group by eth_address
),
caps as (
    select
        orch_address,
        argMax(orch_name, snapshot_ts)        as orch_name,
        argMax(orch_uri, snapshot_ts)         as uri,
        argMax(version, snapshot_ts)          as version,
        argMax(org, snapshot_ts)              as org,
        max(snapshot_ts)                      as last_seen,
        argMax(raw_capabilities, snapshot_ts) as raw_capabilities
    from {{ ref('capability_snapshots') }}
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
