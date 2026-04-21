with ens as (
    select
        eth_address,
        argMax(name, updated_at) as ens_name
    from naap.orch_metadata final
    group by eth_address
),
latest_nodes as (
    select
        org,
        orch_address,
        orch_uri_norm,
        argMaxIfMerge(snapshot_row_id) as capability_version_id,
        argMaxIfMerge(source_event_id) as snapshot_event_id,
        maxMerge(snapshot_ts) as last_seen,
        argMaxIfMerge(orch_name) as orch_name,
        argMaxIfMerge(orch_uri) as orchestrator_uri,
        argMaxIfMerge(version) as version
    from naap.canonical_capability_snapshot_latest
    group by
        org,
        orch_address,
        orch_uri_norm
),
latest_orchestrators as (
    select
        t.orch_address,
        min(n.org) as org,
        min(n.capability_version_id) as capability_version_id,
        min(n.snapshot_event_id) as snapshot_event_id,
        t.last_seen,
        min(n.orch_name) as orch_name,
        min(n.orchestrator_uri) as orchestrator_uri,
        min(n.orch_uri_norm) as orch_uri_norm,
        min(n.version) as version
    from (
        select
            orch_address,
            max(last_seen) as last_seen
        from latest_nodes
        group by orch_address
    ) t
    inner join latest_nodes n
        on n.orch_address = t.orch_address
       and n.last_seen = t.last_seen
    group by
        t.orch_address,
        t.last_seen
),
named as (
    select
        l.capability_version_id,
        l.snapshot_event_id,
        l.orch_address,
        coalesce(nullIf(e.ens_name, ''), nullIf(l.orch_name, ''), l.orch_address) as name,
        coalesce(nullIf(e.ens_name, ''), nullIf(l.orch_name, ''), l.orch_address) as orch_name,
        l.orchestrator_uri,
        l.orch_uri_norm,
        l.version,
        l.org,
        l.last_seen
    from latest_orchestrators l
    left join ens e
        on lower(l.orch_address) = lower(e.eth_address)
),
name_collisions as (
    select
        lower(name) as name_key,
        count() as name_collision_count
    from named
    group by lower(name)
)
select
    n.capability_version_id,
    n.snapshot_event_id,
    n.orch_address,
    n.name,
    n.orch_name,
    n.orchestrator_uri,
    n.orch_uri_norm,
    n.version,
    n.org,
    n.last_seen,
    if(
        ifNull(n.name, '') != '' and not startsWith(lower(n.name), '0x'),
        if(
            ifNull(c.name_collision_count, 0) > 1,
            concat(n.name, ' (', left(n.orch_address, 8), '...', right(n.orch_address, 4), ')'),
            n.name
        ),
        concat(left(n.orch_address, 8), '...', right(n.orch_address, 4))
    ) as orch_label
from named n
left join name_collisions c
    on lower(n.name) = c.name_key
