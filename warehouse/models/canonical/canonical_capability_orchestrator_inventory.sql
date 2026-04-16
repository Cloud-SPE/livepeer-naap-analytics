with ens as (
    select
        eth_address,
        argMax(name, updated_at) as ens_name
    from naap.orch_metadata final
    group by eth_address
)
select distinct
    s.snapshot_row_id as capability_version_id,
    s.source_event_id as snapshot_event_id,
    s.snapshot_ts as last_seen,
    s.org,
    s.orch_address,
    coalesce(nullIf(e.ens_name, ''), nullIf(s.orch_name, ''), s.orch_address) as name,
    coalesce(nullIf(e.ens_name, ''), nullIf(s.orch_name, ''), s.orch_address) as orch_name,
    s.orch_uri as orchestrator_uri,
    s.orch_uri as orch_uri,
    s.orch_uri_norm,
    s.version,
    s.raw_capabilities
from {{ ref('canonical_capability_snapshots') }} s
left join ens e
    on lower(s.orch_address) = lower(e.eth_address)
