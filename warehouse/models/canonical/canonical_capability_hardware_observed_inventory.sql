with catalog as (
    select *
    from {{ ref('canonical_capability_catalog') }}
),
hardware as (
    select *
    from naap.canonical_capability_hardware_inventory
),
snapshots as (
    select
        snapshot_row_id,
        argMax(source_event_id, snapshot_ts) as source_event_id,
        argMax(orch_uri, snapshot_ts) as orch_uri
    from naap.canonical_capability_snapshots_store
    group by snapshot_row_id
)
select
    h.snapshot_row_id as capability_version_id,
    s.source_event_id as snapshot_event_id,
    h.org,
    h.orch_address,
    ifNull(s.orch_uri, '') as orchestrator_uri,
    h.orch_uri_norm,
    h.snapshot_ts as last_seen,
    cast(ifNull(c.capability_id, toUInt16(37)), 'Nullable(UInt16)') as capability_id,
    cast(ifNull(c.capability_name, 'byoc'), 'Nullable(String)') as capability_name,
    cast(ifNull(c.capability_family, 'byoc'), 'Nullable(String)') as capability_family,
    cast(nullIf(h.pipeline_id, ''), 'Nullable(String)') as canonical_pipeline,
    cast(nullIf(h.pipeline_id, ''), 'Nullable(String)') as offered_name,
    cast(nullIf(h.model_id, ''), 'Nullable(String)') as model_id,
    h.gpu_id,
    h.gpu_model_name,
    h.gpu_memory_bytes_total,
    toUInt8(ifNull(c.supports_stream, 1)) as supports_stream,
    toUInt8(ifNull(c.supports_request, 1)) as supports_request
from hardware h
left join snapshots s
    on s.snapshot_row_id = h.snapshot_row_id
left join catalog c
    on lower(h.pipeline_id) = lower(c.canonical_pipeline)
where h.gpu_id != ''
