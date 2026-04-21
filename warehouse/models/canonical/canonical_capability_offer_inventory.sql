with catalog as (
    select *
    from {{ ref('canonical_capability_catalog') }}
),
inventory as (
    select *
    from naap.canonical_capability_offer_inventory_store
)
select
    i.snapshot_row_id as capability_version_id,
    i.source_event_id as snapshot_event_id,
    i.org,
    i.orch_address,
    i.orch_uri as orchestrator_uri,
    i.orch_uri_norm,
    i.snapshot_ts as last_seen,
    cast(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.capability_id, toUInt16(37)),
            ifNull(i.capability_id, toUInt16(0))
        ),
        'Nullable(UInt16)'
    ) as capability_id,
    cast(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.capability_name, 'byoc'),
            c_id.capability_name
        ),
        'Nullable(String)'
    ) as capability_name,
    cast(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.capability_family, 'byoc'),
            c_id.capability_family
        ),
        'Nullable(String)'
    ) as capability_family,
    cast(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.canonical_pipeline, i.offered_name),
            c_id.canonical_pipeline
        ),
        'Nullable(String)'
    ) as canonical_pipeline,
    cast(
        if(i.hardware_present = 1, i.offered_name, c_id.capability_name),
        'Nullable(String)'
    ) as offered_name,
    -- Phase 4 unified capability spine: one pipeline_id per row so handlers
    -- no longer have to branch on capability_family. byoc rows carry the
    -- external capability_name verbatim; builtin rows use canonical_pipeline
    -- with a capability_name fallback for entries the catalog has not
    -- backfilled yet.
    if(
        if(i.hardware_present = 1,
           ifNull(c_pipeline.capability_family, 'byoc'),
           c_id.capability_family) = 'byoc',
        if(i.hardware_present = 1, i.offered_name, c_id.capability_name),
        ifNull(
            if(i.hardware_present = 1, c_pipeline.canonical_pipeline, c_id.canonical_pipeline),
            if(i.hardware_present = 1, i.offered_name, c_id.capability_name)
        )
    ) as pipeline_id,
    i.model_id,
    i.warm,
    i.advertised_capacity,
    i.hardware_present,
    i.gpu_id,
    i.gpu_model_name,
    i.gpu_memory_bytes_total,
    toUInt8(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.supports_stream, 1),
            ifNull(c_id.supports_stream, 0)
        )
    ) as supports_stream,
    toUInt8(
        if(
            i.hardware_present = 1,
            ifNull(c_pipeline.supports_request, 1),
            ifNull(c_id.supports_request, 0)
        )
    ) as supports_request
from inventory i
left join catalog c_id
    on c_id.capability_id = i.capability_id
left join catalog c_pipeline
    on i.hardware_present = 1
   and lower(i.offered_name) = lower(c_pipeline.canonical_pipeline)
where (i.hardware_present = 1 and i.offered_name is not null)
   or (i.hardware_present = 0 and c_id.capability_family = 'builtin')
