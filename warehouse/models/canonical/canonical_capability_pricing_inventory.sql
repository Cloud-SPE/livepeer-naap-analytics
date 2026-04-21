with catalog as (
    select *
    from {{ ref('canonical_capability_catalog') }}
),
inventory as (
    select *
    from naap.canonical_capability_pricing_inventory_store
)
select
    p.snapshot_row_id as capability_version_id,
    p.source_event_id as snapshot_event_id,
    p.org,
    p.orch_address,
    p.orch_uri as orchestrator_uri,
    p.orch_uri_norm,
    p.snapshot_ts as last_seen,
    cast(p.capability_id, 'Nullable(UInt16)') as capability_id,
    cast(c.capability_name, 'Nullable(String)') as capability_name,
    cast(c.capability_family, 'Nullable(String)') as capability_family,
    cast(c.canonical_pipeline, 'Nullable(String)') as canonical_pipeline,
    p.constraint_value,
    cast(
        if(c.constraint_kind = 'model_id', p.constraint_value, null),
        'Nullable(String)'
    ) as model_id,
    cast(
        if(c.constraint_kind = 'external_capability_name', p.constraint_value, null),
        'Nullable(String)'
    ) as external_capability_name,
    p.price_per_unit,
    p.pixels_per_unit
from inventory p
left join catalog c
    on c.capability_id = p.capability_id
where p.price_per_unit > 0
  and p.pixels_per_unit > 0
