with catalog as (
    select *
    from {{ ref('canonical_capability_catalog') }}
),
latest as (
    select
        org,
        orch_address,
        uri as orch_uri,
        last_seen,
        raw_capabilities
    from {{ ref('canonical_latest_orchestrator_state') }}
),
capability_prices as (
    select
        l.org,
        l.orch_address,
        l.orch_uri,
        l.last_seen,
        toUInt16(JSONExtractUInt(price_json, 'capability')) as capability_id,
        nullIf(JSONExtractString(price_json, 'constraint'), '') as constraint_value,
        toInt64(JSONExtractInt(price_json, 'pricePerUnit')) as price_per_unit,
        toInt64(JSONExtractInt(price_json, 'pixelsPerUnit')) as pixels_per_unit
    from latest l
    array join JSONExtractArrayRaw(raw_capabilities, 'capabilities_prices') as price_json
),
global_fallback as (
    select
        org,
        orch_address,
        orch_uri,
        last_seen,
        cast(null, 'Nullable(UInt16)') as capability_id,
        cast(null, 'Nullable(String)') as constraint_value,
        toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit')) as price_per_unit,
        toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit')) as pixels_per_unit
    from latest
    where JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit') > 0
      and JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit') > 0
)
select
    p.org,
    p.orch_address,
    p.orch_uri,
    p.last_seen,
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
from capability_prices p
left join catalog c
    on c.capability_id = p.capability_id
where p.price_per_unit > 0
  and p.pixels_per_unit > 0
union all
select
    g.org,
    g.orch_address,
    g.orch_uri,
    g.last_seen,
    cast(null, 'Nullable(UInt16)') as capability_id,
    cast(null, 'Nullable(String)') as capability_name,
    cast(null, 'Nullable(String)') as capability_family,
    cast(null, 'Nullable(String)') as canonical_pipeline,
    cast(null, 'Nullable(String)') as constraint_value,
    cast(null, 'Nullable(String)') as model_id,
    cast(null, 'Nullable(String)') as external_capability_name,
    g.price_per_unit,
    g.pixels_per_unit
from global_fallback g
