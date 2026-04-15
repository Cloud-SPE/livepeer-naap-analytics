select
    toUInt16(capability_id) as capability_id,
    trim(capability_name) as capability_name,
    trim(capability_family) as capability_family,
    cast(nullIf(trim(canonical_pipeline), ''), 'Nullable(String)') as canonical_pipeline,
    trim(constraint_kind) as constraint_kind,
    toUInt8(supports_stream) as supports_stream,
    toUInt8(supports_request) as supports_request
from {{ ref('capability_catalog') }}
where capability_id > 0
