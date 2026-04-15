select
    org,
    orch_address,
    orch_uri,
    last_seen,
    capability_id,
    capability_name,
    capability_family,
    canonical_pipeline,
    offered_name,
    model_id,
    gpu_id,
    gpu_model_name,
    gpu_memory_bytes_total,
    supports_stream,
    supports_request
from {{ ref('canonical_capability_offer_current') }}
where hardware_present = 1
  and gpu_id is not null
