-- Phase 6.4: thin alias over canonical_ai_batch_jobs with the unified
-- capability_family column stamped as 'builtin' so handlers joining this
-- fact with api_hourly_request_demand / api_observed_capability_offer
-- can filter consistently across the three-families table set.

select
    'builtin' as capability_family,
    j.*
from {{ ref('canonical_ai_batch_jobs') }} j
