-- Phase 6.4: thin alias over canonical_byoc_jobs with the unified
-- capability_family column stamped as 'byoc'.

select
    'byoc' as capability_family,
    j.*
from {{ ref('canonical_byoc_jobs') }} j
