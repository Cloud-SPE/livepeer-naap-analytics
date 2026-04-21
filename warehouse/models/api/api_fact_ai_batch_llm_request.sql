-- Phase 6.4: thin alias over canonical_ai_llm_requests with the unified
-- capability_family column stamped as 'builtin' — LLM requests only flow
-- through the builtin AI batch pipeline.

select
    'builtin' as capability_family,
    r.*
from {{ ref('canonical_ai_llm_requests') }} r
