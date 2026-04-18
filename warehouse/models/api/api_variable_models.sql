-- Perf-pass: tiny template-variable source for the $model dropdown.
-- Derived from api_current_orchestrator_store's pipeline_model_pairs
-- column (already materialized on every resolver tail). Replaces a
-- DISTINCT scan of api_observed_capability_offer.

{{ config(materialized='view') }}

with pairs as (
    select arrayJoin(pipeline_model_pairs) as pair
    from naap.api_current_orchestrator_store
    where length(pipeline_model_pairs) > 0
)
select distinct tupleElement(pair, 2) as model_id
from pairs
where tupleElement(pair, 2) != ''
order by model_id
