-- Perf-pass: tiny template-variable source. Dashboards that used to
-- scan api_observed_capability_offer for DISTINCT orch_address (~1 GiB
-- per refresh) read this view instead — one row per orch_address from
-- the resolver-written api_orchestrator_identity_store.

{{ config(materialized='view') }}

select distinct
    orch_address,
    orch_label,
    name,
    org
from {{ ref('api_orchestrator_identity') }}
where orch_address != ''
order by orch_address
