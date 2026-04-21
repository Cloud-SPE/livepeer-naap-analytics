-- Perf-pass: tiny global template-variable source. This is a base list,
-- not a contextual cascading filter. Dashboards that need org/model
-- scoping should express those predicates in the Grafana variable query.
-- One row per orch_address from the resolver-written
-- api_orchestrator_identity_store.

{{ config(materialized='view') }}

select distinct
    orch_address,
    orch_label,
    name,
    org
from naap.api_orchestrator_identity_store
where orch_address != ''
order by orch_address
