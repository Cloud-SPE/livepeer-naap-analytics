-- Perf-pass: tiny global template-variable source for the $org dropdown.
-- This is an unscoped base list only. Dashboards that need contextual
-- cascading filters should embed parent-variable predicates in the
-- Grafana query itself rather than reading this view directly.
--
-- Reads canonical-layer stores directly (rather than the api_hourly_*
-- views) to satisfy the serving contract rule that api_* views only
-- depend on canonical_* or api_*_store tables.

{{ config(materialized='view') }}

select distinct org from (
    select org from naap.canonical_streaming_demand_hourly_store where org != ''
    union distinct
    select org from naap.api_hourly_request_demand_store where org != ''
)
order by org
