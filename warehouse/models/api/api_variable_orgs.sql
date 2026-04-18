-- Perf-pass: tiny template-variable source for the $org dropdown.
-- Reads from api_hourly_request_demand_store (resolver-pre-aggregated).

{{ config(materialized='view') }}

select distinct org
from naap.api_hourly_request_demand
where org != ''
order by org
