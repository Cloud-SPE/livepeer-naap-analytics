-- Perf-pass: tiny template-variable source for the $capability dropdown.
-- Reads from api_hourly_byoc_auth_store (resolver-pre-aggregated) so the
-- list is <30 rows even on a fresh DB.

{{ config(materialized='view') }}

select distinct capability_name as capability
from naap.api_hourly_byoc_auth
where capability_name != ''
order by capability
