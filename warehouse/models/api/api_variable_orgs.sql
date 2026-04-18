-- Perf-pass: tiny template-variable source for the $org dropdown.
-- Unions the two hourly demand surfaces so orgs that only run streaming
-- (e.g. daydream) AND orgs that only run request-response (e.g. cloudspe)
-- both appear in the dropdown.
--
-- The prior single-source version read from api_hourly_request_demand
-- alone and silently dropped streaming-only orgs; the Served Sessions
-- panel on the Overview dashboard then showed no daydream data because
-- daydream wasn't an `All` selection target.

{{ config(materialized='view') }}

select distinct org from (
    select org from naap.api_hourly_streaming_demand where org != ''
    union distinct
    select org from naap.api_hourly_request_demand where org != ''
)
order by org
