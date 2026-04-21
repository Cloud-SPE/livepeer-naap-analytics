-- Additive-primitives test: every api_hourly_* view must expose the
-- additive counters that downstream consumers reaggregate from, and
-- those counters must be non-null on every row.
--
-- Contract (docs/design-docs/api-table-contract.md §Additivity rule):
-- Derived metrics (ratios, averages, scores) live alongside the
-- additive primitives they are computed from, so clients rolling up
-- over wider windows can recompute correctly.
--
-- If a primitive is NULL or missing, the test emits a row naming the
-- offender; dbt fails the test when row count > 0.

{%- set required = {
    'api_hourly_streaming_sla': [
        'requested_sessions',
        'startup_success_sessions',
        'startup_excused_sessions',
        'total_swapped_sessions',
        'output_failed_sessions',
        'effective_failed_sessions',
        'status_samples',
        'output_fps_sum',
        'prompt_to_first_frame_sum_ms',
        'prompt_to_first_frame_sample_count',
        'e2e_latency_sum_ms',
        'e2e_latency_sample_count',
        'health_signal_count',
        'health_expected_signal_count',
    ],
    'api_hourly_streaming_demand': [
        'sessions_count',
        'requested_sessions',
        'startup_success_sessions',
        'total_swapped_sessions',
        'output_fps_sum',
        'status_samples',
        'health_signal_count',
        'health_expected_signal_count',
    ],
    'api_hourly_streaming_gpu_metrics': [
        'status_samples',
        'output_fps_sum',
        'prompt_to_first_frame_sum_ms',
        'prompt_to_first_frame_sample_count',
        'e2e_latency_sum_ms',
        'e2e_latency_sample_count',
        'health_signal_count',
        'health_expected_signal_count',
    ],
    'api_hourly_request_demand': [
        'job_count',
        'selected_count',
        'no_orch_count',
        'success_count',
        'duration_ms_sum',
    ],
    'api_hourly_byoc_auth': [
        'total_events',
        'success_count',
        'failure_count',
    ],
    'api_hourly_byoc_payments': [
        'payment_count',
        'total_amount',
        'unique_orchs',
    ],
} -%}

{%- set parts = [] -%}
{%- for model_name, primitives in required.items() -%}
  {%- for col in primitives -%}
    {# ClickHouse requires LIMIT-bearing SELECTs in a UNION to be
       wrapped in parentheses, so each probe is its own subquery. #}
    {%- set probe -%}
(select '{{ model_name }}.{{ col }}' as violation
 from {{ ref(model_name) }}
 where {{ col }} is null
 limit 1)
    {%- endset -%}
    {%- do parts.append(probe) -%}
  {%- endfor -%}
{%- endfor -%}

-- dbt wraps this SELECT in an outer `select * from (<this>) dbt_internal_test`,
-- so we emit a single top-level SELECT that reads from a UNION-derived
-- table.
select violation from (
  {%- if parts %}
  {% for p in parts -%}
    {{ p }}
    {%- if not loop.last %} union all {% endif %}
  {%- endfor %}
  {%- else %}
  select 'none' as violation where 1 = 0
  {%- endif %}
)
