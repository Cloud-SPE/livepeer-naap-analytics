{{ config(materialized='view') }}

with {{ latest_value_cte('latest_slices', 'naap.canonical_streaming_sla_input_hourly_store', ['org', 'window_start'], 'refresh_run_id') }},
inventory as (
    select
        inv.org,
        inv.orch_address as orchestrator_address,
        coalesce(nullIf(inv.model_id, ''), nullIf(inv.pipeline_id, '')) as inventory_key,
        nullIf(argMaxIfMerge(inv.gpu_model_name_state), '') as gpu_model_name
    from naap.canonical_latest_orchestrator_pipeline_inventory_agg inv
    group by inv.org, orchestrator_address, inventory_key
)
select
    s.window_start as window_start,
    s.org as org,
    s.orchestrator_address as orchestrator_address,
    s.pipeline_id as pipeline_id,
    s.model_id as model_id,
    s.gpu_id as gpu_id,
    any(coalesce(s.gpu_model_name, i.gpu_model_name)) as gpu_model_name,
    sum(s.known_sessions_count) as known_sessions_count,
    sum(s.requested_sessions) as requested_sessions,
    sum(s.startup_success_sessions) as startup_success_sessions,
    sum(s.no_orch_sessions) as no_orch_sessions,
    sum(s.startup_excused_sessions) as startup_excused_sessions,
    sum(s.startup_failed_sessions) as startup_failed_sessions,
    sum(s.loading_only_sessions) as loading_only_sessions,
    sum(s.zero_output_fps_sessions) as zero_output_fps_sessions,
    sum(s.output_failed_sessions) as output_failed_sessions,
    sum(s.effective_failed_sessions) as effective_failed_sessions,
    sum(s.confirmed_swapped_sessions) as confirmed_swapped_sessions,
    sum(s.inferred_swap_sessions) as inferred_swap_sessions,
    sum(s.total_swapped_sessions) as total_swapped_sessions,
    sum(s.sessions_ending_in_error) as sessions_ending_in_error,
    sum(s.error_status_samples) as error_status_samples,
    sum(s.health_signal_count) as health_signal_count,
    sum(s.health_expected_signal_count) as health_expected_signal_count,
    least(
        if(sum(s.health_expected_signal_count) > 0, sum(s.health_signal_count) / toFloat64(sum(s.health_expected_signal_count)), 1.0),
        1.0
    ) as health_signal_coverage_ratio,
    if(sum(s.requested_sessions) > 0, sum(s.startup_success_sessions) / toFloat64(sum(s.requested_sessions)), cast(null as Nullable(Float64))) as startup_success_rate,
    if(sum(s.requested_sessions) > 0, sum(s.startup_excused_sessions) / toFloat64(sum(s.requested_sessions)), cast(null as Nullable(Float64))) as excused_failure_rate,
    if(
        sum(s.requested_sessions) > 0,
        1.0 - (sum(s.effective_failed_sessions) / toFloat64(sum(s.requested_sessions))),
        cast(null as Nullable(Float64))
    ) as effective_success_rate,
    if(
        sum(s.requested_sessions) > 0,
        1.0 - (sum(s.total_swapped_sessions) / toFloat64(sum(s.requested_sessions))),
        cast(null as Nullable(Float64))
    ) as no_swap_rate,
    if(
        sum(s.requested_sessions) > 0,
        1.0 - (sum(s.output_failed_sessions) / toFloat64(sum(s.requested_sessions))),
        cast(null as Nullable(Float64))
    ) as output_viability_rate,
    sum(s.output_fps_sum) as output_fps_sum,
    sum(s.status_samples) as status_samples,
    if(sum(s.status_samples) > 0, sum(s.output_fps_sum) / toFloat64(sum(s.status_samples)), cast(null as Nullable(Float64))) as avg_output_fps,
    sum(s.prompt_to_first_frame_sum_ms) as prompt_to_first_frame_sum_ms,
    sum(s.prompt_to_first_frame_sample_count) as prompt_to_first_frame_sample_count,
    if(
        sum(s.prompt_to_first_frame_sample_count) > 0,
        sum(s.prompt_to_first_frame_sum_ms) / toFloat64(sum(s.prompt_to_first_frame_sample_count)),
        cast(null as Nullable(Float64))
    ) as avg_prompt_to_first_frame_ms,
    sum(s.e2e_latency_sum_ms) as e2e_latency_sum_ms,
    sum(s.e2e_latency_sample_count) as e2e_latency_sample_count,
    if(
        sum(s.e2e_latency_sample_count) > 0,
        sum(s.e2e_latency_sum_ms) / toFloat64(sum(s.e2e_latency_sample_count)),
        cast(null as Nullable(Float64))
    ) as avg_e2e_latency_ms
from naap.canonical_streaming_sla_input_hourly_store s
inner join latest_slices l
    on {{ join_on_columns('s', 'l', ['org', 'window_start']) }}
   and s.refresh_run_id = l.refresh_run_id
left join inventory i
    on s.org = i.org
   and s.orchestrator_address = i.orchestrator_address
   and coalesce(nullIf(s.model_id, ''), nullIf(s.pipeline_id, '')) = i.inventory_key
group by
    s.window_start,
    s.org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id
