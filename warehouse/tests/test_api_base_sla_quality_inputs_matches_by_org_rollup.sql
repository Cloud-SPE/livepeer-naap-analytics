with expected as (
    select
        window_start,
        orchestrator_address,
        pipeline_id,
        ifNull(model_id, '') as model_id_key,
        ifNull(gpu_id, '') as gpu_id_key,
        sum(known_sessions_count) as known_sessions_count,
        sum(requested_sessions) as requested_sessions,
        sum(startup_success_sessions) as startup_success_sessions,
        sum(no_orch_sessions) as no_orch_sessions,
        sum(startup_excused_sessions) as startup_excused_sessions,
        sum(startup_failed_sessions) as startup_failed_sessions,
        sum(loading_only_sessions) as loading_only_sessions,
        sum(zero_output_fps_sessions) as zero_output_fps_sessions,
        sum(output_failed_sessions) as output_failed_sessions,
        sum(effective_failed_sessions) as effective_failed_sessions,
        sum(confirmed_swapped_sessions) as confirmed_swapped_sessions,
        sum(inferred_swap_sessions) as inferred_swap_sessions,
        sum(total_swapped_sessions) as total_swapped_sessions,
        sum(sessions_ending_in_error) as sessions_ending_in_error,
        sum(error_status_samples) as error_status_samples,
        sum(health_signal_count) as health_signal_count,
        sum(health_expected_signal_count) as health_expected_signal_count,
        sum(output_fps_sum) as output_fps_sum,
        sum(status_samples) as status_samples,
        sum(prompt_to_first_frame_sum_ms) as prompt_to_first_frame_sum_ms,
        sum(prompt_to_first_frame_sample_count) as prompt_to_first_frame_sample_count,
        sum(e2e_latency_sum_ms) as e2e_latency_sum_ms,
        sum(e2e_latency_sample_count) as e2e_latency_sample_count
    from {{ ref('api_base_sla_quality_inputs_by_org') }}
    group by 1, 2, 3, 4, 5
),
actual as (
    select
        window_start,
        orchestrator_address,
        pipeline_id,
        ifNull(model_id, '') as model_id_key,
        ifNull(gpu_id, '') as gpu_id_key,
        known_sessions_count,
        requested_sessions,
        startup_success_sessions,
        no_orch_sessions,
        startup_excused_sessions,
        startup_failed_sessions,
        loading_only_sessions,
        zero_output_fps_sessions,
        output_failed_sessions,
        effective_failed_sessions,
        confirmed_swapped_sessions,
        inferred_swap_sessions,
        total_swapped_sessions,
        sessions_ending_in_error,
        error_status_samples,
        health_signal_count,
        health_expected_signal_count,
        output_fps_sum,
        status_samples,
        prompt_to_first_frame_sum_ms,
        prompt_to_first_frame_sample_count,
        e2e_latency_sum_ms,
        e2e_latency_sample_count
    from {{ ref('api_base_sla_quality_inputs') }}
)
select
    coalesce(e.window_start, a.window_start) as window_start,
    coalesce(e.orchestrator_address, a.orchestrator_address) as orchestrator_address,
    coalesce(e.pipeline_id, a.pipeline_id) as pipeline_id,
    coalesce(e.model_id_key, a.model_id_key) as model_id_key,
    coalesce(e.gpu_id_key, a.gpu_id_key) as gpu_id_key,
    e.known_sessions_count as expected_known_sessions_count,
    a.known_sessions_count as actual_known_sessions_count,
    e.requested_sessions as expected_requested_sessions,
    a.requested_sessions as actual_requested_sessions,
    e.startup_success_sessions as expected_startup_success_sessions,
    a.startup_success_sessions as actual_startup_success_sessions,
    e.no_orch_sessions as expected_no_orch_sessions,
    a.no_orch_sessions as actual_no_orch_sessions,
    e.startup_excused_sessions as expected_startup_excused_sessions,
    a.startup_excused_sessions as actual_startup_excused_sessions,
    e.startup_failed_sessions as expected_startup_failed_sessions,
    a.startup_failed_sessions as actual_startup_failed_sessions,
    e.loading_only_sessions as expected_loading_only_sessions,
    a.loading_only_sessions as actual_loading_only_sessions,
    e.zero_output_fps_sessions as expected_zero_output_fps_sessions,
    a.zero_output_fps_sessions as actual_zero_output_fps_sessions,
    e.output_failed_sessions as expected_output_failed_sessions,
    a.output_failed_sessions as actual_output_failed_sessions,
    e.effective_failed_sessions as expected_effective_failed_sessions,
    a.effective_failed_sessions as actual_effective_failed_sessions,
    e.confirmed_swapped_sessions as expected_confirmed_swapped_sessions,
    a.confirmed_swapped_sessions as actual_confirmed_swapped_sessions,
    e.inferred_swap_sessions as expected_inferred_swap_sessions,
    a.inferred_swap_sessions as actual_inferred_swap_sessions,
    e.total_swapped_sessions as expected_total_swapped_sessions,
    a.total_swapped_sessions as actual_total_swapped_sessions,
    e.sessions_ending_in_error as expected_sessions_ending_in_error,
    a.sessions_ending_in_error as actual_sessions_ending_in_error,
    e.error_status_samples as expected_error_status_samples,
    a.error_status_samples as actual_error_status_samples,
    e.health_signal_count as expected_health_signal_count,
    a.health_signal_count as actual_health_signal_count,
    e.health_expected_signal_count as expected_health_expected_signal_count,
    a.health_expected_signal_count as actual_health_expected_signal_count,
    e.output_fps_sum as expected_output_fps_sum,
    a.output_fps_sum as actual_output_fps_sum,
    e.status_samples as expected_status_samples,
    a.status_samples as actual_status_samples,
    e.prompt_to_first_frame_sum_ms as expected_prompt_to_first_frame_sum_ms,
    a.prompt_to_first_frame_sum_ms as actual_prompt_to_first_frame_sum_ms,
    e.prompt_to_first_frame_sample_count as expected_prompt_to_first_frame_sample_count,
    a.prompt_to_first_frame_sample_count as actual_prompt_to_first_frame_sample_count,
    e.e2e_latency_sum_ms as expected_e2e_latency_sum_ms,
    a.e2e_latency_sum_ms as actual_e2e_latency_sum_ms,
    e.e2e_latency_sample_count as expected_e2e_latency_sample_count,
    a.e2e_latency_sample_count as actual_e2e_latency_sample_count
from expected e
full outer join actual a
    on e.window_start = a.window_start
   and e.orchestrator_address = a.orchestrator_address
   and e.pipeline_id = a.pipeline_id
   and e.model_id_key = a.model_id_key
   and e.gpu_id_key = a.gpu_id_key
where ifNull(e.known_sessions_count, 0) != ifNull(a.known_sessions_count, 0)
   or ifNull(e.requested_sessions, 0) != ifNull(a.requested_sessions, 0)
   or ifNull(e.startup_success_sessions, 0) != ifNull(a.startup_success_sessions, 0)
   or ifNull(e.no_orch_sessions, 0) != ifNull(a.no_orch_sessions, 0)
   or ifNull(e.startup_excused_sessions, 0) != ifNull(a.startup_excused_sessions, 0)
   or ifNull(e.startup_failed_sessions, 0) != ifNull(a.startup_failed_sessions, 0)
   or ifNull(e.loading_only_sessions, 0) != ifNull(a.loading_only_sessions, 0)
   or ifNull(e.zero_output_fps_sessions, 0) != ifNull(a.zero_output_fps_sessions, 0)
   or ifNull(e.output_failed_sessions, 0) != ifNull(a.output_failed_sessions, 0)
   or ifNull(e.effective_failed_sessions, 0) != ifNull(a.effective_failed_sessions, 0)
   or ifNull(e.confirmed_swapped_sessions, 0) != ifNull(a.confirmed_swapped_sessions, 0)
   or ifNull(e.inferred_swap_sessions, 0) != ifNull(a.inferred_swap_sessions, 0)
   or ifNull(e.total_swapped_sessions, 0) != ifNull(a.total_swapped_sessions, 0)
   or ifNull(e.sessions_ending_in_error, 0) != ifNull(a.sessions_ending_in_error, 0)
   or ifNull(e.error_status_samples, 0) != ifNull(a.error_status_samples, 0)
   or ifNull(e.health_signal_count, 0) != ifNull(a.health_signal_count, 0)
   or ifNull(e.health_expected_signal_count, 0) != ifNull(a.health_expected_signal_count, 0)
   or abs(ifNull(e.output_fps_sum, 0.0) - ifNull(a.output_fps_sum, 0.0)) > 1e-9
   or ifNull(e.status_samples, 0) != ifNull(a.status_samples, 0)
   or abs(ifNull(e.prompt_to_first_frame_sum_ms, 0.0) - ifNull(a.prompt_to_first_frame_sum_ms, 0.0)) > 1e-9
   or ifNull(e.prompt_to_first_frame_sample_count, 0) != ifNull(a.prompt_to_first_frame_sample_count, 0)
   or abs(ifNull(e.e2e_latency_sum_ms, 0.0) - ifNull(a.e2e_latency_sum_ms, 0.0)) > 1e-9
   or ifNull(e.e2e_latency_sample_count, 0) != ifNull(a.e2e_latency_sample_count, 0)
