with latest_slices as (
    select
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_sla_compliance_by_org_store
    group by window_start
)
select
    s.window_start,
    cast(null as Nullable(String)) as org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region,
    sum(s.known_sessions_count) as known_sessions_count,
    sum(s.startup_success_sessions) as startup_success_sessions,
    sum(s.no_orch_sessions) as no_orch_sessions,
    sum(s.startup_excused_sessions) as startup_excused_sessions,
    sum(s.startup_failed_sessions) as startup_failed_sessions,
    sum(s.confirmed_swapped_sessions) as confirmed_swapped_sessions,
    sum(s.inferred_swap_sessions) as inferred_swap_sessions,
    sum(s.total_swapped_sessions) as total_swapped_sessions,
    sum(s.sessions_ending_in_error) as sessions_ending_in_error,
    sum(s.error_status_samples) as error_status_samples,
    avg(s.health_signal_coverage_ratio) as health_signal_coverage_ratio,
    if(sum(s.known_sessions_count) >= 5, sum(s.startup_success_sessions) / toFloat64(sum(s.known_sessions_count)), cast(null as Nullable(Float64))) as startup_success_rate,
    if(sum(s.known_sessions_count) >= 5, sum(s.startup_excused_sessions) / toFloat64(sum(s.known_sessions_count)), cast(null as Nullable(Float64))) as excused_failure_rate,
    if(sum(s.known_sessions_count) >= 5, 1.0 - sum(s.total_swapped_sessions) / toFloat64(sum(s.known_sessions_count)), cast(null as Nullable(Float64))) as no_swap_rate,
    if(
        sum(s.known_sessions_count) >= 5,
        0.6 * (sum(s.startup_success_sessions) / toFloat64(sum(s.known_sessions_count))) +
        0.4 * (1.0 - sum(s.total_swapped_sessions) / toFloat64(sum(s.known_sessions_count))),
        cast(null as Nullable(Float64))
    ) as sla_score
from naap.api_sla_compliance_by_org_store s
inner join latest_slices l
    on s.window_start = l.window_start
   and s.refresh_run_id = l.refresh_run_id
group by
    s.window_start,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region
