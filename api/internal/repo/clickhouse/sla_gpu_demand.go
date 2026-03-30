package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ---------------------------------------------------------------------------
// SLA Compliance  —  GET /v1/sla/compliance
// ---------------------------------------------------------------------------

// ListSLACompliance returns paginated SLA compliance rows from api_* read
// models. These are service-facing relations only; downstream derivations must
// use canonical_* instead.
func (r *Repo) ListSLACompliance(ctx context.Context, p types.SLAComplianceParams) ([]types.SLAComplianceRow, int, error) {
	view := "naap.api_sla_compliance"
	if p.Org != "" {
		view = "naap.api_sla_compliance_by_org"
	}

	where, args := buildSLAWhere(p)
	offset := (p.Page - 1) * p.PageSize

	// Count query
	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM `+view+` `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse sla compliance count: %w", err)
	}

	// Data query
	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, orchestrator_address, pipeline_id,
			model_id, gpu_id, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions, startup_excused_sessions,
			startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions, effective_failed_sessions,
			confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions, sessions_ending_in_error, error_status_samples,
			health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio,
			startup_success_rate, excused_failure_rate, effective_success_rate, no_swap_rate, output_viability_rate, sla_score
		FROM `+view+` `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse sla compliance query: %w", err)
	}
	defer rows.Close()

	var result []types.SLAComplianceRow
	for rows.Next() {
		var row types.SLAComplianceRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.OrchestratorAddress, &row.PipelineID,
			&row.ModelID, &row.GPUID, &row.Region,
			&row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions, &row.NoOrchSessions, &row.StartupExcusedSessions,
			&row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions, &row.EffectiveFailedSessions,
			&row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions, &row.SessionsEndingInError, &row.ErrorStatusSamples,
			&row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio,
			&row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate, &row.NoSwapRate, &row.OutputViabilityRate, &row.SLAScore,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse sla compliance scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse sla compliance rows: %w", err)
	}
	if result == nil {
		result = []types.SLAComplianceRow{}
	}
	return result, int(total), nil
}

func buildSLAWhere(p types.SLAComplianceParams) (string, []any) {
	conds := []string{"window_start >= ? AND window_start < ?"}
	args := []any{p.Start.UTC(), p.End.UTC()}
	if p.Org != "" {
		conds = append(conds, "org = ?")
		args = append(args, p.Org)
	}
	if p.OrchestratorAddress != "" {
		conds = append(conds, "orchestrator_address = ?")
		args = append(args, strings.ToLower(p.OrchestratorAddress))
	}
	if p.PipelineID != "" {
		conds = append(conds, "pipeline_id = ?")
		args = append(args, p.PipelineID)
	}
	if p.ModelID != "" {
		conds = append(conds, "model_id = ?")
		args = append(args, p.ModelID)
	}
	if p.GPUID != "" {
		conds = append(conds, "gpu_id = ?")
		args = append(args, p.GPUID)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

// ---------------------------------------------------------------------------
// Network Demand  —  GET /v1/network/demand
// ---------------------------------------------------------------------------

// ListNetworkDemand returns paginated network demand rows from api_* read
// models. These are service-facing relations only; downstream derivations must
// use canonical_* instead.
func (r *Repo) ListNetworkDemand(ctx context.Context, p types.NetworkDemandParams) ([]types.NetworkDemandRow, int, error) {
	view := "naap.api_network_demand"
	if p.Org != "" {
		view = "naap.api_network_demand_by_org"
	}

	where, args := buildDemandWhere(p)
	offset := (p.Page - 1) * p.PageSize

	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM `+view+` `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse network demand count: %w", err)
	}

	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, gateway, region, pipeline_id, model_id,
			sessions_count, avg_output_fps, total_minutes,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, error_status_samples, health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio, startup_success_rate, excused_failure_rate, effective_success_rate,
			ticket_face_value_eth
		FROM `+view+` `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse network demand query: %w", err)
	}
	defer rows.Close()

	var result []types.NetworkDemandRow
	for rows.Next() {
		var row types.NetworkDemandRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.Gateway, &row.Region, &row.PipelineID, &row.ModelID,
			&row.SessionsCount, &row.AvgOutputFPS, &row.TotalMinutes,
			&row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions, &row.NoOrchSessions,
			&row.StartupExcusedSessions, &row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions,
			&row.EffectiveFailedSessions, &row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions,
			&row.SessionsEndingInError, &row.ErrorStatusSamples, &row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio, &row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate,
			&row.TicketFaceValueETH,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse network demand scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse network demand rows: %w", err)
	}
	if result == nil {
		result = []types.NetworkDemandRow{}
	}
	return result, int(total), nil
}

func buildDemandWhere(p types.NetworkDemandParams) (string, []any) {
	conds := []string{"window_start >= ? AND window_start < ?"}
	args := []any{p.Start.UTC(), p.End.UTC()}
	if p.Org != "" {
		conds = append(conds, "org = ?")
		args = append(args, p.Org)
	}
	if p.Gateway != "" {
		conds = append(conds, "gateway = ?")
		args = append(args, p.Gateway)
	}
	if p.PipelineID != "" {
		conds = append(conds, "pipeline_id = ?")
		args = append(args, p.PipelineID)
	}
	if p.ModelID != "" {
		conds = append(conds, "model_id = ?")
		args = append(args, p.ModelID)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

// ---------------------------------------------------------------------------
// GPU-Sliced Network Demand  —  GET /v1/gpu/network-demand
// ---------------------------------------------------------------------------

func (r *Repo) ListGPUNetworkDemand(ctx context.Context, p types.GPUNetworkDemandParams) ([]types.GPUNetworkDemandRow, int, error) {
	view := "naap.api_gpu_network_demand"
	if p.Org != "" {
		view = "naap.api_gpu_network_demand_by_org"
	}

	where, args := buildGPUDemandWhere(p)
	offset := (p.Page - 1) * p.PageSize

	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM `+view+` `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu network demand count: %w", err)
	}

	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, gateway, orchestrator_address, region, pipeline_id, model_id, gpu_id, gpu_identity_status,
			sessions_count, avg_output_fps, total_minutes, known_sessions_count, requested_sessions, startup_success_sessions,
			no_orch_sessions, startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, error_status_samples, health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio, startup_success_rate, excused_failure_rate, effective_success_rate, ticket_face_value_eth
		FROM `+view+` `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu network demand query: %w", err)
	}
	defer rows.Close()

	var result []types.GPUNetworkDemandRow
	for rows.Next() {
		var row types.GPUNetworkDemandRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.Gateway, &row.OrchestratorAddress, &row.Region, &row.PipelineID, &row.ModelID, &row.GPUID, &row.GPUIdentityStatus,
			&row.SessionsCount, &row.AvgOutputFPS, &row.TotalMinutes, &row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions,
			&row.NoOrchSessions, &row.StartupExcusedSessions, &row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions,
			&row.EffectiveFailedSessions, &row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions,
			&row.SessionsEndingInError, &row.ErrorStatusSamples, &row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio, &row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate, &row.TicketFaceValueETH,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse gpu network demand scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu network demand rows: %w", err)
	}
	if result == nil {
		result = []types.GPUNetworkDemandRow{}
	}
	return result, int(total), nil
}

func buildGPUDemandWhere(p types.GPUNetworkDemandParams) (string, []any) {
	conds := []string{"window_start >= ? AND window_start < ?"}
	args := []any{p.Start.UTC(), p.End.UTC()}
	if p.Org != "" {
		conds = append(conds, "org = ?")
		args = append(args, p.Org)
	}
	if p.Gateway != "" {
		conds = append(conds, "gateway = ?")
		args = append(args, p.Gateway)
	}
	if p.OrchestratorAddress != "" {
		conds = append(conds, "orchestrator_address = ?")
		args = append(args, strings.ToLower(p.OrchestratorAddress))
	}
	if p.PipelineID != "" {
		conds = append(conds, "pipeline_id = ?")
		args = append(args, p.PipelineID)
	}
	if p.ModelID != "" {
		conds = append(conds, "model_id = ?")
		args = append(args, p.ModelID)
	}
	if p.GPUID != "" {
		conds = append(conds, "gpu_id = ?")
		args = append(args, p.GPUID)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

// ---------------------------------------------------------------------------
// GPU Metrics  —  GET /v1/gpu/metrics
// ---------------------------------------------------------------------------

// ListGPUMetrics returns paginated GPU performance metrics from api_* read
// models. These are service-facing relations only; downstream derivations must
// use canonical_* instead.
//
// Field approximations:
//   - region, runner_version, cuda_version may be NULL when inventory is absent
//   - fps_jitter_coefficient may be NULL when no jitter rollup is available
//   - confirmed/inferred swapped may be 0 when no swap evidence exists
func (r *Repo) ListGPUMetrics(ctx context.Context, p types.GPUMetricsParams) ([]types.GPUMetric, int, error) {
	view := "naap.api_gpu_metrics"
	if p.Org != "" {
		view = "naap.api_gpu_metrics_by_org"
	}

	where, args := buildGPUWhere(p)
	offset := (p.Page - 1) * p.PageSize

	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM `+view+` `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu metrics count: %w", err)
	}

	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, region,
			avg_output_fps, p95_output_fps, fps_jitter_coefficient,
			status_samples, error_status_samples, health_signal_coverage_ratio,
			gpu_model_name, gpu_memory_bytes_total, runner_version, cuda_version,
			avg_prompt_to_first_frame_ms, avg_startup_latency_ms, avg_e2e_latency_ms,
			p95_prompt_to_first_frame_latency_ms, p95_startup_latency_ms, p95_e2e_latency_ms,
			prompt_to_first_frame_sample_count, startup_latency_sample_count, e2e_latency_sample_count,
			known_sessions_count, startup_success_sessions, no_orch_sessions, startup_excused_sessions,
			startup_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error,
			startup_failed_rate, swap_rate
		FROM `+view+` `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu metrics query: %w", err)
	}
	defer rows.Close()

	var result []types.GPUMetric
	for rows.Next() {
		var m types.GPUMetric
		// GPU inventory columns may be NULL when the serving rollup has no
		// matching inventory snapshot for the GPU slice.
		var gpuModel *string
		var memBytes *uint64
		if err := rows.Scan(
			&m.WindowStart, &m.Org, &m.OrchestratorAddress, &m.PipelineID, &m.ModelID, &m.GPUID, &m.Region,
			&m.AvgOutputFPS, &m.P95OutputFPS, &m.FPSJitterCoefficient,
			&m.StatusSamples, &m.ErrorStatusSamples, &m.HealthSignalCoverageRatio,
			&gpuModel, &memBytes, &m.RunnerVersion, &m.CudaVersion,
			&m.AvgPromptToFirstFrameMS, &m.AvgStartupLatencyMS, &m.AvgE2ELatencyMS,
			&m.P95PromptToFirstFrameLatencyMS, &m.P95StartupLatencyMS, &m.P95E2ELatencyMS,
			&m.PromptToFirstFrameSampleCount, &m.StartupLatencySampleCount, &m.E2ELatencySampleCount,
			&m.KnownSessionsCount, &m.StartupSuccessSessions, &m.NoOrchSessions, &m.StartupExcusedSessions,
			&m.StartupFailedSessions, &m.ConfirmedSwappedSessions, &m.InferredSwapSessions,
			&m.TotalSwappedSessions, &m.SessionsEndingInError,
			&m.StartupFailedRate, &m.SwapRate,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse gpu metrics scan: %w", err)
		}
		m.GPUModelName = gpuModel
		m.GPUMemoryBytesTotal = memBytes
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse gpu metrics rows: %w", err)
	}
	if result == nil {
		result = []types.GPUMetric{}
	}
	return result, int(total), nil
}

func buildGPUWhere(p types.GPUMetricsParams) (string, []any) {
	conds := []string{"window_start >= ? AND window_start < ?"}
	args := []any{p.Start.UTC(), p.End.UTC()}
	if p.Org != "" {
		conds = append(conds, "org = ?")
		args = append(args, p.Org)
	}
	if p.OrchestratorAddress != "" {
		conds = append(conds, "orchestrator_address = ?")
		args = append(args, strings.ToLower(p.OrchestratorAddress))
	}
	if p.PipelineID != "" {
		conds = append(conds, "pipeline_id = ?")
		args = append(args, p.PipelineID)
	}
	if p.ModelID != "" {
		conds = append(conds, "model_id = ?")
		args = append(args, p.ModelID)
	}
	if p.GPUID != "" {
		conds = append(conds, "gpu_id = ?")
		args = append(args, p.GPUID)
	}
	if p.GPUModelName != "" {
		conds = append(conds, "gpu_model_name = ?")
		args = append(args, p.GPUModelName)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}
