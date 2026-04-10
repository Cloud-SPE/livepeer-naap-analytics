package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ---------------------------------------------------------------------------
// SLA Compliance  —  GET /v1/sla/compliance
// ---------------------------------------------------------------------------

// ListSLACompliance returns paginated SLA compliance rows from thin api_* read
// models backed by resolver-published final SLA stores. Stable ordering and
// cursor tuple: (window_start DESC, orchestrator_address DESC, pipeline_id DESC,
// model_id DESC, gpu_id DESC, region DESC). These are service-facing relations
// only; downstream derivations must use canonical_* instead.
func (r *Repo) ListSLACompliance(ctx context.Context, p types.SLAComplianceParams) ([]types.SLAComplianceRow, types.CursorPageInfo, error) {
	view := "naap.api_sla_compliance"
	if p.Org != "" {
		view = "naap.api_sla_compliance_by_org"
	}

	where, args := buildSLAWhere(p)
	limit := normalizeLimit(p.Limit)
	if values, err := decodeCursorValues(p.Cursor, 6); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 6 {
		cursorWindowStart, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, orchestrator_address, pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), ifNull(region, '')) < (?, ?, ?, ?, ?, ?)"
		args = append(args, cursorWindowStart.UTC(), values[1], values[2], values[3], values[4], values[5])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, orchestrator_address, pipeline_id,
			model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions, startup_excused_sessions,
			startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions, output_failed_sessions, effective_failed_sessions,
			confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions, sessions_ending_in_error, error_status_samples,
			health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio,
			startup_success_rate, excused_failure_rate, effective_success_rate, no_swap_rate, output_viability_rate,
			output_fps_sum, status_samples, avg_output_fps,
			prompt_to_first_frame_sum_ms, prompt_to_first_frame_sample_count, avg_prompt_to_first_frame_ms,
			e2e_latency_sum_ms, e2e_latency_sample_count, avg_e2e_latency_ms,
			reliability_score, ptff_score, e2e_score, latency_score, fps_score, quality_score,
			sla_semantics_version, sla_score
		FROM `+view+` `+where+`
		ORDER BY
			window_start DESC,
			orchestrator_address DESC,
			pipeline_id DESC,
			ifNull(model_id, '') DESC,
			ifNull(gpu_id, '') DESC,
			ifNull(region, '') DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse sla compliance query: %w", err)
	}
	defer rows.Close()

	var result []types.SLAComplianceRow
	for rows.Next() {
		var row types.SLAComplianceRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.OrchestratorAddress, &row.PipelineID,
			&row.ModelID, &row.GPUID, &row.GPUModelName, &row.Region,
			&row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions, &row.NoOrchSessions, &row.StartupExcusedSessions,
			&row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions, &row.OutputFailedSessions, &row.EffectiveFailedSessions,
			&row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions, &row.SessionsEndingInError, &row.ErrorStatusSamples,
			&row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio,
			&row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate, &row.NoSwapRate, &row.OutputViabilityRate,
			&row.OutputFPSSum, &row.StatusSamples, &row.AvgOutputFPS,
			&row.PromptToFirstFrameSumMS, &row.PromptToFirstFrameSamples, &row.AvgPromptToFirstFrameMS,
			&row.E2ELatencySumMS, &row.E2ELatencySamples, &row.AvgE2ELatencyMS,
			&row.ReliabilityScore, &row.PTFFScore, &row.E2EScore, &row.LatencyScore, &row.FPSScore, &row.QualityScore,
			&row.SLASemanticsVersion, &row.SLAScore,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse sla compliance scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse sla compliance rows: %w", err)
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}

	if result == nil {
		result = []types.SLAComplianceRow{}
	}
	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.OrchestratorAddress,
			last.PipelineID,
			nullableString(last.ModelID),
			nullableString(last.GPUID),
			nullableString(last.Region),
		)
	}
	return result, page, nil
}

func buildSLAWhere(p types.SLAComplianceParams) (string, []any) {
	conds := []string{
		"window_start >= ? AND window_start < ?",
		"orchestrator_address != ''",
		"pipeline_id != ''",
	}
	args := []any{p.Start.UTC(), p.End.UTC()}
	if p.Org != "" {
		conds = append(conds, "org = ?")
		args = append(args, p.Org)
	}
	if p.OrchestratorAddress != "" {
		conds = append(conds, "orchestrator_address = ?")
		args = append(args, strings.ToLower(p.OrchestratorAddress))
	}
	if p.Region != "" {
		conds = append(conds, "region = ?")
		args = append(args, p.Region)
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
// models. Stable ordering and cursor tuple: (window_start DESC, gateway DESC,
// region DESC, pipeline_id DESC, model_id DESC). These are service-facing
// relations only; downstream derivations must use canonical_* instead.
func (r *Repo) ListNetworkDemand(ctx context.Context, p types.NetworkDemandParams) ([]types.NetworkDemandRow, types.CursorPageInfo, error) {
	view := "naap.api_network_demand"
	if p.Org != "" {
		view = "naap.api_network_demand_by_org"
	}

	where, args := buildDemandWhere(p)
	limit := normalizeLimit(p.Limit)
	if values, err := decodeCursorValues(p.Cursor, 5); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 5 {
		cursorWindowStart, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, gateway, ifNull(region, ''), pipeline_id, ifNull(model_id, '')) < (?, ?, ?, ?, ?)"
		args = append(args, cursorWindowStart.UTC(), values[1], values[2], values[3], values[4])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, gateway, region, pipeline_id, model_id,
			sessions_count, avg_output_fps, output_fps_sum, status_samples, total_minutes,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, error_status_samples, health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio, startup_success_rate, excused_failure_rate, effective_success_rate,
			ticket_face_value_eth
		FROM `+view+` `+where+`
		ORDER BY
			window_start DESC,
			gateway DESC,
			ifNull(region, '') DESC,
			pipeline_id DESC,
			ifNull(model_id, '') DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse network demand query: %w", err)
	}
	defer rows.Close()

	var result []types.NetworkDemandRow
	for rows.Next() {
		var row types.NetworkDemandRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.Gateway, &row.Region, &row.PipelineID, &row.ModelID,
			&row.SessionsCount, &row.AvgOutputFPS, &row.OutputFPSSum, &row.StatusSamples, &row.TotalMinutes,
			&row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions, &row.NoOrchSessions,
			&row.StartupExcusedSessions, &row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions,
			&row.EffectiveFailedSessions, &row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions,
			&row.SessionsEndingInError, &row.ErrorStatusSamples, &row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio, &row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate,
			&row.TicketFaceValueETH,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse network demand scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse network demand rows: %w", err)
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}

	if result == nil {
		result = []types.NetworkDemandRow{}
	}
	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.Gateway,
			nullableString(last.Region),
			last.PipelineID,
			nullableString(last.ModelID),
		)
	}
	return result, page, nil
}

func buildDemandWhere(p types.NetworkDemandParams) (string, []any) {
	conds := []string{
		"window_start >= ? AND window_start < ?",
		"pipeline_id != ''",
	}
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

// ListGPUNetworkDemand returns paginated GPU-sliced network demand rows using
// the stable cursor tuple (window_start DESC, gateway DESC,
// orchestrator_address DESC, region DESC, pipeline_id DESC, model_id DESC,
// gpu_id DESC, gpu_identity_status DESC).
func (r *Repo) ListGPUNetworkDemand(ctx context.Context, p types.GPUNetworkDemandParams) ([]types.GPUNetworkDemandRow, types.CursorPageInfo, error) {
	view := "naap.api_gpu_network_demand"
	if p.Org != "" {
		view = "naap.api_gpu_network_demand_by_org"
	}

	where, args := buildGPUDemandWhere(p)
	limit := normalizeLimit(p.Limit)
	if values, err := decodeCursorValues(p.Cursor, 8); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 8 {
		cursorWindowStart, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, gateway, orchestrator_address, ifNull(region, ''), pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), gpu_identity_status) < (?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args, cursorWindowStart.UTC(), values[1], values[2], values[3], values[4], values[5], values[6], values[7])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, gateway, orchestrator_address, region, pipeline_id, model_id, gpu_id, gpu_identity_status,
			sessions_count, avg_output_fps, output_fps_sum, status_samples, total_minutes, known_sessions_count, requested_sessions, startup_success_sessions,
			no_orch_sessions, startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, error_status_samples, health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio, startup_success_rate, excused_failure_rate, effective_success_rate, ticket_face_value_eth
		FROM `+view+` `+where+`
		ORDER BY
			window_start DESC,
			gateway DESC,
			orchestrator_address DESC,
			ifNull(region, '') DESC,
			pipeline_id DESC,
			ifNull(model_id, '') DESC,
			ifNull(gpu_id, '') DESC,
			gpu_identity_status DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu network demand query: %w", err)
	}
	defer rows.Close()

	var result []types.GPUNetworkDemandRow
	for rows.Next() {
		var row types.GPUNetworkDemandRow
		if err := rows.Scan(
			&row.WindowStart, &row.Org, &row.Gateway, &row.OrchestratorAddress, &row.Region, &row.PipelineID, &row.ModelID, &row.GPUID, &row.GPUIdentityStatus,
			&row.SessionsCount, &row.AvgOutputFPS, &row.OutputFPSSum, &row.StatusSamples, &row.TotalMinutes, &row.KnownSessionsCount, &row.RequestedSessions, &row.StartupSuccessSessions,
			&row.NoOrchSessions, &row.StartupExcusedSessions, &row.StartupFailedSessions, &row.LoadingOnlySessions, &row.ZeroOutputFPSSessions,
			&row.EffectiveFailedSessions, &row.ConfirmedSwappedSessions, &row.InferredSwapSessions, &row.TotalSwappedSessions,
			&row.SessionsEndingInError, &row.ErrorStatusSamples, &row.HealthSignalCount, &row.HealthExpectedSignalCount,
			&row.HealthSignalCoverageRatio, &row.StartupSuccessRate, &row.ExcusedFailureRate, &row.EffectiveSuccessRate, &row.TicketFaceValueETH,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu network demand scan: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu network demand rows: %w", err)
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.GPUNetworkDemandRow{}
	}
	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.Gateway,
			last.OrchestratorAddress,
			nullableString(last.Region),
			last.PipelineID,
			nullableString(last.ModelID),
			nullableString(last.GPUID),
			last.GPUIdentityStatus,
		)
	}
	return result, page, nil
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
// models using the stable cursor tuple (window_start DESC,
// orchestrator_address DESC, pipeline_id DESC, model_id DESC, gpu_id DESC,
// region DESC). These are service-facing relations only; downstream
// derivations must use canonical_* instead.
//
// Field approximations:
//   - region, runner_version, cuda_version may be NULL when inventory is absent
//   - fps_jitter_coefficient may be NULL when no jitter rollup is available
//   - confirmed/inferred swapped may be 0 when no swap evidence exists
func (r *Repo) ListGPUMetrics(ctx context.Context, p types.GPUMetricsParams) ([]types.GPUMetric, types.CursorPageInfo, error) {
	view := "naap.api_gpu_metrics"
	if p.Org != "" {
		view = "naap.api_gpu_metrics_by_org"
	}

	where, args := buildGPUWhere(p)
	limit := normalizeLimit(p.Limit)
	if values, err := decodeCursorValues(p.Cursor, 6); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 6 {
		cursorWindowStart, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, orchestrator_address, pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), ifNull(region, '')) < (?, ?, ?, ?, ?, ?)"
		args = append(args, cursorWindowStart.UTC(), values[1], values[2], values[3], values[4], values[5])
	}

	// Public gpu metrics views can surface NaN from quantile merges when a
	// slice has no eligible latency samples. Normalize those to NULL here so
	// the API contract remains JSON-encodable and consistent with nullable
	// latency percentiles.
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, region,
			avg_output_fps, output_fps_sum, toFloat64(p95_output_fps) AS p95_output_fps, fps_jitter_coefficient,
			status_samples, error_status_samples, health_signal_count, health_expected_signal_count, health_signal_coverage_ratio,
			gpu_model_name, gpu_memory_bytes_total, runner_version, cuda_version,
			avg_prompt_to_first_frame_ms, prompt_to_first_frame_sum_ms, avg_startup_latency_ms, startup_latency_sum_ms, avg_e2e_latency_ms, e2e_latency_sum_ms,
			if(
				p95_prompt_to_first_frame_latency_ms IS NULL OR isNaN(p95_prompt_to_first_frame_latency_ms),
				cast(null, 'Nullable(Float64)'),
				cast(p95_prompt_to_first_frame_latency_ms, 'Nullable(Float64)')
			) AS p95_prompt_to_first_frame_latency_ms,
			if(
				p95_startup_latency_ms IS NULL OR isNaN(p95_startup_latency_ms),
				cast(null, 'Nullable(Float64)'),
				cast(p95_startup_latency_ms, 'Nullable(Float64)')
			) AS p95_startup_latency_ms,
			if(
				p95_e2e_latency_ms IS NULL OR isNaN(p95_e2e_latency_ms),
				cast(null, 'Nullable(Float64)'),
				cast(p95_e2e_latency_ms, 'Nullable(Float64)')
			) AS p95_e2e_latency_ms,
			prompt_to_first_frame_sample_count, startup_latency_sample_count, e2e_latency_sample_count,
			known_sessions_count, startup_success_sessions, no_orch_sessions, startup_excused_sessions,
			startup_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error,
			startup_failed_rate, swap_rate
		FROM `+view+` `+where+`
		ORDER BY
			window_start DESC,
			orchestrator_address DESC,
			pipeline_id DESC,
			ifNull(model_id, '') DESC,
			ifNull(gpu_id, '') DESC,
			ifNull(region, '') DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu metrics query: %w", err)
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
			&m.AvgOutputFPS, &m.OutputFPSSum, &m.P95OutputFPS, &m.FPSJitterCoefficient,
			&m.StatusSamples, &m.ErrorStatusSamples, &m.HealthSignalCount, &m.HealthExpectedSignalCount, &m.HealthSignalCoverageRatio,
			&gpuModel, &memBytes, &m.RunnerVersion, &m.CudaVersion,
			&m.AvgPromptToFirstFrameMS, &m.PromptToFirstFrameSumMS, &m.AvgStartupLatencyMS, &m.StartupLatencySumMS, &m.AvgE2ELatencyMS, &m.E2ELatencySumMS,
			&m.P95PromptToFirstFrameLatencyMS, &m.P95StartupLatencyMS, &m.P95E2ELatencyMS,
			&m.PromptToFirstFrameSampleCount, &m.StartupLatencySampleCount, &m.E2ELatencySampleCount,
			&m.KnownSessionsCount, &m.StartupSuccessSessions, &m.NoOrchSessions, &m.StartupExcusedSessions,
			&m.StartupFailedSessions, &m.ConfirmedSwappedSessions, &m.InferredSwapSessions,
			&m.TotalSwappedSessions, &m.SessionsEndingInError,
			&m.StartupFailedRate, &m.SwapRate,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu metrics scan: %w", err)
		}
		m.GPUModelName = gpuModel
		m.GPUMemoryBytesTotal = memBytes
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse gpu metrics rows: %w", err)
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.GPUMetric{}
	}
	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.OrchestratorAddress,
			last.PipelineID,
			nullableString(last.ModelID),
			nullableString(last.GPUID),
			nullableString(last.Region),
		)
	}
	return result, page, nil
}

func nullableString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
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
