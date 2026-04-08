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

// ListGPUNetworkDemand, ListGPUMetrics, and their WHERE builders are in gpu_metrics.go.
