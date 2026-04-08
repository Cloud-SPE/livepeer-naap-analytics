package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ---------------------------------------------------------------------------
// GET /v1/jobs/demand
// ---------------------------------------------------------------------------

// ListJobsDemand returns paginated hourly demand rows for non-streaming jobs
// (ai-batch + byoc). Source: naap.api_unified_demand.
func (r *Repo) ListJobsDemand(ctx context.Context, p types.JobsParams) ([]types.JobsDemandRow, int, error) {
	where, args := buildJobsDemandWhere(p)
	offset := (p.Page - 1) * p.PageSize

	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM naap.api_unified_demand `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs demand count: %w", err)
	}

	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, gateway, pipeline_id, model_id, job_type,
			job_count, success_count, success_rate, avg_duration_ms, total_minutes
		FROM naap.api_unified_demand `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs demand query: %w", err)
	}
	defer rows.Close()

	var result []types.JobsDemandRow
	for rows.Next() {
		var row types.JobsDemandRow
		var org, modelID *string
		if err := rows.Scan(
			&row.WindowStart, &org, &row.Gateway, &row.PipelineID, &modelID, &row.JobType,
			&row.JobCount, &row.SuccessCount, &row.SuccessRate, &row.AvgDurationMs, &row.TotalMinutes,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse jobs demand scan: %w", err)
		}
		row.Org = org
		row.ModelID = modelID
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs demand rows: %w", err)
	}
	if result == nil {
		result = []types.JobsDemandRow{}
	}
	return result, int(total), nil
}

func buildJobsDemandWhere(p types.JobsParams) (string, []any) {
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
	if p.JobType != "" {
		conds = append(conds, "job_type = ?")
		args = append(args, p.JobType)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

// ---------------------------------------------------------------------------
// GET /v1/jobs/sla
// ---------------------------------------------------------------------------

// ListJobsSLA returns paginated SLA rows for non-streaming jobs (ai-batch + byoc).
// Source: naap.api_unified_sla.
func (r *Repo) ListJobsSLA(ctx context.Context, p types.JobsParams) ([]types.JobsSLARow, int, error) {
	where, args := buildJobsSLAWhere(p)
	offset := (p.Page - 1) * p.PageSize

	var total uint64
	countRow := r.conn.QueryRow(ctx, `SELECT count() FROM naap.api_unified_sla `+where, args...)
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs sla count: %w", err)
	}

	args = append(args, p.PageSize, offset)
	rows, err := r.conn.Query(ctx, `
		SELECT
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, job_type,
			job_count, success_count, success_rate, avg_duration_ms, sla_score
		FROM naap.api_unified_sla `+where+`
		ORDER BY window_start DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs sla query: %w", err)
	}
	defer rows.Close()

	var result []types.JobsSLARow
	for rows.Next() {
		var row types.JobsSLARow
		var org, modelID, gpuID *string
		if err := rows.Scan(
			&row.WindowStart, &org, &row.OrchestratorAddress, &row.PipelineID, &modelID, &gpuID, &row.JobType,
			&row.JobCount, &row.SuccessCount, &row.SuccessRate, &row.AvgDurationMs, &row.SLAScore,
		); err != nil {
			return nil, 0, fmt.Errorf("clickhouse jobs sla scan: %w", err)
		}
		row.Org = org
		row.ModelID = modelID
		row.GPUID = gpuID
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("clickhouse jobs sla rows: %w", err)
	}
	if result == nil {
		result = []types.JobsSLARow{}
	}
	return result, int(total), nil
}

func buildJobsSLAWhere(p types.JobsParams) (string, []any) {
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
	if p.JobType != "" {
		conds = append(conds, "job_type = ?")
		args = append(args, p.JobType)
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

// ---------------------------------------------------------------------------
// GET /v1/jobs/by-model
// ---------------------------------------------------------------------------

// ListJobsByModel returns performance stats per (pipeline, model) for non-streaming jobs.
// Sources: naap.canonical_ai_batch_jobs and naap.canonical_byoc_jobs.
func (r *Repo) ListJobsByModel(ctx context.Context, p types.JobsParams) ([]types.JobModelPerformance, error) {
	var result []types.JobModelPerformance

	// AI-batch
	if p.JobType == "" || p.JobType == "ai-batch" {
		aiBatchArgs := []any{p.Start.UTC(), p.End.UTC()}
		aiBatchWhere := "WHERE completed_at >= ? AND completed_at < ?"
		if p.Org != "" {
			aiBatchWhere += " AND org = ?"
			aiBatchArgs = append(aiBatchArgs, p.Org)
		}
		if p.PipelineID != "" {
			aiBatchWhere += " AND pipeline = ?"
			aiBatchArgs = append(aiBatchArgs, p.PipelineID)
		}
		if p.ModelID != "" {
			aiBatchWhere += " AND model_id = ?"
			aiBatchArgs = append(aiBatchArgs, p.ModelID)
		}
		aiBatchRows, err := r.conn.Query(ctx, `
			SELECT
				ifNull(model_id, '')                  AS model_id,
				ifNull(pipeline, '')                   AS pipeline,
				avg(toFloat64(duration_ms))            AS avg_duration,
				quantile(0.5)(toFloat64(duration_ms))  AS p50_duration,
				quantile(0.99)(toFloat64(duration_ms)) AS p99_duration,
				count()                                AS job_count,
				countDistinct(orch_url_norm)           AS warm_orchs
			FROM naap.canonical_ai_batch_jobs
			`+aiBatchWhere+`
			  AND pipeline != ''
			GROUP BY model_id, pipeline
			ORDER BY job_count DESC
			LIMIT 100
		`, aiBatchArgs...)
		if err != nil {
			return nil, fmt.Errorf("clickhouse jobs by model ai_batch: %w", err)
		}
		defer aiBatchRows.Close()

		for aiBatchRows.Next() {
			var mp types.JobModelPerformance
			var avgDur, p50Dur, p99Dur float64
			var jobCount, warmOrchs uint64
			if err := aiBatchRows.Scan(&mp.ModelID, &mp.Pipeline, &avgDur, &p50Dur, &p99Dur, &jobCount, &warmOrchs); err != nil {
				return nil, fmt.Errorf("clickhouse jobs by model ai_batch scan: %w", err)
			}
			mp.JobType = "ai-batch"
			mp.JobCount = int64(jobCount)
			mp.WarmOrchCount = int64(warmOrchs)
			mp.AvgDurationMs = &avgDur
			mp.P50DurationMs = &p50Dur
			mp.P99DurationMs = &p99Dur
			result = append(result, mp)
		}
		if err := aiBatchRows.Err(); err != nil {
			return nil, fmt.Errorf("clickhouse jobs by model ai_batch rows: %w", err)
		}
	}

	// BYOC
	if p.JobType == "" || p.JobType == "byoc" {
		byocArgs := []any{p.Start.UTC(), p.End.UTC()}
		byocWhere := "WHERE completed_at >= ? AND completed_at < ?"
		if p.Org != "" {
			byocWhere += " AND org = ?"
			byocArgs = append(byocArgs, p.Org)
		}
		if p.PipelineID != "" {
			byocWhere += " AND capability = ?"
			byocArgs = append(byocArgs, p.PipelineID)
		}
		if p.ModelID != "" {
			byocWhere += " AND model = ?"
			byocArgs = append(byocArgs, p.ModelID)
		}
		byocRows, err := r.conn.Query(ctx, `
			SELECT
				ifNull(model, '')                      AS model_id,
				ifNull(capability, '')                  AS pipeline,
				avg(toFloat64(duration_ms))             AS avg_duration,
				quantile(0.5)(toFloat64(duration_ms))   AS p50_duration,
				quantile(0.99)(toFloat64(duration_ms))  AS p99_duration,
				count()                                 AS job_count,
				countDistinct(orch_url_norm)            AS warm_orchs
			FROM naap.canonical_byoc_jobs
			`+byocWhere+`
			  AND capability != ''
			GROUP BY model, capability
			ORDER BY job_count DESC
			LIMIT 100
		`, byocArgs...)
		if err != nil {
			return nil, fmt.Errorf("clickhouse jobs by model byoc: %w", err)
		}
		defer byocRows.Close()

		for byocRows.Next() {
			var mp types.JobModelPerformance
			var avgDur, p50Dur, p99Dur float64
			var jobCount, warmOrchs uint64
			if err := byocRows.Scan(&mp.ModelID, &mp.Pipeline, &avgDur, &p50Dur, &p99Dur, &jobCount, &warmOrchs); err != nil {
				return nil, fmt.Errorf("clickhouse jobs by model byoc scan: %w", err)
			}
			mp.JobType = "byoc"
			mp.JobCount = int64(jobCount)
			mp.WarmOrchCount = int64(warmOrchs)
			mp.AvgDurationMs = &avgDur
			mp.P50DurationMs = &p50Dur
			mp.P99DurationMs = &p99Dur
			result = append(result, mp)
		}
		if err := byocRows.Err(); err != nil {
			return nil, fmt.Errorf("clickhouse jobs by model byoc rows: %w", err)
		}
	}

	if result == nil {
		result = []types.JobModelPerformance{}
	}
	return result, nil
}
