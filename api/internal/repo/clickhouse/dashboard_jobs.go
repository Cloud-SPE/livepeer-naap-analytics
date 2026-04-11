package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetDashboardJobsOverview returns top-level AI batch + BYOC aggregates.
func (r *Repo) GetDashboardJobsOverview(ctx context.Context, p types.QueryParams) (*types.DashboardJobsOverview, error) {
	start, end := effectiveWindow(p)

	// Guard against NaN/Inf on empty result sets.
	// count() always returns one aggregate row. When it is zero:
	//   - division produces NaN (not NULL) in ClickHouse — ifNull alone is not enough
	//   - quantile() / avg() on empty produce nan, not NULL
	// Wrapping each aggregate with if(count() > 0, ..., 0) is the safe pattern.
	safeAggs := `
		count()                                                                                    AS total_jobs,
		countIf(selection_outcome = 'selected')                                                    AS selected_jobs,
		countIf(selection_outcome = 'no_orch')                                                     AS no_orch_jobs,
		countIf(selection_outcome = 'unknown')                                                     AS unknown_jobs,
		if(count() > 0, toFloat64(countIf(success = 1)) / toFloat64(count()), 0.0)               AS success_rate,
		if(count() > 0, avg(toFloat64(duration_ms)), 0.0)                                         AS avg_duration_ms,
		if(count() > 0, toFloat64(quantile(0.99)(duration_ms)), 0.0)                              AS p99_duration_ms,
		if(
			countIf(selection_outcome = 'selected') > 0,
			toFloat64(countIf(
				selection_outcome = 'selected'
				AND attribution_status IN ('resolved', 'hardware_less', 'stale')
			)) / toFloat64(countIf(selection_outcome = 'selected')),
			0.0
		)                                                                                          AS selected_attribution_worked_rate`

	var ai types.DashboardJobsStats
	aiRow := r.conn.QueryRow(ctx,
		`SELECT`+safeAggs+`
		FROM naap.api_ai_batch_jobs
		WHERE completed_at >= ? AND completed_at < ?`,
		start, end)
	var aiTotal, aiSelected, aiNoOrch, aiUnknown uint64
	if err := aiRow.Scan(
		&aiTotal,
		&aiSelected,
		&aiNoOrch,
		&aiUnknown,
		&ai.SuccessRate,
		&ai.AvgDurationMs,
		&ai.P99DurationMs,
		&ai.SelectedAttributionWorkedRate,
	); err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs overview ai: %w", err)
	}
	ai.TotalJobs = int64(aiTotal)
	ai.SelectedJobs = int64(aiSelected)
	ai.NoOrchJobs = int64(aiNoOrch)
	ai.UnknownJobs = int64(aiUnknown)

	var byoc types.DashboardJobsStats
	byocRow := r.conn.QueryRow(ctx,
		`SELECT`+safeAggs+`
		FROM naap.api_byoc_jobs
		WHERE completed_at >= ? AND completed_at < ?`,
		start, end)
	var byocTotal, byocSelected, byocNoOrch, byocUnknown uint64
	if err := byocRow.Scan(
		&byocTotal,
		&byocSelected,
		&byocNoOrch,
		&byocUnknown,
		&byoc.SuccessRate,
		&byoc.AvgDurationMs,
		&byoc.P99DurationMs,
		&byoc.SelectedAttributionWorkedRate,
	); err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs overview byoc: %w", err)
	}
	byoc.TotalJobs = int64(byocTotal)
	byoc.SelectedJobs = int64(byocSelected)
	byoc.NoOrchJobs = int64(byocNoOrch)
	byoc.UnknownJobs = int64(byocUnknown)

	return &types.DashboardJobsOverview{AIBatch: ai, BYOC: byoc}, nil
}

// GetDashboardJobsByPipeline returns AI batch breakdown by pipeline.
func (r *Repo) GetDashboardJobsByPipeline(ctx context.Context, p types.QueryParams) ([]types.DashboardJobsByPipelineRow, error) {
	start, end := effectiveWindow(p)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			count()                                                AS total_jobs,
			countIf(selection_outcome = 'selected')                AS selected_jobs,
			countIf(selection_outcome = 'no_orch')                 AS no_orch_jobs,
			countIf(selection_outcome = 'unknown')                 AS unknown_jobs,
			toFloat64(countIf(success = 1)) / toFloat64(count())  AS success_rate,
			avg(duration_ms)                                       AS avg_duration_ms,
			if(
				countIf(selection_outcome = 'selected') > 0,
				toFloat64(countIf(
					selection_outcome = 'selected'
					AND attribution_status IN ('resolved', 'hardware_less', 'stale')
				)) / toFloat64(countIf(selection_outcome = 'selected')),
				0.0
			)                                                      AS selected_attribution_worked_rate
		FROM naap.api_ai_batch_jobs
		`+where+`
		GROUP BY pipeline
		ORDER BY total_jobs DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs by pipeline: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardJobsByPipelineRow
	for rows.Next() {
		var row types.DashboardJobsByPipelineRow
		var total, selected, noOrch, unknown uint64
		if err := rows.Scan(
			&row.Pipeline,
			&total,
			&selected,
			&noOrch,
			&unknown,
			&row.SuccessRate,
			&row.AvgDurationMs,
			&row.SelectedAttributionWorkedRate,
		); err != nil {
			return nil, fmt.Errorf("clickhouse dashboard jobs by pipeline scan: %w", err)
		}
		row.TotalJobs = int64(total)
		row.SelectedJobs = int64(selected)
		row.NoOrchJobs = int64(noOrch)
		row.UnknownJobs = int64(unknown)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs by pipeline rows: %w", err)
	}
	if result == nil {
		result = []types.DashboardJobsByPipelineRow{}
	}
	return result, nil
}

// GetDashboardJobsByCapability returns BYOC breakdown by capability.
func (r *Repo) GetDashboardJobsByCapability(ctx context.Context, p types.QueryParams) ([]types.DashboardJobsByCapabilityRow, error) {
	start, end := effectiveWindow(p)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			capability,
			count()                                                AS total_jobs,
			countIf(selection_outcome = 'selected')                AS selected_jobs,
			countIf(selection_outcome = 'no_orch')                 AS no_orch_jobs,
			countIf(selection_outcome = 'unknown')                 AS unknown_jobs,
			toFloat64(countIf(success = 1)) / toFloat64(count())  AS success_rate,
			avg(duration_ms)                                       AS avg_duration_ms,
			if(
				countIf(selection_outcome = 'selected') > 0,
				toFloat64(countIf(
					selection_outcome = 'selected'
					AND attribution_status IN ('resolved', 'hardware_less', 'stale')
				)) / toFloat64(countIf(selection_outcome = 'selected')),
				0.0
			)                                                      AS selected_attribution_worked_rate
		FROM naap.api_byoc_jobs
		`+where+`
		GROUP BY capability
		ORDER BY total_jobs DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs by capability: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardJobsByCapabilityRow
	for rows.Next() {
		var row types.DashboardJobsByCapabilityRow
		var total, selected, noOrch, unknown uint64
		if err := rows.Scan(
			&row.Capability,
			&total,
			&selected,
			&noOrch,
			&unknown,
			&row.SuccessRate,
			&row.AvgDurationMs,
			&row.SelectedAttributionWorkedRate,
		); err != nil {
			return nil, fmt.Errorf("clickhouse dashboard jobs by capability scan: %w", err)
		}
		row.TotalJobs = int64(total)
		row.SelectedJobs = int64(selected)
		row.NoOrchJobs = int64(noOrch)
		row.UnknownJobs = int64(unknown)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse dashboard jobs by capability rows: %w", err)
	}
	if result == nil {
		result = []types.DashboardJobsByCapabilityRow{}
	}
	return result, nil
}
