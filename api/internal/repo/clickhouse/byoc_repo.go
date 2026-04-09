package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetBYOCSummary returns per-capability aggregates from api_byoc_jobs (R18).
func (r *Repo) GetBYOCSummary(ctx context.Context, p types.QueryParams) ([]types.BYOCJobSummary, error) {
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
			count()                                              AS total_jobs,
			toFloat64(countIf(success = 1)) / toFloat64(count()) AS success_rate,
			avg(duration_ms)                                     AS avg_duration_ms
		FROM naap.api_byoc_jobs
		`+where+`
		GROUP BY capability
		ORDER BY total_jobs DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get byoc summary: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCJobSummary
	for rows.Next() {
		var s types.BYOCJobSummary
		var totalJobs uint64
		if err := rows.Scan(&s.Capability, &totalJobs, &s.SuccessRate, &s.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("clickhouse get byoc summary scan: %w", err)
		}
		s.TotalJobs = int64(totalJobs)
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get byoc summary rows: %w", err)
	}
	if result == nil {
		result = []types.BYOCJobSummary{}
	}
	return result, nil
}

// ListBYOCJobs returns cursor-paginated completed BYOC jobs (R18).
// Stable cursor = base64(completed_at_ns|request_id). Fetch limit+1 rows to
// determine has_more without a separate COUNT query.
func (r *Repo) ListBYOCJobs(ctx context.Context, p types.QueryParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if cursorTs, cursorID := decodeCursor(p.Cursor); !cursorTs.IsZero() {
		where += " AND (completed_at < ? OR (completed_at = ? AND request_id < ?))"
		args = append(args, cursorTs, cursorTs, cursorID)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			org,
			capability,
			completed_at,
			success,
			duration_ms,
			http_status,
			orch_address,
			orch_url,
			worker_url,
			error,
			ifNull(gpu_model_name, '')  AS gpu_model_name,
			attribution_status
		FROM naap.api_byoc_jobs
		`+where+`
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list byoc jobs: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCJobRecord
	for rows.Next() {
		var rec types.BYOCJobRecord
		var successRaw *uint8
		var durationMs int64
		var httpStatus uint16
		if err := rows.Scan(
			&rec.RequestID,
			&rec.Org,
			&rec.Capability,
			&rec.CompletedAt,
			&successRaw,
			&durationMs,
			&httpStatus,
			&rec.OrchAddress,
			&rec.OrchURL,
			&rec.WorkerURL,
			&rec.Error,
			&rec.GPUModel,
			&rec.AttributionStatus,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list byoc jobs scan: %w", err)
		}
		rec.DurationMs = durationMs
		rec.HTTPStatus = int64(httpStatus)
		if successRaw != nil {
			v := *successRaw == 1
			rec.Success = &v
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list byoc jobs rows: %w", err)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.BYOCJobRecord{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursor(last.CompletedAt, last.RequestID)
	}
	return result, page, nil
}

// GetBYOCWorkers returns per-capability worker inventory from api_byoc_workers (R18).
func (r *Repo) GetBYOCWorkers(ctx context.Context, p types.QueryParams) ([]types.BYOCWorkerSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			capability,
			uniq(orch_address)                       AS worker_count,
			groupUniqArrayIf(model, model != '')     AS models,
			avg(price_per_unit)                      AS avg_price_per_unit
		FROM naap.api_byoc_workers
		`+where+`
		GROUP BY capability
		ORDER BY worker_count DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get byoc workers: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCWorkerSummary
	for rows.Next() {
		var s types.BYOCWorkerSummary
		var workerCount uint64
		if err := rows.Scan(&s.Capability, &workerCount, &s.Models, &s.AvgPricePerUnit); err != nil {
			return nil, fmt.Errorf("clickhouse get byoc workers scan: %w", err)
		}
		s.WorkerCount = int64(workerCount)
		if s.Models == nil {
			s.Models = []string{}
		}
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get byoc workers rows: %w", err)
	}
	if result == nil {
		result = []types.BYOCWorkerSummary{}
	}
	return result, nil
}

// GetBYOCAuthSummary returns per-capability auth event aggregates from api_byoc_auth (R18).
func (r *Repo) GetBYOCAuthSummary(ctx context.Context, p types.QueryParams) ([]types.BYOCAuthSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			capability,
			count()                                              AS total_events,
			toFloat64(countIf(success = 1)) / toFloat64(count()) AS success_rate,
			countIf(success = 0)                                AS failure_count
		FROM naap.api_byoc_auth
		`+where+`
		GROUP BY capability
		ORDER BY total_events DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get byoc auth summary: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCAuthSummary
	for rows.Next() {
		var s types.BYOCAuthSummary
		var totalEvents, failureCount uint64
		if err := rows.Scan(&s.Capability, &totalEvents, &s.SuccessRate, &failureCount); err != nil {
			return nil, fmt.Errorf("clickhouse get byoc auth summary scan: %w", err)
		}
		s.TotalEvents = int64(totalEvents)
		s.FailureCount = int64(failureCount)
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get byoc auth summary rows: %w", err)
	}
	if result == nil {
		result = []types.BYOCAuthSummary{}
	}
	return result, nil
}
