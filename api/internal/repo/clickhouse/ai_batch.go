package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetAIBatchSummary returns per-pipeline aggregates from api_ai_batch_jobs (R17).
func (r *Repo) GetAIBatchSummary(ctx context.Context, p types.QueryParams) ([]types.AIBatchJobSummary, error) {
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
			count()                                              AS total_jobs,
			countIf(selection_outcome = 'selected')              AS selected_jobs,
			countIf(selection_outcome = 'no_orch')               AS no_orch_jobs,
			countIf(selection_outcome = 'unknown')               AS unknown_jobs,
			toFloat64(countIf(success = 1)) / toFloat64(count()) AS success_rate,
			avg(duration_ms)                                     AS avg_duration_ms,
			avg(latency_score)                                   AS avg_latency_score,
			if(
				countIf(selection_outcome = 'selected') > 0,
				toFloat64(countIf(
					selection_outcome = 'selected'
					AND attribution_status IN ('resolved', 'hardware_less', 'stale')
				)) / toFloat64(countIf(selection_outcome = 'selected')),
				0.0
			) AS selected_attribution_worked_rate
		FROM naap.api_ai_batch_jobs
		`+where+`
		GROUP BY pipeline
		ORDER BY total_jobs DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get ai batch summary: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchJobSummary
	for rows.Next() {
		var s types.AIBatchJobSummary
		var totalJobs, selectedJobs, noOrchJobs, unknownJobs uint64
		if err := rows.Scan(
			&s.Pipeline,
			&totalJobs,
			&selectedJobs,
			&noOrchJobs,
			&unknownJobs,
			&s.SuccessRate,
			&s.AvgDurationMs,
			&s.AvgLatency,
			&s.SelectedAttributionWorkedRate,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get ai batch summary scan: %w", err)
		}
		s.TotalJobs = int64(totalJobs)
		s.SelectedJobs = int64(selectedJobs)
		s.NoOrchJobs = int64(noOrchJobs)
		s.UnknownJobs = int64(unknownJobs)
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get ai batch summary rows: %w", err)
	}
	if result == nil {
		result = []types.AIBatchJobSummary{}
	}
	return result, nil
}

// ListAIBatchJobs returns cursor-paginated completed AI batch jobs (R17).
// Stable ordering is completed_at DESC, request_id DESC; the cursor stores that
// tuple opaquely. Fetch limit+1 rows to determine has_more without COUNT.
func (r *Repo) ListAIBatchJobs(ctx context.Context, p types.QueryParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if values, err := decodeCursorValues(p.Cursor, 2); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 2 {
		cursorTs, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse completed_at", types.ErrInvalidCursor)
		}
		where += " AND (completed_at < ? OR (completed_at = ? AND request_id < ?))"
		args = append(args, cursorTs.UTC(), cursorTs.UTC(), values[1])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			org,
			gateway,
			pipeline,
			model_id,
			completed_at,
			success,
			tries,
			duration_ms,
			orch_url,
			selection_outcome,
			latency_score,
			price_per_unit,
			error_type,
			error,
			ifNull(gpu_model_name, '')  AS gpu_model_name,
			attribution_status
		FROM naap.api_ai_batch_jobs
		`+where+`
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list ai batch jobs: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchJobRecord
	for rows.Next() {
		var rec types.AIBatchJobRecord
		var successRaw *uint8
		var tries uint16
		var durationMs int64
		if err := rows.Scan(
			&rec.RequestID,
			&rec.Org,
			&rec.Gateway,
			&rec.Pipeline,
			&rec.ModelID,
			&rec.CompletedAt,
			&successRaw,
			&tries,
			&durationMs,
			&rec.OrchURL,
			&rec.SelectionOutcome,
			&rec.LatencyScore,
			&rec.PricePerUnit,
			&rec.ErrorType,
			&rec.Error,
			&rec.GPUModel,
			&rec.AttributionStatus,
		); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list ai batch jobs scan: %w", err)
		}
		rec.Tries = int64(tries)
		rec.DurationMs = durationMs
		if successRaw != nil {
			v := *successRaw == 1
			rec.Success = &v
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list ai batch jobs rows: %w", err)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.AIBatchJobRecord{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(last.CompletedAt.UTC().Format(time.RFC3339Nano), last.RequestID)
	}
	return result, page, nil
}

// GetAIBatchLLMSummary returns per-model LLM aggregates from api_ai_llm_requests (R17).
func (r *Repo) GetAIBatchLLMSummary(ctx context.Context, p types.QueryParams) ([]types.AIBatchLLMSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			model,
			count()                                              AS total_requests,
			toFloat64(countIf(error = '')) / toFloat64(count()) AS success_rate,
			avg(tokens_per_second)                              AS avg_tokens_per_sec,
			avg(ttft_ms)                                        AS avg_ttft_ms,
			avg(total_tokens)                                   AS avg_total_tokens
		FROM naap.api_ai_llm_requests
		`+where+`
		GROUP BY model
		ORDER BY total_requests DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get ai batch llm summary: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchLLMSummary
	for rows.Next() {
		var s types.AIBatchLLMSummary
		var totalRequests uint64
		if err := rows.Scan(
			&s.Model,
			&totalRequests,
			&s.SuccessRate,
			&s.AvgTokensPerSec,
			&s.AvgTTFTMs,
			&s.AvgTotalTokens,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get ai batch llm summary scan: %w", err)
		}
		s.TotalRequests = int64(totalRequests)
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get ai batch llm summary rows: %w", err)
	}
	if result == nil {
		result = []types.AIBatchLLMSummary{}
	}
	return result, nil
}
