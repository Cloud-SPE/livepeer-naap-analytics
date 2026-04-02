package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetAIBatchSummary returns per-pipeline aggregates from normalized_ai_batch_job (R17).
func (r *Repo) GetAIBatchSummary(ctx context.Context, p types.QueryParams) ([]types.AIBatchJobSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE subtype = 'ai_batch_request_completed' AND event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			count()                                              AS total_jobs,
			toFloat64(countIf(success = 1)) / toFloat64(count()) AS success_rate,
			avg(duration_ms)                                     AS avg_duration_ms,
			avg(latency_score)                                   AS avg_latency_score
		FROM naap.normalized_ai_batch_job FINAL
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
		var totalJobs uint64
		if err := rows.Scan(&s.Pipeline, &totalJobs, &s.SuccessRate, &s.AvgDurationMs, &s.AvgLatency); err != nil {
			return nil, fmt.Errorf("clickhouse get ai batch summary scan: %w", err)
		}
		s.TotalJobs = int64(totalJobs)
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

// ListAIBatchJobs returns paginated completed AI batch jobs (R17).
func (r *Repo) ListAIBatchJobs(ctx context.Context, p types.QueryParams) ([]types.AIBatchJobRecord, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)
	offset := p.Offset

	where := "WHERE subtype = 'ai_batch_request_completed' AND event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit, offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			org,
			gateway,
			pipeline,
			model_id,
			event_ts,
			success,
			tries,
			duration_ms,
			orch_url,
			latency_score,
			price_per_unit,
			error_type,
			error
		FROM naap.normalized_ai_batch_job FINAL
		`+where+`
		ORDER BY event_ts DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list ai batch jobs: %w", err)
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
			&rec.LatencyScore,
			&rec.PricePerUnit,
			&rec.ErrorType,
			&rec.Error,
		); err != nil {
			return nil, fmt.Errorf("clickhouse list ai batch jobs scan: %w", err)
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
		return nil, fmt.Errorf("clickhouse list ai batch jobs rows: %w", err)
	}
	if result == nil {
		result = []types.AIBatchJobRecord{}
	}
	return result, nil
}

// GetAIBatchLLMSummary returns per-model LLM aggregates from normalized_ai_llm_request (R17).
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
		FROM naap.normalized_ai_llm_request FINAL
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

// GetBYOCSummary returns per-capability aggregates from normalized_byoc_job (R18).
func (r *Repo) GetBYOCSummary(ctx context.Context, p types.QueryParams) ([]types.BYOCJobSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE subtype = 'job_gateway_completed' AND event_ts >= ? AND event_ts < ?"
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
		FROM naap.normalized_byoc_job FINAL
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

// ListBYOCJobs returns paginated completed BYOC jobs (R18).
func (r *Repo) ListBYOCJobs(ctx context.Context, p types.QueryParams) ([]types.BYOCJobRecord, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)
	offset := p.Offset

	where := "WHERE subtype = 'job_gateway_completed' AND event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit, offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			org,
			capability,
			event_ts,
			success,
			duration_ms,
			http_status,
			orch_address,
			orch_url,
			worker_url,
			error
		FROM naap.normalized_byoc_job FINAL
		`+where+`
		ORDER BY event_ts DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list byoc jobs: %w", err)
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
		); err != nil {
			return nil, fmt.Errorf("clickhouse list byoc jobs scan: %w", err)
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
		return nil, fmt.Errorf("clickhouse list byoc jobs rows: %w", err)
	}
	if result == nil {
		result = []types.BYOCJobRecord{}
	}
	return result, nil
}

// GetBYOCWorkers returns per-capability worker inventory (R18).
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
			uniq(orch_address)                          AS worker_count,
			groupUniqArrayIf(model, model != '')        AS models,
			avg(price_per_unit)      AS avg_price_per_unit
		FROM naap.normalized_worker_lifecycle FINAL
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

// GetBYOCAuthSummary returns per-capability auth event aggregates (R18).
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
		FROM naap.normalized_byoc_auth FINAL
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
