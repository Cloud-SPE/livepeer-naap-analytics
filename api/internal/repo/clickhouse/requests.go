package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetRequestsModels serves GET /v1/requests/models.
// Non-streaming pipelines: capacity from pipeline models + job stats from api_requests_sla.
func (r *Repo) GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT s.pipeline_id AS pipeline, s.model_id AS model,
		    ifNull(t.job_type, 'unknown') AS job_type,
		    s.warm_orch_count, s.gpu_slots,
		    ifNull(t.job_count_24h, 0) AS job_count_24h,
		    round(ifNull(t.success_rate, 0), 3) AS success_rate,
		    round(ifNull(t.avg_duration_ms, 0), 0) AS avg_duration_ms
		FROM (
		    SELECT pipeline_id, model_id,
		           toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		           toInt64(count(DISTINCT nullIf(ifNull(gpu_id, ''), ''))) AS gpu_slots
		    FROM naap.api_latest_orchestrator_pipeline_models
		    WHERE pipeline_id != 'live-video-to-video'
		      AND last_seen > now() - INTERVAL 30 MINUTE AND pipeline_id != '' AND model_id != ''
		    GROUP BY pipeline_id, model_id
		) s
		LEFT JOIN (
		    SELECT pipeline_id, model_id, job_type,
		           toInt64(sum(job_count)) AS job_count_24h,
		           sum(success_count) / nullIf(sum(job_count), 0) AS success_rate,
		           sum(avg_duration_ms * job_count) / nullIf(sum(job_count), 0) AS avg_duration_ms
		    FROM naap.api_requests_sla
		    WHERE window_start >= now() - INTERVAL 24 HOUR
		    GROUP BY pipeline_id, model_id, job_type
		) t ON s.pipeline_id = t.pipeline_id AND s.model_id = t.model_id
		ORDER BY job_count_24h DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("requests models: %w", err)
	}
	defer rows.Close()

	var result []types.RequestsModel
	for rows.Next() {
		var m types.RequestsModel
		if err := rows.Scan(&m.Pipeline, &m.Model, &m.JobType,
			&m.WarmOrchCount, &m.GPUSlots,
			&m.JobCount24h, &m.SuccessRate, &m.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("requests models scan: %w", err)
		}
		result = append(result, m)
	}
	if result == nil {
		result = []types.RequestsModel{}
	}
	return result, nil
}

// GetRequestsOrchestrators serves GET /v1/requests/orchestrators.
func (r *Repo) GetRequestsOrchestrators(ctx context.Context) ([]types.RequestsOrchestrator, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT o.orch_address AS address, anyLast(o.uri) AS uri,
		    arrayDistinct(groupArray(p.pipeline_id || ':' || p.model_id)) AS capabilities,
		    toInt64(count(DISTINCT nullIf(ifNull(p.gpu_id, ''), ''))) AS gpu_count,
		    max(o.last_seen) AS last_seen
		FROM naap.api_latest_orchestrator_state o
		INNER JOIN naap.api_latest_orchestrator_pipeline_models p ON o.orch_address = p.orch_address
		WHERE p.pipeline_id != 'live-video-to-video'
		  AND o.last_seen > now() - INTERVAL 30 MINUTE AND p.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.pipeline_id != '' AND p.model_id != ''
		GROUP BY o.orch_address
		ORDER BY gpu_count DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("requests orchestrators: %w", err)
	}
	defer rows.Close()

	var result []types.RequestsOrchestrator
	for rows.Next() {
		var o types.RequestsOrchestrator
		var lastSeen time.Time
		if err := rows.Scan(&o.Address, &o.URI, &o.Capabilities, &o.GPUCount, &lastSeen); err != nil {
			return nil, fmt.Errorf("requests orchestrators scan: %w", err)
		}
		o.LastSeen = lastSeen.Format(time.RFC3339)
		if o.Capabilities == nil {
			o.Capabilities = []string{}
		}
		result = append(result, o)
	}
	if result == nil {
		result = []types.RequestsOrchestrator{}
	}
	return result, nil
}

// GetAIBatchSummary serves GET /v1/requests/ai-batch/summary.
func (r *Repo) GetAIBatchSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchSummaryRow, error) {
	start, end := defaultWindow(p)

	rows, err := r.conn.Query(ctx, `
		SELECT pipeline, toInt64(count()) AS total_jobs,
		    toInt64(countIf(selection_outcome = 'selected')) AS selected_jobs,
		    toInt64(countIf(selection_outcome = 'no_orch')) AS no_orch_jobs,
		    countIf(ifNull(success, 0) = 1) / nullIf(toFloat64(count()), 0) AS success_rate,
		    avg(toFloat64(duration_ms)) AS avg_duration_ms
		FROM naap.api_ai_batch_jobs
		WHERE completed_at >= ? AND completed_at < ?
		GROUP BY pipeline ORDER BY total_jobs DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("ai batch summary: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchSummaryRow
	for rows.Next() {
		var row types.AIBatchSummaryRow
		if err := rows.Scan(&row.Pipeline, &row.TotalJobs, &row.SelectedJobs,
			&row.NoOrchJobs, &row.SuccessRate, &row.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("ai batch summary scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.AIBatchSummaryRow{}
	}
	return result, nil
}

// ListAIBatchJobs serves GET /v1/requests/ai-batch/jobs (cursor-paginated).
func (r *Repo) ListAIBatchJobs(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}

	if values, err := decodeCursorValues(p.Cursor, 2); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 2 {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse completed_at", types.ErrInvalidCursor)
		}
		where += " AND (completed_at, request_id) < (?, ?)"
		args = append(args, cursorTime.UTC(), values[1])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT request_id, org, gateway, pipeline, model_id, completed_at,
		    ifNull(success, 0) AS success, toInt32(tries) AS tries, duration_ms, orch_url_norm AS orch_url,
		    selection_outcome, ifNull(gpu_model_name, '') AS gpu_model_name, attribution_status
		FROM naap.api_ai_batch_jobs `+where+`
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("ai batch jobs: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchJobRecord
	for rows.Next() {
		var row types.AIBatchJobRecord
		var success uint8
		if err := rows.Scan(&row.RequestID, &row.Org, &row.Gateway, &row.Pipeline,
			&row.ModelID, &row.CompletedAt, &success, &row.Tries, &row.DurationMs,
			&row.OrchURL, &row.SelectionOutcome, &row.GPUModelName, &row.AttributionStatus); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("ai batch jobs scan: %w", err)
		}
		row.Success = success == 1
		result = append(result, row)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.AIBatchJobRecord{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore && len(result) > 0 {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.CompletedAt.UTC().Format(time.RFC3339Nano),
			last.RequestID,
		)
	}
	return result, page, nil
}

// GetAIBatchLLMSummary serves GET /v1/requests/ai-batch/llm-summary.
func (r *Repo) GetAIBatchLLMSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error) {
	start, end := defaultWindow(p)

	rows, err := r.conn.Query(ctx, `
		SELECT model, toInt64(count()) AS total_requests,
		    countIf(error = '') / nullIf(toFloat64(count()), 0) AS success_rate,
		    avg(tokens_per_second) AS avg_tokens_per_sec,
		    avg(ttft_ms) AS avg_ttft_ms,
		    avg(total_tokens) AS avg_total_tokens
		FROM naap.api_ai_llm_requests
		WHERE event_ts >= ? AND event_ts < ?
		GROUP BY model ORDER BY total_requests DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("ai batch llm summary: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchLLMSummaryRow
	for rows.Next() {
		var row types.AIBatchLLMSummaryRow
		if err := rows.Scan(&row.Model, &row.TotalRequests, &row.SuccessRate,
			&row.AvgTokensPerSec, &row.AvgTTFTMs, &row.AvgTotalTokens); err != nil {
			return nil, fmt.Errorf("ai batch llm summary scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.AIBatchLLMSummaryRow{}
	}
	return result, nil
}

// GetBYOCSummary serves GET /v1/requests/byoc/summary.
func (r *Repo) GetBYOCSummary(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCSummaryRow, error) {
	start, end := defaultWindow(p)

	rows, err := r.conn.Query(ctx, `
		SELECT capability, toInt64(count()) AS total_jobs,
		    toInt64(countIf(selection_outcome = 'selected')) AS selected_jobs,
		    toInt64(countIf(selection_outcome = 'no_orch')) AS no_orch_jobs,
		    countIf(ifNull(success, 0) = 1) / nullIf(toFloat64(count()), 0) AS success_rate,
		    avg(toFloat64(duration_ms)) AS avg_duration_ms
		FROM naap.api_byoc_jobs
		WHERE completed_at >= ? AND completed_at < ?
		GROUP BY capability ORDER BY total_jobs DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("byoc summary: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCSummaryRow
	for rows.Next() {
		var row types.BYOCSummaryRow
		if err := rows.Scan(&row.Capability, &row.TotalJobs, &row.SelectedJobs,
			&row.NoOrchJobs, &row.SuccessRate, &row.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("byoc summary scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.BYOCSummaryRow{}
	}
	return result, nil
}

// ListBYOCJobs serves GET /v1/requests/byoc/jobs (cursor-paginated).
func (r *Repo) ListBYOCJobs(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	where := "WHERE completed_at >= ? AND completed_at < ?"
	args := []any{start, end}

	if values, err := decodeCursorValues(p.Cursor, 2); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 2 {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse completed_at", types.ErrInvalidCursor)
		}
		where += " AND (completed_at, request_id) < (?, ?)"
		args = append(args, cursorTime.UTC(), values[1])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT request_id, org, capability, ifNull(model, '') AS model, completed_at,
		    ifNull(success, 0) AS success,
		    duration_ms, toInt32(http_status) AS http_status, orch_address, orch_url_norm AS orch_url,
		    worker_url, selection_outcome, ifNull(gpu_model_name, '') AS gpu_model_name,
		    attribution_status, error
		FROM naap.api_byoc_jobs `+where+`
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("byoc jobs: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCJobRecord
	for rows.Next() {
		var row types.BYOCJobRecord
		var success uint8
		if err := rows.Scan(&row.RequestID, &row.Org, &row.Capability, &row.Model,
			&row.CompletedAt, &success, &row.DurationMs, &row.HTTPStatus,
			&row.OrchAddress, &row.OrchURL, &row.WorkerURL,
			&row.SelectionOutcome, &row.GPUModelName, &row.AttributionStatus, &row.Error); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("byoc jobs scan: %w", err)
		}
		row.Success = success == 1
		result = append(result, row)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.BYOCJobRecord{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore && len(result) > 0 {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.CompletedAt.UTC().Format(time.RFC3339Nano),
			last.RequestID,
		)
	}
	return result, page, nil
}

// GetBYOCWorkers serves GET /v1/requests/byoc/workers.
func (r *Repo) GetBYOCWorkers(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCWorkerRow, error) {
	start, end := defaultWindow(p)

	rows, err := r.conn.Query(ctx, `
		SELECT capability, toInt64(count(DISTINCT worker_url)) AS worker_count,
		    groupArray(DISTINCT model) AS models,
		    avg(price_per_unit) AS avg_price_per_unit
		FROM naap.api_byoc_workers
		WHERE event_ts >= ? AND event_ts < ?
		GROUP BY capability ORDER BY worker_count DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("byoc workers: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCWorkerRow
	for rows.Next() {
		var row types.BYOCWorkerRow
		if err := rows.Scan(&row.Capability, &row.WorkerCount, &row.Models, &row.AvgPricePerUnit); err != nil {
			return nil, fmt.Errorf("byoc workers scan: %w", err)
		}
		if row.Models == nil {
			row.Models = []string{}
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.BYOCWorkerRow{}
	}
	return result, nil
}

// GetBYOCAuth serves GET /v1/requests/byoc/auth.
func (r *Repo) GetBYOCAuth(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCAuthRow, error) {
	start, end := defaultWindow(p)

	rows, err := r.conn.Query(ctx, `
		SELECT capability, toInt64(count()) AS total_events,
		    countIf(success = 1) / nullIf(toFloat64(count()), 0) AS success_rate,
		    toInt64(countIf(success = 0)) AS failure_count
		FROM naap.api_byoc_auth
		WHERE event_ts >= ? AND event_ts < ?
		GROUP BY capability ORDER BY total_events DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("byoc auth: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCAuthRow
	for rows.Next() {
		var row types.BYOCAuthRow
		if err := rows.Scan(&row.Capability, &row.TotalEvents, &row.SuccessRate, &row.FailureCount); err != nil {
			return nil, fmt.Errorf("byoc auth scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.BYOCAuthRow{}
	}
	return result, nil
}
