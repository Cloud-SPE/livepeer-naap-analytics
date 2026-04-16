package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetRequestsModels serves GET /v1/requests/models.
// Non-streaming capacity and 24h stats come from the capability-aware offer
// and request-demand surfaces. The public job_type field is derived here from
// capability_family rather than being treated as a base storage dimension.
func (r *Repo) GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error) {
	rows, err := r.conn.Query(ctx, `
		WITH offers AS (
		    SELECT
		        ifNull(canonical_pipeline, capability_name) AS pipeline,
		        model_id AS model,
		        capability_family,
		        toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		        toInt64(count(DISTINCT nullIf(ifNull(gpu_id, ''), ''))) AS gpu_slots
		    FROM naap.api_current_capability_offer
		    WHERE supports_request = 1
		      AND capability_family = 'builtin'
		      AND model_id IS NOT NULL AND model_id != ''
		      AND ifNull(canonical_pipeline, '') != 'live-video-to-video'
		      AND ifNull(canonical_pipeline, capability_name) != ''
		      AND last_seen > now() - INTERVAL 30 MINUTE
		    GROUP BY pipeline, model, capability_family
		    UNION ALL
		    SELECT
		        capability_name AS pipeline,
		        ifNull(model, '') AS model,
		        'byoc' AS capability_family,
		        toInt64(uniqExact(orch_address)) AS warm_orch_count,
		        toInt64(0) AS gpu_slots
		    FROM naap.api_current_byoc_worker
		    WHERE capability_name != ''
		      AND last_seen > now() - INTERVAL 30 MINUTE
		    GROUP BY pipeline, model, capability_family
		),
		demand AS (
		    SELECT
		        if(capability_family = 'byoc', capability_name, ifNull(canonical_pipeline, capability_name)) AS pipeline,
		        canonical_model AS model,
		        capability_family,
		        toInt64(sum(job_count)) AS job_count_24h,
		        sum(success_count) / nullIf(sum(job_count), 0) AS success_rate,
		        sum(duration_ms_sum) / nullIf(sum(job_count), 0) AS avg_duration_ms
		    FROM naap.api_hourly_request_demand
		    WHERE window_start >= now() - INTERVAL 24 HOUR
		    GROUP BY pipeline, model, capability_family
		)
		SELECT
		    o.pipeline,
		    o.model,
		    if(o.capability_family = 'byoc', 'byoc', 'ai-batch') AS job_type,
		    o.warm_orch_count,
		    o.gpu_slots,
		    ifNull(d.job_count_24h, 0) AS job_count_24h,
		    round(ifNull(d.success_rate, 0), 3) AS success_rate,
		    round(ifNull(d.avg_duration_ms, 0), 0) AS avg_duration_ms
		FROM offers o
		LEFT JOIN demand d
		  ON d.pipeline = o.pipeline
		 AND ifNull(d.model, '') = ifNull(o.model, '')
		 AND d.capability_family = o.capability_family
		ORDER BY job_count_24h DESC, warm_orch_count DESC, pipeline ASC, model ASC
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
// Orchestrators that advertise any non-streaming capability in the last 30m.
func (r *Repo) GetRequestsOrchestrators(ctx context.Context) ([]types.RequestsOrchestrator, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
		    o.orch_address AS address,
		    anyLast(o.orchestrator_uri) AS uri,
		    arrayDistinct(groupArrayIf(
		        concat(
		            if(p.capability_family = 'byoc', p.offered_name, ifNull(p.canonical_pipeline, p.capability_name)),
		            '/',
		            ifNull(p.model_id, '')
		        ),
		        ifNull(p.model_id, '') != ''
		    )) AS capabilities,
		    toInt64(countDistinctIf(p.gpu_id, ifNull(p.gpu_id, '') != '')) AS gpu_count,
		    max(o.last_seen) AS last_seen
		FROM naap.api_current_orchestrator o
		INNER JOIN naap.api_current_capability_offer p
		    ON o.orch_address = p.orch_address
		WHERE o.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.supports_request = 1
		  AND ifNull(p.canonical_pipeline, '') != 'live-video-to-video'
		GROUP BY o.orch_address
		HAVING length(capabilities) > 0
		ORDER BY gpu_count DESC, address ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("requests orchestrators: %w", err)
	}
	defer rows.Close()

	var result []types.RequestsOrchestrator
	for rows.Next() {
		var o types.RequestsOrchestrator
		if err := rows.Scan(&o.Address, &o.URI, &o.Capabilities, &o.GPUCount, &o.LastSeen); err != nil {
			return nil, fmt.Errorf("requests orchestrators scan: %w", err)
		}
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
		SELECT
		    pipeline,
		    toInt64(count()) AS total_jobs,
		    toInt64(countIf(selection_outcome = 'selected')) AS selected_jobs,
		    toInt64(countIf(selection_outcome = 'no_orch')) AS no_orch_jobs,
		    countIf(success = 1) / toFloat64(count()) AS success_rate,
		    sum(toInt64(ifNull(duration_ms, 0))) / toFloat64(count()) AS avg_duration_ms
		FROM naap.api_fact_ai_batch_job
		WHERE completed_at >= ? AND completed_at < ?
		  AND pipeline != ''
		GROUP BY pipeline
		ORDER BY total_jobs DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("ai batch summary: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchSummaryRow
	for rows.Next() {
		var row types.AIBatchSummaryRow
		if err := rows.Scan(&row.Pipeline, &row.TotalJobs, &row.SelectedJobs, &row.NoOrchJobs,
			&row.SuccessRate, &row.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("ai batch summary scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.AIBatchSummaryRow{}
	}
	return result, nil
}

// ListAIBatchJobs serves GET /v1/requests/ai-batch/jobs.
func (r *Repo) ListAIBatchJobs(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, string, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	cursorTs, cursorKey, err := decodeTimeAddrCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	cursorMs := int64(0)
	if !cursorTs.IsZero() {
		cursorMs = cursorTs.UnixMilli()
	}

	query := `
		SELECT
		    ifNull(request_id, '') AS request_id,
		    org,
		    ifNull(gateway, '') AS gateway,
		    ifNull(pipeline, '') AS pipeline,
		    ifNull(model_id, '') AS model_id,
		    completed_at,
		    success,
		    toInt64(ifNull(tries, 0)) AS tries,
		    toInt64(ifNull(duration_ms, 0)) AS duration_ms,
		    ifNull(orch_url, '') AS orch_url,
		    ifNull(selection_outcome, '') AS selection_outcome,
		    ifNull(gpu_model_name, '') AS gpu_model_name,
		    ifNull(attribution_status, '') AS attribution_status
		FROM naap.api_fact_ai_batch_job
		WHERE completed_at >= ? AND completed_at < ?
		  AND (? = 0 OR (completed_at, request_id) < (fromUnixTimestamp64Milli(?), ?))
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`

	rows, err := r.conn.Query(ctx, query, start, end, cursorMs, cursorMs, cursorKey, uint64(limit+1))
	if err != nil {
		return nil, "", fmt.Errorf("ai batch jobs: %w", err)
	}
	defer rows.Close()

	var result []types.AIBatchJobRecord
	for rows.Next() {
		var row types.AIBatchJobRecord
		var success uint8
		if err := rows.Scan(&row.RequestID, &row.Org, &row.Gateway, &row.Pipeline, &row.ModelID,
			&row.CompletedAt, &success, &row.Tries, &row.DurationMs, &row.OrchURL,
			&row.SelectionOutcome, &row.GPUModelName, &row.AttributionStatus); err != nil {
			return nil, "", fmt.Errorf("ai batch jobs scan: %w", err)
		}
		row.Success = success == 1
		result = append(result, row)
	}

	nextCursor := ""
	if len(result) > limit {
		last := result[limit-1]
		nextCursor = encodeTimeAddrCursor(last.CompletedAt, last.RequestID)
		result = result[:limit]
	}
	if result == nil {
		result = []types.AIBatchJobRecord{}
	}
	return result, nextCursor, nil
}

// GetAIBatchLLMSummary serves GET /v1/requests/ai-batch/llm-summary.
func (r *Repo) GetAIBatchLLMSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error) {
	start, end := defaultWindow(p)
	rows, err := r.conn.Query(ctx, `
		SELECT
		    ifNull(model, '') AS model,
		    toInt64(count()) AS total_requests,
		    countIf(ifNull(error, '') = '') / toFloat64(count()) AS success_rate,
		    avg(ifNull(tokens_per_second, 0)) AS avg_tokens_per_sec,
		    avg(ifNull(ttft_ms, 0)) AS avg_ttft_ms,
		    avg(ifNull(total_tokens, 0)) AS avg_total_tokens
		FROM naap.api_fact_ai_batch_llm_request
		WHERE event_ts >= ? AND event_ts < ?
		  AND ifNull(model, '') != ''
		GROUP BY model
		ORDER BY total_requests DESC
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
		SELECT
		    capability,
		    toInt64(count()) AS total_jobs,
		    toInt64(countIf(selection_outcome = 'selected')) AS selected_jobs,
		    toInt64(countIf(selection_outcome = 'no_orch')) AS no_orch_jobs,
		    countIf(success = 1) / toFloat64(count()) AS success_rate,
		    sum(toInt64(ifNull(duration_ms, 0))) / toFloat64(count()) AS avg_duration_ms
		FROM naap.api_fact_byoc_job
		WHERE completed_at >= ? AND completed_at < ?
		  AND capability != ''
		GROUP BY capability
		ORDER BY total_jobs DESC
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("byoc summary: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCSummaryRow
	for rows.Next() {
		var row types.BYOCSummaryRow
		if err := rows.Scan(&row.Capability, &row.TotalJobs, &row.SelectedJobs, &row.NoOrchJobs,
			&row.SuccessRate, &row.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("byoc summary scan: %w", err)
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.BYOCSummaryRow{}
	}
	return result, nil
}

// ListBYOCJobs serves GET /v1/requests/byoc/jobs.
func (r *Repo) ListBYOCJobs(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, string, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	cursorTs, cursorKey, err := decodeTimeAddrCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	cursorMs := int64(0)
	if !cursorTs.IsZero() {
		cursorMs = cursorTs.UnixMilli()
	}

	query := `
		SELECT
		    ifNull(request_id, '') AS request_id,
		    org,
		    ifNull(capability, '') AS capability,
		    ifNull(model, '') AS model,
		    completed_at,
		    success,
		    toInt64(ifNull(duration_ms, 0)) AS duration_ms,
		    toInt64(ifNull(http_status, 0)) AS http_status,
		    ifNull(orch_address, '') AS orch_address,
		    ifNull(orch_url, '') AS orch_url,
		    ifNull(worker_url, '') AS worker_url,
		    ifNull(selection_outcome, '') AS selection_outcome,
		    ifNull(gpu_model_name, '') AS gpu_model_name,
		    ifNull(attribution_status, '') AS attribution_status,
		    ifNull(error, '') AS error
		FROM naap.api_fact_byoc_job
		WHERE completed_at >= ? AND completed_at < ?
		  AND (? = 0 OR (completed_at, request_id) < (fromUnixTimestamp64Milli(?), ?))
		ORDER BY completed_at DESC, request_id DESC
		LIMIT ?
	`

	rows, err := r.conn.Query(ctx, query, start, end, cursorMs, cursorMs, cursorKey, uint64(limit+1))
	if err != nil {
		return nil, "", fmt.Errorf("byoc jobs: %w", err)
	}
	defer rows.Close()

	var result []types.BYOCJobRecord
	for rows.Next() {
		var row types.BYOCJobRecord
		var success uint8
		if err := rows.Scan(&row.RequestID, &row.Org, &row.Capability, &row.Model,
			&row.CompletedAt, &success, &row.DurationMs, &row.HTTPStatus,
			&row.OrchAddress, &row.OrchURL, &row.WorkerURL, &row.SelectionOutcome,
			&row.GPUModelName, &row.AttributionStatus, &row.Error); err != nil {
			return nil, "", fmt.Errorf("byoc jobs scan: %w", err)
		}
		row.Success = success == 1
		result = append(result, row)
	}

	nextCursor := ""
	if len(result) > limit {
		last := result[limit-1]
		nextCursor = encodeTimeAddrCursor(last.CompletedAt, last.RequestID)
		result = result[:limit]
	}
	if result == nil {
		result = []types.BYOCJobRecord{}
	}
	return result, nextCursor, nil
}

// GetBYOCWorkers serves GET /v1/requests/byoc/workers.
func (r *Repo) GetBYOCWorkers(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCWorkerRow, error) {
	start, end := defaultWindow(p)
	rows, err := r.conn.Query(ctx, `
		SELECT
		    capability_name AS capability,
		    toInt64(uniqExact(worker_url)) AS worker_count,
		    arrayDistinct(groupArrayIf(ifNull(model, ''), ifNull(model, '') != '')) AS models,
		    avg(ifNull(price_per_unit, 0)) AS avg_price_per_unit
		FROM naap.api_current_byoc_worker
		WHERE last_seen >= ? AND last_seen < ?
		  AND capability_name != ''
		GROUP BY capability
		ORDER BY worker_count DESC
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
		SELECT
		    capability,
		    toInt64(sum(ev)) AS total_events,
		    sum(ok) / nullIf(toFloat64(sum(ev)), 0) AS success_rate,
		    toInt64(sum(bad)) AS failure_count
		FROM (
		    SELECT
		        capability_name AS capability,
		        total_events AS ev,
		        success_count AS ok,
		        failure_count AS bad
		    FROM naap.api_hourly_byoc_auth
		    WHERE window_start >= ? AND window_start < ?
		      AND capability_name != ''
		) t
		GROUP BY capability
		ORDER BY total_events DESC
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
