package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// appendBYOCModels fetches BYOC model availability from normalized_worker_lifecycle
// and appends the results to the provided slice.
func appendBYOCModels(ctx context.Context, r *Repo, result []types.ModelAvailability, org string) ([]types.ModelAvailability, error) {
	// Worker lifecycle events are infrequent registration events (not continuously
	// refreshed like orchestrator state). Use a 24-hour window so workers that
	// registered earlier in the day still appear as available.
	byocArgs := []any{}
	byocWhere := "WHERE event_ts > now() - INTERVAL 24 HOUR"
	if org != "" {
		byocWhere += " AND org = ?"
		byocArgs = append(byocArgs, org)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			capability                  AS pipeline,
			ifNull(model, '')           AS model,
			countDistinct(orch_address) AS warm_count,
			avg(price_per_unit)         AS avg_price
		FROM naap.normalized_worker_lifecycle FINAL
		`+byocWhere+`
		  AND capability != ''
		  AND model      != ''
		GROUP BY capability, model
	`, byocArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list models byoc: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pipeline, model string
		var warmCount uint64
		var avgPrice float64
		if err := rows.Scan(&pipeline, &model, &warmCount, &avgPrice); err != nil {
			return nil, fmt.Errorf("clickhouse list models byoc scan: %w", err)
		}
		result = append(result, types.ModelAvailability{
			Pipeline:            pipeline,
			Model:               model,
			WarmOrchCount:       int64(warmCount),
			PriceAvgWeiPerPixel: avgPrice,
		})
	}
	return result, rows.Err()
}

// appendAIBatchModels fetches AI-batch pipeline/model data from normalized_ai_batch_job
// (last 7 days) and appends the results to the provided slice.
func appendAIBatchModels(ctx context.Context, r *Repo, result []types.ModelAvailability, org string) ([]types.ModelAvailability, error) {
	aiBatchArgs := []any{}
	aiBatchWhere := "WHERE subtype = 'ai_batch_request_completed' AND event_ts >= now() - INTERVAL 168 HOUR"
	if org != "" {
		aiBatchWhere += " AND org = ?"
		aiBatchArgs = append(aiBatchArgs, org)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			ifNull(pipeline, '') AS pipeline,
			ifNull(model_id, '') AS model,
			count()              AS job_count,
			avg(latency_score)   AS avg_latency,
			avg(price_per_unit)  AS avg_price
		FROM naap.normalized_ai_batch_job FINAL
		`+aiBatchWhere+`
		  AND pipeline != ''
		GROUP BY pipeline, model_id
	`, aiBatchArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list models ai_batch: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pipeline, model string
		var jobCount uint64
		var avgLatency, avgPrice float64
		if err := rows.Scan(&pipeline, &model, &jobCount, &avgLatency, &avgPrice); err != nil {
			return nil, fmt.Errorf("clickhouse list models ai_batch scan: %w", err)
		}
		result = append(result, types.ModelAvailability{
			Pipeline:            pipeline,
			Model:               model,
			WarmOrchCount:       0,
			PriceAvgWeiPerPixel: avgPrice,
		})
		_ = avgLatency
	}
	return result, rows.Err()
}
