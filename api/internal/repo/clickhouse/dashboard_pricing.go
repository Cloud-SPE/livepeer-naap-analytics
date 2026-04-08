package clickhouse

import (
	"context"
	"fmt"
	"sort"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetDashboardPricing returns raw wei pricing per pipeline+model (R16-6).
// Merges streaming pricing (from raw_capabilities JSON), BYOC pricing, and AI-batch pricing.
func (r *Repo) GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error) {
	rows, err := r.conn.Query(ctx,
		"SELECT raw_capabilities FROM naap.api_latest_orchestrator_state WHERE last_seen > now() - INTERVAL ? MINUTE",
		activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing: %w", err)
	}
	defer rows.Close()

	agg := map[modelKey]*modelAgg{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pricing scan: %w", err)
		}
		parseModelsFromCapabilities(raw, agg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing rows: %w", err)
	}

	result := make([]types.DashboardPipelinePricing, 0, len(agg))
	for k, a := range agg {
		if len(a.prices) == 0 {
			continue
		}
		var sum int64
		mn, mx := a.prices[0], a.prices[0]
		for _, p := range a.prices {
			sum += p
			if p < mn {
				mn = p
			}
			if p > mx {
				mx = p
			}
		}
		result = append(result, types.DashboardPipelinePricing{
			Pipeline:           k.pipeline,
			Model:              k.model,
			OrchCount:          a.warmCount,
			PriceMinWeiPerUnit: mn,
			PriceMaxWeiPerUnit: mx,
			PriceAvgWeiPerUnit: float64(sum) / float64(len(a.prices)),
			PixelsPerUnit:      1,
		})
	}

	// Second query: BYOC pricing from canonical_byoc_jobs
	byocRows, err := r.conn.Query(ctx, `
		SELECT
			capability                          AS pipeline,
			ifNull(model, '')                   AS model,
			countDistinct(orch_address)         AS orch_count,
			avg(price_per_unit)                 AS avg_price,
			min(price_per_unit)                 AS min_price,
			max(price_per_unit)                 AS max_price
		FROM naap.canonical_byoc_jobs
		WHERE price_per_unit > 0
		GROUP BY capability, model
	`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing byoc: %w", err)
	}
	defer byocRows.Close()

	for byocRows.Next() {
		var pipeline, model string
		var orchCount uint64
		var avgP, minP, maxP float64
		if err := byocRows.Scan(&pipeline, &model, &orchCount, &avgP, &minP, &maxP); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pricing byoc scan: %w", err)
		}
		result = append(result, types.DashboardPipelinePricing{
			Pipeline:           pipeline,
			Model:              model,
			OrchCount:          int64(orchCount),
			PriceMinWeiPerUnit: int64(minP),
			PriceMaxWeiPerUnit: int64(maxP),
			PriceAvgWeiPerUnit: avgP,
			PixelsPerUnit:      1,
		})
	}
	if err := byocRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing byoc rows: %w", err)
	}

	// Third query: AI-batch pricing from canonical_ai_batch_jobs
	aiBatchRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			ifNull(model_id, '')                AS model,
			countDistinct(orch_url_norm)        AS orch_count,
			avg(price_per_unit)                 AS avg_price,
			min(price_per_unit)                 AS min_price,
			max(price_per_unit)                 AS max_price
		FROM naap.canonical_ai_batch_jobs
		WHERE price_per_unit > 0
		GROUP BY pipeline, model_id
	`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing ai_batch: %w", err)
	}
	defer aiBatchRows.Close()

	for aiBatchRows.Next() {
		var pipeline, model string
		var orchCount uint64
		var avgP, minP, maxP float64
		if err := aiBatchRows.Scan(&pipeline, &model, &orchCount, &avgP, &minP, &maxP); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pricing ai_batch scan: %w", err)
		}
		result = append(result, types.DashboardPipelinePricing{
			Pipeline:           pipeline,
			Model:              model,
			OrchCount:          int64(orchCount),
			PriceMinWeiPerUnit: int64(minP),
			PriceMaxWeiPerUnit: int64(maxP),
			PriceAvgWeiPerUnit: avgP,
			PixelsPerUnit:      1,
		})
	}
	if err := aiBatchRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing ai_batch rows: %w", err)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].OrchCount > result[j].OrchCount })
	return result, nil
}
