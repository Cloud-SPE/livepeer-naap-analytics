package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListModelPerformance returns FPS performance broken down by AI model (MPERF-001).
func (r *Repo) ListModelPerformance(ctx context.Context, p types.QueryParams) ([]types.ModelPerformance, error) {
	start, end := effectiveWindow(p)

	// Build WHERE fragments separately for the two sub-expressions joined
	warmCond := fmt.Sprintf("gi.last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	fpsCond := "fh.hour >= ? AND fh.hour < ?"

	args := []any{}
	if p.Org != "" {
		warmCond += " AND gi.org = ?"
		args = append(args, p.Org)
	}
	args = append(args, start, end)
	if p.Org != "" {
		fpsCond += " AND fh.org = ?"
		args = append(args, p.Org)
	}
	if p.Pipeline != "" {
		fpsCond += " AND fh.pipeline = ?"
		args = append(args, p.Pipeline)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			gi.model_id,
			gi.pipeline,
			sum(fh.inference_fps_sum)               AS fps_sum,
			sum(fh.sample_count)                    AS samp,
			count(DISTINCT gi.orch_address)          AS warm_orchs
		FROM naap.agg_gpu_inventory AS gi FINAL
		JOIN naap.agg_fps_hourly AS fh FINAL
			ON gi.orch_address = fh.orch_address AND gi.pipeline = fh.pipeline
		WHERE `+warmCond+` AND `+fpsCond+`
		GROUP BY gi.model_id, gi.pipeline
		ORDER BY samp DESC
		LIMIT 100
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list model performance: %w", err)
	}
	defer rows.Close()

	var result []types.ModelPerformance
	for rows.Next() {
		var mp types.ModelPerformance
		var fpsSum float64
		var samp, warmOrchs uint64
		if err := rows.Scan(&mp.ModelID, &mp.Pipeline, &fpsSum, &samp, &warmOrchs); err != nil {
			return nil, fmt.Errorf("clickhouse list model performance scan: %w", err)
		}
		mp.AvgFPS = divSafe(fpsSum, float64(samp))
		mp.WarmOrchCount = int64(warmOrchs)
		// P50/P99 not available from hourly aggregates; use AvgFPS as approximation
		mp.P50FPS = mp.AvgFPS
		mp.P99FPS = mp.AvgFPS
		result = append(result, mp)
	}
	return result, rows.Err()
}

// GetModelDetail returns detail for one (pipeline, model) pair (MPERF-002).
func (r *Repo) GetModelDetail(ctx context.Context, modelID string, p types.QueryParams) (*types.ModelDetail, error) {
	start, end := effectiveWindow(p)

	if p.Pipeline == "" {
		// Resolve pipeline from GPU inventory
		row := r.conn.QueryRow(ctx, `
			SELECT pipeline FROM naap.agg_gpu_inventory FINAL
			WHERE model_id = ? LIMIT 1
		`, modelID)
		_ = row.Scan(&p.Pipeline)
	}

	// FPS for orchs with this model
	orchRows, err := r.conn.Query(ctx, `
		SELECT
			gi.orch_address,
			coalesce(os.name, gi.orch_address)                                   AS name,
			gi.last_seen > now() - INTERVAL ? MINUTE                             AS is_warm,
			sum(fh.inference_fps_sum) / greatest(sum(fh.sample_count), 1)        AS avg_fps
		FROM naap.agg_gpu_inventory AS gi FINAL
		LEFT JOIN naap.agg_orch_state AS os FINAL ON gi.orch_address = os.orch_address
		LEFT JOIN naap.agg_fps_hourly AS fh FINAL
			ON gi.orch_address = fh.orch_address
			AND fh.hour >= ? AND fh.hour < ?
		WHERE gi.model_id = ?
		GROUP BY gi.orch_address, name, is_warm
		ORDER BY avg_fps DESC
		LIMIT 100
	`, activeOrchMinutes, start, end, modelID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get model detail orchs: %w", err)
	}
	defer orchRows.Close()

	var orchs []types.OrchModelStats
	var totalFPS float64
	var warmCount int64
	for orchRows.Next() {
		var o types.OrchModelStats
		var isWarm uint8
		if err := orchRows.Scan(&o.Address, &o.Name, &isWarm, &o.AvgFPS); err != nil {
			return nil, fmt.Errorf("clickhouse get model detail orchs scan: %w", err)
		}
		o.IsWarm = isWarm != 0
		if o.IsWarm {
			warmCount++
		}
		totalFPS += o.AvgFPS
		orchs = append(orchs, o)
	}
	if err := orchRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get model detail orchs rows: %w", err)
	}
	if orchs == nil {
		orchs = []types.OrchModelStats{}
	}

	avgFPS := divSafe(totalFPS, float64(len(orchs)))

	return &types.ModelDetail{
		ModelID:       modelID,
		Pipeline:      p.Pipeline,
		AvgFPS:        avgFPS,
		P50FPS:        avgFPS,
		P99FPS:        avgFPS,
		WarmOrchCount: warmCount,
		TotalStreams:   0, // Would require JOIN with stream data
		Orchs:         orchs,
	}, nil
}
