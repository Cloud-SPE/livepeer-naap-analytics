package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetStreamingModels serves GET /v1/streaming/models.
// Consolidates the current capability offer spine, active live sessions, and
// hourly streaming SLA inputs for the live-video-to-video pipeline.
func (r *Repo) GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT s.s_pipeline AS pipeline, s.s_model AS model,
		    s.warm_orch_count, s.gpu_slots,
		    ifNull(d.active_streams, 0) AS active_streams,
		    toInt64(s.gpu_slots) - toInt64(ifNull(d.active_streams, 0)) AS available_capacity,
		    round(ifNull(f.avg_fps, 0), 1) AS avg_fps
		FROM (
		    SELECT canonical_pipeline AS s_pipeline, ifNull(model_id, '') AS s_model,
		           toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		           toInt64(countDistinctIf(gpu_id, gpu_id IS NOT NULL AND gpu_id != '')) AS gpu_slots
		    FROM naap.api_current_capability_offer
		    WHERE canonical_pipeline = 'live-video-to-video'
		      AND capability_family = 'builtin'
		      AND last_seen > now() - INTERVAL 30 MINUTE
		      AND ifNull(model_id, '') != ''
		    GROUP BY s_pipeline, s_model
		) s
		LEFT JOIN (
		    SELECT pipeline AS d_pipeline, ifNull(model_id, '') AS d_model, toInt64(uniqExact(canonical_session_key)) AS active_streams
		    FROM naap.api_current_active_stream_state
		    WHERE pipeline = 'live-video-to-video'
		      AND completed = 0
		      AND last_seen > now() - INTERVAL 30 MINUTE
		    GROUP BY d_pipeline, d_model
		) d ON s.s_pipeline = d.d_pipeline AND s.s_model = d.d_model
		LEFT JOIN (
		    SELECT pipeline_id AS f_pipeline, ifNull(model_id, '') AS f_model,
		           sum(ifNull(output_fps_sum, 0)) / nullIf(sum(ifNull(status_samples, 0)), 0) AS avg_fps
		    FROM naap.api_hourly_streaming_sla
		    WHERE window_start >= now() - INTERVAL 24 HOUR AND pipeline_id = 'live-video-to-video'
		    GROUP BY f_pipeline, f_model
		) f ON s.s_pipeline = f.f_pipeline AND s.s_model = f.f_model
		ORDER BY s.warm_orch_count DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("streaming models: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingModel
	for rows.Next() {
		var m types.StreamingModel
		if err := rows.Scan(&m.Pipeline, &m.Model, &m.WarmOrchCount, &m.GPUSlots,
			&m.ActiveStreams, &m.AvailableCapacity, &m.AvgFPS); err != nil {
			return nil, fmt.Errorf("streaming models scan: %w", err)
		}
		result = append(result, m)
	}
	if result == nil {
		result = []types.StreamingModel{}
	}
	return result, nil
}
