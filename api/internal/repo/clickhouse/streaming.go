package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetStreamingModels serves GET /v1/streaming/models.
// Consolidates orchestrator pipeline models, active streams, and SLA FPS data.
func (r *Repo) GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT s.s_pipeline AS pipeline, s.s_model AS model,
		    s.warm_orch_count, s.gpu_slots,
		    ifNull(d.active_streams, 0) AS active_streams,
		    toInt64(s.gpu_slots) - toInt64(ifNull(d.active_streams, 0)) AS available_capacity,
		    round(ifNull(f.avg_fps, 0), 1) AS avg_fps
		FROM (
		    SELECT pipeline_id AS s_pipeline, model_id AS s_model,
		           toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		           toInt64(count(DISTINCT ifNull(gpu_id, ''))) AS gpu_slots
		    FROM naap.api_latest_orchestrator_pipeline_models
		    WHERE pipeline_id = 'live-video-to-video'
		      AND last_seen > now() - INTERVAL 30 MINUTE AND model_id != ''
		    GROUP BY s_pipeline, s_model
		) s
		LEFT JOIN (
		    SELECT pipeline AS d_pipeline, ifNull(model_id, '') AS d_model, toInt64(count()) AS active_streams
		    FROM naap.api_active_stream_state WHERE pipeline = 'live-video-to-video' AND completed = 0
		    GROUP BY d_pipeline, d_model
		) d ON s.s_pipeline = d.d_pipeline AND s.s_model = d.d_model
		LEFT JOIN (
		    SELECT pipeline_id AS f_pipeline, ifNull(model_id, '') AS f_model,
		           sum(ifNull(avg_output_fps, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS avg_fps
		    FROM naap.api_sla_compliance
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

// GetStreamingOrchestrators serves GET /v1/streaming/orchestrators.
func (r *Repo) GetStreamingOrchestrators(ctx context.Context) ([]types.StreamingOrchestrator, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT o.orch_address AS address, anyLast(o.uri) AS uri,
		    arrayDistinct(groupArray(p.model_id)) AS models,
		    toInt64(count(DISTINCT ifNull(p.gpu_id, ''))) AS gpu_count,
		    max(o.last_seen) AS last_seen
		FROM naap.api_latest_orchestrator_state o
		INNER JOIN naap.api_latest_orchestrator_pipeline_models p ON o.orch_address = p.orch_address
		WHERE p.pipeline_id = 'live-video-to-video'
		  AND o.last_seen > now() - INTERVAL 30 MINUTE AND p.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.model_id != ''
		GROUP BY o.orch_address
		ORDER BY gpu_count DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("streaming orchestrators: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingOrchestrator
	for rows.Next() {
		var o types.StreamingOrchestrator
		var lastSeen time.Time
		if err := rows.Scan(&o.Address, &o.URI, &o.Models, &o.GPUCount, &lastSeen); err != nil {
			return nil, fmt.Errorf("streaming orchestrators scan: %w", err)
		}
		o.LastSeen = lastSeen.Format(time.RFC3339)
		if o.Models == nil {
			o.Models = []string{}
		}
		result = append(result, o)
	}
	if result == nil {
		result = []types.StreamingOrchestrator{}
	}
	return result, nil
}

// ListStreamingSLA serves GET /v1/streaming/sla (cursor-paginated).
func (r *Repo) ListStreamingSLA(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, types.CursorPageInfo, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	where := "WHERE window_start >= ? AND window_start < ?"
	args := []any{start, end}

	if values, err := decodeCursorValues(p.Cursor, 4); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 4 {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, orchestrator_address, pipeline_id, model_id) < (?, ?, ?, ?)"
		args = append(args, cursorTime.UTC(), values[1], values[2], values[3])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT window_start, org, orchestrator_address, pipeline_id,
		    ifNull(model_id, '') AS model_id, ifNull(gpu_id, '') AS gpu_id,
		    requested_sessions, startup_success_sessions,
		    ifNull(effective_success_rate, 0) AS effective_success_rate,
		    ifNull(no_swap_rate, 0) AS no_swap_rate,
		    ifNull(avg_output_fps, 0) AS avg_output_fps,
		    ifNull(sla_score, 0) AS sla_score
		FROM naap.api_sla_compliance `+where+`
		ORDER BY window_start DESC, orchestrator_address DESC, pipeline_id DESC, ifNull(model_id, '') DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("streaming sla: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingSLARow
	for rows.Next() {
		var row types.StreamingSLARow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.OrchestratorAddress,
			&row.PipelineID, &row.ModelID, &row.GPUID,
			&row.RequestedSessions, &row.StartupSuccessSessions, &row.EffectiveSuccessRate,
			&row.NoSwapRate, &row.AvgOutputFPS, &row.SLAScore); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("streaming sla scan: %w", err)
		}
		result = append(result, row)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingSLARow{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore && len(result) > 0 {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.OrchestratorAddress,
			last.PipelineID,
			last.ModelID,
		)
	}
	return result, page, nil
}

// ListStreamingDemand serves GET /v1/streaming/demand (cursor-paginated).
func (r *Repo) ListStreamingDemand(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, types.CursorPageInfo, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	where := "WHERE window_start >= ? AND window_start < ?"
	args := []any{start, end}

	if values, err := decodeCursorValues(p.Cursor, 3); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 3 {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, gateway, pipeline_id) < (?, ?, ?)"
		args = append(args, cursorTime.UTC(), values[1], values[2])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT window_start, org, gateway, pipeline_id, model_id,
		    requested_sessions, startup_success_sessions,
		    if(requested_sessions > 0, startup_success_sessions / toFloat64(requested_sessions), 0) AS effective_success_rate,
		    total_minutes, no_orch_sessions
		FROM naap.api_network_demand `+where+`
		ORDER BY window_start DESC, gateway DESC, pipeline_id DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("streaming demand: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingDemandRow
	for rows.Next() {
		var row types.StreamingDemandRow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.Gateway,
			&row.PipelineID, &row.ModelID,
			&row.RequestedSessions, &row.StartupSuccessSessions, &row.EffectiveSuccessRate,
			&row.TotalMinutes, &row.NoOrchSessions); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("streaming demand scan: %w", err)
		}
		result = append(result, row)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingDemandRow{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore && len(result) > 0 {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.Gateway,
			last.PipelineID,
		)
	}
	return result, page, nil
}

// ListStreamingGPUMetrics serves GET /v1/streaming/gpu-metrics (cursor-paginated).
func (r *Repo) ListStreamingGPUMetrics(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, types.CursorPageInfo, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	where := "WHERE window_start >= ? AND window_start < ?"
	args := []any{start, end}

	if values, err := decodeCursorValues(p.Cursor, 3); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 3 {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse window_start", types.ErrInvalidCursor)
		}
		where += " AND (window_start, orchestrator_address, gpu_id) < (?, ?, ?)"
		args = append(args, cursorTime.UTC(), values[1], values[2])
	}

	rows, err := r.conn.Query(ctx, `
		SELECT window_start, org, orchestrator_address, pipeline_id,
		    ifNull(model_id, '') AS model_id, ifNull(gpu_id, '') AS gpu_id,
		    ifNull(gpu_model_name, '') AS gpu_model_name,
		    known_sessions_count, startup_success_sessions,
		    avg_output_fps, ifNull(avg_e2e_latency_ms, 0) AS avg_e2e_latency_ms,
		    ifNull(swap_rate, 0) AS swap_rate
		FROM naap.api_gpu_metrics `+where+`
		ORDER BY window_start DESC, orchestrator_address DESC, ifNull(gpu_id, '') DESC
		LIMIT ?
	`, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("streaming gpu metrics: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingGPUMetricRow
	for rows.Next() {
		var row types.StreamingGPUMetricRow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.OrchestratorAddress,
			&row.PipelineID, &row.ModelID,
			&row.GPUID, &row.GPUModelName, &row.KnownSessionsCount, &row.StartupSuccessSessions,
			&row.AvgOutputFPS, &row.AvgE2ELatencyMs, &row.SwapRate); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("streaming gpu metrics scan: %w", err)
		}
		result = append(result, row)
	}

	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingGPUMetricRow{}
	}

	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(result)}
	if hasMore && len(result) > 0 {
		last := result[len(result)-1]
		page.NextCursor = encodeCursorValues(
			last.WindowStart.UTC().Format(time.RFC3339Nano),
			last.OrchestratorAddress,
			last.GPUID,
		)
	}
	return result, page, nil
}
