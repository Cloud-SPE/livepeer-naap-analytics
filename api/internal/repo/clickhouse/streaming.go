package clickhouse

import (
	"context"
	"fmt"
	"time"

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

// GetStreamingOrchestrators serves GET /v1/streaming/orchestrators.
// Orchestrators offering live-video-to-video in the last 30 minutes.
func (r *Repo) GetStreamingOrchestrators(ctx context.Context) ([]types.StreamingOrchestrator, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
		    o.orch_address AS address,
		    anyLast(o.orchestrator_uri) AS uri,
		    arrayDistinct(groupArrayIf(p.model_id, ifNull(p.model_id, '') != '')) AS models,
		    toInt64(countDistinctIf(p.gpu_id, ifNull(p.gpu_id, '') != '')) AS gpu_count,
		    max(o.last_seen) AS last_seen
		FROM naap.api_current_orchestrator o
		INNER JOIN naap.api_current_capability_offer p
		    ON o.orch_address = p.orch_address
		WHERE o.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.canonical_pipeline = 'live-video-to-video'
		  AND p.capability_family = 'builtin'
		GROUP BY o.orch_address
		HAVING length(models) > 0
		ORDER BY gpu_count DESC, address ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("streaming orchestrators: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingOrchestrator
	for rows.Next() {
		var o types.StreamingOrchestrator
		if err := rows.Scan(&o.Address, &o.URI, &o.Models, &o.GPUCount, &o.LastSeen); err != nil {
			return nil, fmt.Errorf("streaming orchestrators scan: %w", err)
		}
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

// ListStreamingSLA serves GET /v1/streaming/sla.
func (r *Repo) ListStreamingSLA(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, string, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	cursorTs, cursorOrch, err := decodeTimeAddrCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}

	query := `
		SELECT
		    window_start,
		    org,
		    orchestrator_address,
		    pipeline_id,
		    ifNull(model_id, '') AS model_id,
		    ifNull(gpu_id, '') AS gpu_id,
		    toInt64(requested_sessions) AS requested_sessions,
		    toInt64(startup_success_sessions) AS startup_success_sessions,
		    ifNull(effective_success_rate, 0) AS effective_success_rate,
		    ifNull(no_swap_rate, 0) AS no_swap_rate,
		    ifNull(avg_output_fps, 0) AS avg_output_fps,
		    ifNull(sla_score, 0) AS sla_score
		FROM naap.api_hourly_streaming_sla
		WHERE window_start >= ? AND window_start < ?
		  AND pipeline_id = 'live-video-to-video'
		  AND (? = 0 OR (window_start, orchestrator_address) < (fromUnixTimestamp64Milli(?), ?))
		ORDER BY window_start DESC, orchestrator_address DESC
		LIMIT ?
	`
	cursorMs := int64(0)
	if !cursorTs.IsZero() {
		cursorMs = cursorTs.UnixMilli()
	}
	rows, err := r.conn.Query(ctx, query, start, end, cursorMs, cursorMs, cursorOrch, uint64(limit+1))
	if err != nil {
		return nil, "", fmt.Errorf("streaming sla: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingSLARow
	for rows.Next() {
		var row types.StreamingSLARow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.OrchestratorAddress, &row.PipelineID,
			&row.ModelID, &row.GPUID, &row.RequestedSessions, &row.StartupSuccessSessions,
			&row.EffectiveSuccessRate, &row.NoSwapRate, &row.AvgOutputFPS, &row.SLAScore); err != nil {
			return nil, "", fmt.Errorf("streaming sla scan: %w", err)
		}
		result = append(result, row)
	}

	nextCursor := ""
	if len(result) > limit {
		last := result[limit-1]
		nextCursor = encodeTimeAddrCursor(last.WindowStart, last.OrchestratorAddress)
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingSLARow{}
	}
	return result, nextCursor, nil
}

// ListStreamingDemand serves GET /v1/streaming/demand.
func (r *Repo) ListStreamingDemand(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, string, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	cursorTs, cursorKey, err := decodeTimeAddrCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}

	query := `
		SELECT
		    window_start,
		    org,
		    gateway,
		    pipeline_id,
		    ifNull(model_id, '') AS model_id,
		    toInt64(requested_sessions) AS requested_sessions,
		    toInt64(startup_success_sessions) AS startup_success_sessions,
		    ifNull(effective_success_rate, 0) AS effective_success_rate,
		    ifNull(total_minutes, 0) AS total_minutes,
		    toInt64(no_orch_sessions) AS no_orch_sessions
		FROM naap.api_hourly_streaming_demand
		WHERE window_start >= ? AND window_start < ?
		  AND pipeline_id = 'live-video-to-video'
		  AND (? = 0 OR (window_start, gateway) < (fromUnixTimestamp64Milli(?), ?))
		ORDER BY window_start DESC, gateway DESC
		LIMIT ?
	`
	cursorMs := int64(0)
	if !cursorTs.IsZero() {
		cursorMs = cursorTs.UnixMilli()
	}
	rows, err := r.conn.Query(ctx, query, start, end, cursorMs, cursorMs, cursorKey, uint64(limit+1))
	if err != nil {
		return nil, "", fmt.Errorf("streaming demand: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingDemandRow
	for rows.Next() {
		var row types.StreamingDemandRow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.Gateway, &row.PipelineID, &row.ModelID,
			&row.RequestedSessions, &row.StartupSuccessSessions, &row.EffectiveSuccessRate,
			&row.TotalMinutes, &row.NoOrchSessions); err != nil {
			return nil, "", fmt.Errorf("streaming demand scan: %w", err)
		}
		result = append(result, row)
	}

	nextCursor := ""
	if len(result) > limit {
		last := result[limit-1]
		nextCursor = encodeTimeAddrCursor(last.WindowStart, last.Gateway)
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingDemandRow{}
	}
	return result, nextCursor, nil
}

// ListStreamingGPUMetrics serves GET /v1/streaming/gpu-metrics.
func (r *Repo) ListStreamingGPUMetrics(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, string, error) {
	start, end := defaultWindow(p)
	limit := normalizeLimit(p.Limit)

	cursorTs, cursorKey, err := decodeTimeAddrCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}

	query := `
		SELECT
		    window_start,
		    org,
		    orchestrator_address,
		    pipeline_id,
		    ifNull(model_id, '') AS model_id,
		    ifNull(gpu_id, '') AS gpu_id,
		    ifNull(gpu_model_name, '') AS gpu_model_name,
		    toInt64(known_sessions_count) AS known_sessions_count,
		    toInt64(startup_success_sessions) AS startup_success_sessions,
		    ifNull(avg_output_fps, 0) AS avg_output_fps,
		    ifNull(avg_e2e_latency_ms, 0) AS avg_e2e_latency_ms,
		    ifNull(swap_rate, 0) AS swap_rate
		FROM naap.api_hourly_streaming_gpu_metrics
		WHERE window_start >= ? AND window_start < ?
		  AND pipeline_id = 'live-video-to-video'
		  AND (? = 0 OR (window_start, gpu_id) < (fromUnixTimestamp64Milli(?), ?))
		ORDER BY window_start DESC, gpu_id DESC
		LIMIT ?
	`
	cursorMs := int64(0)
	if !cursorTs.IsZero() {
		cursorMs = cursorTs.UnixMilli()
	}
	rows, err := r.conn.Query(ctx, query, start, end, cursorMs, cursorMs, cursorKey, uint64(limit+1))
	if err != nil {
		return nil, "", fmt.Errorf("streaming gpu metrics: %w", err)
	}
	defer rows.Close()

	var result []types.StreamingGPUMetricRow
	for rows.Next() {
		var row types.StreamingGPUMetricRow
		if err := rows.Scan(&row.WindowStart, &row.Org, &row.OrchestratorAddress, &row.PipelineID,
			&row.ModelID, &row.GPUID, &row.GPUModelName, &row.KnownSessionsCount, &row.StartupSuccessSessions,
			&row.AvgOutputFPS, &row.AvgE2ELatencyMs, &row.SwapRate); err != nil {
			return nil, "", fmt.Errorf("streaming gpu metrics scan: %w", err)
		}
		result = append(result, row)
	}

	nextCursor := ""
	if len(result) > limit {
		last := result[limit-1]
		nextCursor = encodeTimeAddrCursor(last.WindowStart, last.GPUID)
		result = result[:limit]
	}
	if result == nil {
		result = []types.StreamingGPUMetricRow{}
	}
	return result, nextCursor, nil
}

// Shared cursor helpers for (time, string) keyset pagination.
func encodeTimeAddrCursor(t time.Time, addr string) string {
	return encodeCursorValues(fmt.Sprintf("%d", t.UnixMilli()), addr)
}

func decodeTimeAddrCursor(cursor string) (time.Time, string, error) {
	vals, err := decodeCursorValues(cursor, 2)
	if err != nil {
		return time.Time{}, "", err
	}
	if vals == nil {
		return time.Time{}, "", nil
	}
	var ms int64
	_, err = fmt.Sscanf(vals[0], "%d", &ms)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: bad timestamp", types.ErrInvalidCursor)
	}
	return time.UnixMilli(ms).UTC(), vals[1], nil
}
