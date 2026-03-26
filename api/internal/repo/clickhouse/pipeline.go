package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListPipelines returns a cross-cutting summary per pipeline (PIPE-001).
// Combines stream, FPS, and payment data aggregated in Go.
func (r *Repo) ListPipelines(ctx context.Context, p types.QueryParams) ([]types.PipelineSummary, error) {
	start, end := effectiveWindow(p)

	orgFilter := ""
	args := []any{start, end}
	if p.Org != "" {
		orgFilter = " AND org = ?"
		args = append(args, p.Org)
	}

	// Stream stats per pipeline
	streamRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			sum(started)   AS started,
			sum(completed) AS completed
		FROM naap.serving_stream_hourly
		WHERE hour >= ? AND hour < ?`+orgFilter+`
		GROUP BY pipeline
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines stream: %w", err)
	}
	defer streamRows.Close()

	type pipeStats struct {
		started   int64
		completed int64
		fpsSumW   float64
		fpsSampW  float64
		payWEI    uint64
		payCount  int64
		warmOrchs int64
		topOrch   string
	}
	pmap := map[string]*pipeStats{}
	for streamRows.Next() {
		var pipeline string
		var started, completed uint64
		if err := streamRows.Scan(&pipeline, &started, &completed); err != nil {
			return nil, fmt.Errorf("clickhouse list pipelines stream scan: %w", err)
		}
		ps := pmap[pipeline]
		if ps == nil {
			ps = &pipeStats{}
			pmap[pipeline] = ps
		}
		ps.started += int64(started)
		ps.completed += int64(completed)
	}
	if err := streamRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines stream rows: %w", err)
	}

	// FPS per pipeline
	fpsRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			sum(inference_fps_sum) AS fps_sum,
			sum(sample_count)      AS samp
		FROM naap.serving_fps_hourly
		WHERE hour >= ? AND hour < ?`+orgFilter+`
		GROUP BY pipeline
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines fps: %w", err)
	}
	defer fpsRows.Close()

	for fpsRows.Next() {
		var pipeline string
		var fpsSum float64
		var samp uint64
		if err := fpsRows.Scan(&pipeline, &fpsSum, &samp); err != nil {
			return nil, fmt.Errorf("clickhouse list pipelines fps scan: %w", err)
		}
		ps := pmap[pipeline]
		if ps == nil {
			ps = &pipeStats{}
			pmap[pipeline] = ps
		}
		ps.fpsSumW += fpsSum
		ps.fpsSampW += float64(samp)
	}
	if err := fpsRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines fps rows: %w", err)
	}

	// Payment per pipeline
	payRows, err := r.conn.Query(ctx, `
		SELECT pipeline, sum(total_wei), sum(event_count)
		FROM naap.serving_payment_hourly
		WHERE hour >= ? AND hour < ?`+orgFilter+`
		GROUP BY pipeline
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines pay: %w", err)
	}
	defer payRows.Close()

	for payRows.Next() {
		var pipeline string
		var wei, cnt uint64
		if err := payRows.Scan(&pipeline, &wei, &cnt); err != nil {
			return nil, fmt.Errorf("clickhouse list pipelines pay scan: %w", err)
		}
		ps := pmap[pipeline]
		if ps == nil {
			ps = &pipeStats{}
			pmap[pipeline] = ps
		}
		ps.payWEI += wei
		ps.payCount += int64(cnt)
	}
	if err := payRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines pay rows: %w", err)
	}

	// Active streams per pipeline.
	// sample_ts filter restores the recency bound that was previously in the view definition.
	activeWhere := fmt.Sprintf("WHERE state = 'ONLINE' AND sample_ts > now() - INTERVAL %d SECOND", activeStreamSecs)
	activeArgs := []any{}
	if p.Org != "" {
		activeWhere += " AND org = ?"
		activeArgs = append(activeArgs, p.Org)
	}
	activeRows, err := r.conn.Query(ctx, `
		SELECT pipeline, count() AS active
		FROM naap.serving_active_stream_state
		`+activeWhere+`
		GROUP BY pipeline
	`, activeArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines active: %w", err)
	}
	defer activeRows.Close()

	activeMap := map[string]int64{}
	for activeRows.Next() {
		var pipeline string
		var active uint64
		if err := activeRows.Scan(&pipeline, &active); err != nil {
			return nil, fmt.Errorf("clickhouse list pipelines active scan: %w", err)
		}
		activeMap[pipeline] = int64(active)
	}
	if err := activeRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines active rows: %w", err)
	}

	// Warm orch count per pipeline from GPU inventory
	warmWhere := fmt.Sprintf("WHERE last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	warmArgs := []any{}
	if p.Org != "" {
		warmWhere += " AND org = ?"
		warmArgs = append(warmArgs, p.Org)
	}
	warmRows, err := r.conn.Query(ctx, `
		SELECT pipeline, count(DISTINCT orch_address) AS warm, any(orch_address) AS top_orch
		FROM naap.serving_gpu_inventory
		`+warmWhere+`
		GROUP BY pipeline
	`, warmArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines warm: %w", err)
	}
	defer warmRows.Close()

	for warmRows.Next() {
		var pipeline, topOrch string
		var warm uint64
		if err := warmRows.Scan(&pipeline, &warm, &topOrch); err != nil {
			return nil, fmt.Errorf("clickhouse list pipelines warm scan: %w", err)
		}
		ps := pmap[pipeline]
		if ps == nil {
			ps = &pipeStats{}
			pmap[pipeline] = ps
		}
		ps.warmOrchs = int64(warm)
		ps.topOrch = topOrch
	}
	if err := warmRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pipelines warm rows: %w", err)
	}

	// Build result
	result := make([]types.PipelineSummary, 0, len(pmap))
	for pipeline, ps := range pmap {
		s := types.PipelineSummary{
			Pipeline:         pipeline,
			ActiveStreams:    activeMap[pipeline],
			StartedCount:     ps.started,
			SuccessRate:      divSafe(float64(ps.completed), float64(ps.started)),
			AvgInferenceFPS:  divSafe(ps.fpsSumW, ps.fpsSampW),
			TotalPaymentsWEI: types.WEI(ps.payWEI),
			WarmOrchCount:    ps.warmOrchs,
			TopOrchAddress:   ps.topOrch,
		}
		result = append(result, s)
	}
	return result, nil
}

// GetPipelineDetail returns detailed stats for one pipeline (PIPE-002).
func (r *Repo) GetPipelineDetail(ctx context.Context, pipeline string, p types.QueryParams) (*types.PipelineDetail, error) {
	start, end := effectiveWindow(p)

	where := "WHERE pipeline = ? AND hour >= ? AND hour < ?"
	args := []any{pipeline, start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	// Stream summary
	streamRow := r.conn.QueryRow(ctx, `
		SELECT sum(started), sum(completed)
		FROM naap.serving_stream_hourly
		`+where, args...)

	var started, completed uint64
	if err := streamRow.Scan(&started, &completed); err != nil {
		return nil, fmt.Errorf("clickhouse get pipeline detail streams: %w", err)
	}

	// FPS
	fpsRow := r.conn.QueryRow(ctx, `
		SELECT sum(inference_fps_sum), sum(sample_count)
		FROM naap.serving_fps_hourly
		`+where, args...)
	var fpsSum float64
	var fpsSamp uint64
	if err := fpsRow.Scan(&fpsSum, &fpsSamp); err != nil {
		return nil, fmt.Errorf("clickhouse get pipeline detail fps: %w", err)
	}

	// Payments
	payRow := r.conn.QueryRow(ctx, `
		SELECT sum(total_wei)
		FROM naap.serving_payment_hourly
		`+where, args...)
	var payWEI uint64
	if err := payRow.Scan(&payWEI); err != nil {
		return nil, fmt.Errorf("clickhouse get pipeline detail pay: %w", err)
	}

	// Active streams.
	// sample_ts filter restores the recency bound that was previously in the view definition.
	activeWhere := fmt.Sprintf("WHERE state = 'ONLINE' AND pipeline = ? AND sample_ts > now() - INTERVAL %d SECOND", activeStreamSecs)
	activeArgs := []any{pipeline}
	if p.Org != "" {
		activeWhere += " AND org = ?"
		activeArgs = append(activeArgs, p.Org)
	}
	activeRow := r.conn.QueryRow(ctx, `
		SELECT count() FROM naap.serving_active_stream_state `+activeWhere, activeArgs...)
	var active uint64
	_ = activeRow.Scan(&active)

	// Model breakdown
	warmWhere := fmt.Sprintf("WHERE pipeline = ? AND last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	warmArgs := []any{pipeline}
	if p.Org != "" {
		warmWhere += " AND org = ?"
		warmArgs = append(warmArgs, p.Org)
	}
	modelRows, err := r.conn.Query(ctx, `
		SELECT model_id, count(DISTINCT orch_address) AS warm
		FROM naap.serving_gpu_inventory
		`+warmWhere+`
		GROUP BY model_id
		ORDER BY warm DESC
	`, warmArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get pipeline detail models: %w", err)
	}
	defer modelRows.Close()

	var byModel []types.ModelPipelineStats
	var totalWarm uint64
	for modelRows.Next() {
		var ms types.ModelPipelineStats
		var warm uint64
		if err := modelRows.Scan(&ms.ModelID, &warm); err != nil {
			return nil, fmt.Errorf("clickhouse get pipeline detail models scan: %w", err)
		}
		ms.WarmOrchCount = int64(warm)
		totalWarm += warm
		byModel = append(byModel, ms)
	}
	if err := modelRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get pipeline detail models rows: %w", err)
	}
	if byModel == nil {
		byModel = []types.ModelPipelineStats{}
	}

	return &types.PipelineDetail{
		Pipeline:         pipeline,
		StartTime:        start,
		EndTime:          end,
		ActiveStreams:    int64(active),
		StartedCount:     int64(started),
		SuccessRate:      divSafe(float64(completed), float64(started)),
		AvgInferenceFPS:  divSafe(fpsSum, float64(fpsSamp)),
		TotalPaymentsWEI: types.WEI(payWEI),
		WarmOrchCount:    int64(totalWarm),
		ByModel:          byModel,
	}, nil
}
