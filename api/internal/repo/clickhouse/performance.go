package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetFPSSummary returns inference and input FPS statistics by pipeline and orch (PERF-001).
// Percentiles are computed from canonical serving samples.
// PERF-001-c: samples with fps == 0 are excluded.
func (r *Repo) GetFPSSummary(ctx context.Context, p types.QueryParams) (*types.FPSSummary, error) {
	start, end := effectiveWindow(p)
	where := "WHERE sample_ts >= ? AND sample_ts < ? AND output_fps > 0"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.Pipeline != "" {
		where += " AND pipeline = ?"
		args = append(args, p.Pipeline)
	}
	if p.OrchAddress != "" {
		where += " AND orch_address = ?"
		args = append(args, p.OrchAddress)
	}

	// By pipeline.
	pipeRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			avg(output_fps)            AS avg_inf,
			quantile(0.05)(output_fps) AS p5_inf,
			quantile(0.5)(output_fps)  AS p50_inf,
			quantile(0.99)(output_fps) AS p99_inf,
			avg(input_fps)             AS avg_inp,
			quantile(0.05)(input_fps)  AS p5_inp,
			quantile(0.5)(input_fps)   AS p50_inp,
			quantile(0.99)(input_fps)  AS p99_inp,
			count()                                                          AS n
		FROM naap.serving_status_samples
		`+where+`
		GROUP BY pipeline
		ORDER BY n DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get fps summary by pipeline: %w", err)
	}
	defer pipeRows.Close()

	var byPipeline []types.PipelineFPS
	for pipeRows.Next() {
		var pf types.PipelineFPS
		var n uint64
		if err := pipeRows.Scan(
			&pf.Pipeline,
			&pf.InferenceFPS.Avg, &pf.InferenceFPS.P5, &pf.InferenceFPS.P50, &pf.InferenceFPS.P99,
			&pf.InputFPS.Avg, &pf.InputFPS.P5, &pf.InputFPS.P50, &pf.InputFPS.P99,
			&n,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get fps summary pipeline scan: %w", err)
		}
		pf.SampleCount = int64(n)
		byPipeline = append(byPipeline, pf)
	}
	if err := pipeRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get fps summary pipeline rows: %w", err)
	}

	// By orchestrator.
	orchRows, err := r.conn.Query(ctx, `
		SELECT
			orch_address AS addr,
			pipeline,
			avg(output_fps)            AS avg_inf,
			quantile(0.05)(output_fps) AS p5_inf,
			quantile(0.5)(output_fps)  AS p50_inf,
			quantile(0.99)(output_fps) AS p99_inf,
			avg(input_fps)             AS avg_inp,
			quantile(0.05)(input_fps)  AS p5_inp,
			quantile(0.5)(input_fps)   AS p50_inp,
			quantile(0.99)(input_fps)  AS p99_inp,
			count()                                                           AS n
		FROM naap.serving_status_samples
		`+where+`
		AND orch_address != ''
		GROUP BY addr, pipeline
		ORDER BY n DESC
		LIMIT 100
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get fps summary by orch: %w", err)
	}
	defer orchRows.Close()

	var byOrch []types.OrchFPS
	for orchRows.Next() {
		var of types.OrchFPS
		var n uint64
		if err := orchRows.Scan(
			&of.Address, &of.Pipeline,
			&of.InferenceFPS.Avg, &of.InferenceFPS.P5, &of.InferenceFPS.P50, &of.InferenceFPS.P99,
			&of.InputFPS.Avg, &of.InputFPS.P5, &of.InputFPS.P50, &of.InputFPS.P99,
			&n,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get fps summary orch scan: %w", err)
		}
		of.SampleCount = int64(n)
		byOrch = append(byOrch, of)
	}
	if err := orchRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get fps summary orch rows: %w", err)
	}

	return &types.FPSSummary{StartTime: start, EndTime: end, ByPipeline: byPipeline, ByOrchestrator: byOrch}, nil
}

// ListFPSHistory returns hourly avg FPS for charting (PERF-002).
func (r *Repo) ListFPSHistory(ctx context.Context, p types.QueryParams) ([]types.FPSBucket, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.Pipeline != "" {
		where += " AND pipeline = ?"
		args = append(args, p.Pipeline)
	}
	if p.OrchAddress != "" {
		where += " AND orch_address = ?"
		args = append(args, p.OrchAddress)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(hour)                          AS ts,
			sum(inference_fps_sum) / sum(sample_count)   AS avg_inf,
			sum(input_fps_sum) / sum(sample_count)       AS avg_inp,
			sum(sample_count)                            AS n
		FROM naap.serving_fps_hourly
		`+where+`
		GROUP BY ts
		ORDER BY ts ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list fps history: %w", err)
	}
	defer rows.Close()

	var result []types.FPSBucket
	var prevAvg float64
	for rows.Next() {
		var b types.FPSBucket
		var n uint64
		if err := rows.Scan(&b.Timestamp, &b.AvgInferenceFPS, &b.AvgInputFPS, &n); err != nil {
			return nil, fmt.Errorf("clickhouse list fps history scan: %w", err)
		}
		b.SampleCount = int64(n)
		// PERF-002-a: flag bucket as degraded if avg FPS drops ≥20% from prior bucket.
		if prevAvg > 0 && b.AvgInferenceFPS < prevAvg*0.8 {
			b.Degraded = true
		}
		prevAvg = b.AvgInferenceFPS
		result = append(result, b)
	}
	return result, rows.Err()
}

// GetLatencySummary returns orch discovery latency percentiles (PERF-003).
// Percentiles are computed from typed discovery candidates.
func (r *Repo) GetLatencySummary(ctx context.Context, p types.QueryParams) (*types.LatencySummary, error) {
	start, end := effectiveWindow(p)
	innerConds := "event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		innerConds += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			orch_address AS addr,
			avg(latency_ms) AS avg_ms,
			quantile(0.5)(latency_ms)  AS p50,
			quantile(0.95)(latency_ms) AS p95,
			quantile(0.99)(latency_ms) AS p99,
			count()                                                            AS n
		FROM naap.serving_discovery_results
		WHERE `+innerConds+`
		  AND orch_address != ''
		  AND latency_ms > 0
		GROUP BY addr
		ORDER BY avg_ms ASC
		LIMIT 200
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get latency summary: %w", err)
	}
	defer rows.Close()

	var orchs []types.OrchLatency
	var totalAvgSum float64
	for rows.Next() {
		var o types.OrchLatency
		var n uint64
		if err := rows.Scan(&o.Address, &o.AvgMS, &o.P50MS, &o.P95MS, &o.P99MS, &n); err != nil {
			return nil, fmt.Errorf("clickhouse get latency summary scan: %w", err)
		}
		o.SampleCount = int64(n)
		totalAvgSum += o.AvgMS
		orchs = append(orchs, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get latency summary rows: %w", err)
	}

	networkAvg := divSafe(totalAvgSum, float64(len(orchs)))
	return &types.LatencySummary{ByOrchestrator: orchs, NetworkAvgMS: networkAvg}, nil
}

// GetWebRTCQuality returns aggregated WebRTC quality metrics (PERF-004).
func (r *Repo) GetWebRTCQuality(ctx context.Context, p types.QueryParams) (*types.WebRTCQuality, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.StreamID != "" {
		where += " AND stream_id = ?"
		args = append(args, p.StreamID)
	}

	row := r.conn.QueryRow(ctx, `
		SELECT
			sum(video_jitter_sum) / nullIf(sum(sample_count), 0)  AS avg_v_jitter,
			sum(video_packets_lost)                               AS v_lost,
			sum(video_packets_recv)                               AS v_recv,
			sum(audio_jitter_sum) / nullIf(sum(sample_count), 0)  AS avg_a_jitter,
			sum(audio_packets_lost)                               AS a_lost,
			sum(audio_packets_recv)                               AS a_recv,
			sum(sample_count)                                     AS total,
			sum(quality_good)                                     AS good,
			sum(quality_fair)                                     AS fair,
			sum(quality_poor)                                     AS poor
		FROM naap.agg_webrtc_hourly FINAL
		`+where, args...)

	var avgVJ, avgAJ *float64
	var vLost, vRecv, aLost, aRecv, total, good, fair, poor uint64
	if err := row.Scan(&avgVJ, &vLost, &vRecv, &avgAJ, &aLost, &aRecv, &total, &good, &fair, &poor); err != nil {
		return nil, fmt.Errorf("clickhouse get webrtc quality: %w", err)
	}

	q := &types.WebRTCQuality{StartTime: start, EndTime: end}
	if avgVJ != nil {
		q.Video.AvgJitterMS = *avgVJ
	}
	if avgAJ != nil {
		q.Audio.AvgJitterMS = *avgAJ
	}
	q.Video.PacketLossRate = divSafe(float64(vLost), float64(vRecv))
	q.Audio.PacketLossRate = divSafe(float64(aLost), float64(aRecv))

	conn := float64(good + fair + poor)
	q.ConnQuality.Good = divSafe(float64(good), conn)
	q.ConnQuality.Fair = divSafe(float64(fair), conn)
	q.ConnQuality.Poor = divSafe(float64(poor), conn)
	return q, nil
}
