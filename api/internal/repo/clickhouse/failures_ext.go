package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListFailuresByPipeline returns aggregated failure counts per pipeline (FAGG-001).
func (r *Repo) ListFailuresByPipeline(ctx context.Context, p types.QueryParams) ([]types.FailuresByPipeline, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit)

	streamRows, err := r.conn.Query(ctx, `
		SELECT pipeline,
			sum(started)   AS started,
			sum(no_orch)   AS no_orch,
			sum(orch_swap) AS orch_swap
		FROM naap.serving_stream_hourly
		`+where+`
		GROUP BY pipeline
		ORDER BY no_orch + orch_swap DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list failures by pipeline: %w", err)
	}
	defer streamRows.Close()

	var result []types.FailuresByPipeline
	for streamRows.Next() {
		var pipeline string
		var started, noOrch, orchSwap uint64
		if err := streamRows.Scan(&pipeline, &started, &noOrch, &orchSwap); err != nil {
			return nil, fmt.Errorf("clickhouse list failures by pipeline scan: %w", err)
		}
		total := int64(noOrch) + int64(orchSwap)
		result = append(result, types.FailuresByPipeline{
			Pipeline:              pipeline,
			NoOrchCount:           int64(noOrch),
			OrchSwapCount:         int64(orchSwap),
			InferenceErrorCount:   0, // Not available at pipeline level
			InferenceRestartCount: 0, // Not available at pipeline level
			TotalFailures:         total,
			FailureRate:           divSafe(float64(total), float64(started)),
		})
	}
	if err := streamRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list failures by pipeline rows: %w", err)
	}
	return result, nil
}

// ListFailuresByOrch returns aggregated failure counts per orchestrator (FAGG-002).
func (r *Repo) ListFailuresByOrch(ctx context.Context, p types.QueryParams) ([]types.FailuresByOrch, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE r.hour >= ? AND r.hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND r.org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			r.orch_address,
			coalesce(s.name, r.orch_address)  AS name,
			sum(r.ai_stream_count)             AS streams_handled,
			sum(r.error_count)                 AS inference_errors,
			sum(r.restart_count)               AS inference_restarts
		FROM naap.serving_orch_reliability_hourly AS r
		LEFT JOIN naap.serving_latest_orchestrator_state AS s ON r.orch_address = s.orch_address
		`+where+`
		GROUP BY r.orch_address, name
		ORDER BY inference_errors + inference_restarts DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list failures by orch: %w", err)
	}
	defer rows.Close()

	var result []types.FailuresByOrch
	for rows.Next() {
		var fo types.FailuresByOrch
		var streamsHandled, errors, restarts uint64
		if err := rows.Scan(&fo.OrchAddress, &fo.Name, &streamsHandled, &errors, &restarts); err != nil {
			return nil, fmt.Errorf("clickhouse list failures by orch scan: %w", err)
		}
		fo.StreamsHandled = int64(streamsHandled)
		fo.InferenceErrors = int64(errors)
		fo.InferenceRestarts = int64(restarts)
		fo.TotalFailures = fo.InferenceErrors + fo.InferenceRestarts
		fo.FailureRate = divSafe(float64(fo.TotalFailures), float64(fo.StreamsHandled))
		result = append(result, fo)
	}
	return result, rows.Err()
}
