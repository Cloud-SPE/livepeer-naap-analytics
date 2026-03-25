package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetReliabilitySummary returns network-level failure rates for a time window (REL-001).
func (r *Repo) GetReliabilitySummary(ctx context.Context, p types.QueryParams) (*types.ReliabilitySummary, error) {
	start, end := effectiveWindow(p)
	where := "WHERE coalesce(started_at, last_seen) >= ? AND coalesce(started_at, last_seen) < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	row := r.conn.QueryRow(ctx, `
		SELECT
			countIf(started = 1),
			countIf(playable_seen = 1 OR completed = 1 OR startup_outcome = 'excused'),
			countIf(no_orch = 1),
			countIf(swap_count > 0)
		FROM naap.serving_stream_sessions
		`+where, args...)

	var started, completed, noOrch, orchSwap uint64
	if err := row.Scan(&started, &completed, &noOrch, &orchSwap); err != nil {
		return nil, fmt.Errorf("clickhouse get reliability summary streams: %w", err)
	}

	statusWhere := "WHERE hour >= ? AND hour < ?"
	statusArgs := []any{start, end}
	if p.Org != "" {
		statusWhere += " AND org = ?"
		statusArgs = append(statusArgs, p.Org)
	}
	relRow := r.conn.QueryRow(ctx, `
		SELECT
			sum(ai_stream_count),
			sum(degraded_count),
			sum(restart_count),
			sum(error_count),
			sum(degraded_inference_count),
			sum(degraded_input_count)
		FROM naap.serving_orch_reliability_hourly
		`+statusWhere, statusArgs...)

	var aiCount, degraded, restarts, errors, degradedInference, degradedInput uint64
	if err := relRow.Scan(&aiCount, &degraded, &restarts, &errors, &degradedInference, &degradedInput); err != nil {
		return nil, fmt.Errorf("clickhouse get reliability summary inference: %w", err)
	}

	return &types.ReliabilitySummary{
		StartTime:            start,
		EndTime:              end,
		StreamSuccessRate:    divSafe(float64(completed), float64(started)),
		NoOrchAvailableRate:  divSafe(float64(noOrch), float64(started)),
		OrchSwapCount:        int64(orchSwap),
		OrchSwapRate:         divSafe(float64(orchSwap), float64(started)),
		InferenceRestartRate: divSafe(float64(restarts), float64(aiCount)),
		DegradedStateRate:    divSafe(float64(degraded), float64(aiCount)),
		FailureBreakdown: types.FailureBreakdown{
			NoOrchAvailable:   int64(noOrch),
			OrchSwap:          int64(orchSwap),
			InferenceRestart:  int64(restarts),
			InferenceError:    int64(errors),
			DegradedInference: int64(degradedInference),
			DegradedInput:     int64(degradedInput),
		},
	}, nil
}

// ListReliabilityHistory returns hourly reliability rates for charting (REL-002).
func (r *Repo) ListReliabilityHistory(ctx context.Context, p types.QueryParams) ([]types.ReliabilityBucket, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(hour) AS ts,
			sum(started),
			sum(completed),
			sum(no_orch)
		FROM naap.serving_stream_hourly
		`+where+`
		GROUP BY ts
		ORDER BY ts ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list reliability history: %w", err)
	}
	defer rows.Close()

	var result []types.ReliabilityBucket
	for rows.Next() {
		var b types.ReliabilityBucket
		var started, completed, noOrch uint64
		if err := rows.Scan(&b.Timestamp, &started, &completed, &noOrch); err != nil {
			return nil, fmt.Errorf("clickhouse list reliability history scan: %w", err)
		}
		b.Started = int64(started)
		b.SuccessRate = rateOrNil(int64(completed), int64(started))
		b.NoOrchAvailableRate = rateOrNil(int64(noOrch), int64(started))
		result = append(result, b)
	}
	return result, rows.Err()
}

// ListOrchReliability returns per-orchestrator reliability metrics (REL-003).
// Only orchs with ≥ minReliabilitySamples*10 stream events are included (REL-003-a).
func (r *Repo) ListOrchReliability(ctx context.Context, p types.QueryParams) ([]types.OrchReliability, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.OrchAddress != "" {
		where += " AND orch_address = ?"
		args = append(args, p.OrchAddress)
	}
	limit := effectiveLimit(p)
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			orch_address,
			sum(ai_stream_count)  AS stream_count,
			sum(degraded_count)   AS degraded,
			sum(restart_count)    AS restarts,
			sum(error_count)      AS errors
		FROM naap.serving_orch_reliability_hourly
		`+where+`
		GROUP BY orch_address
		HAVING stream_count >= 10
		ORDER BY stream_count DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list orch reliability: %w", err)
	}
	defer rows.Close()

	var result []types.OrchReliability
	for rows.Next() {
		var o types.OrchReliability
		var streamCount, degraded, restarts, errors uint64
		if err := rows.Scan(&o.Address, &streamCount, &degraded, &restarts, &errors); err != nil {
			return nil, fmt.Errorf("clickhouse list orch reliability scan: %w", err)
		}
		o.StreamsHandled = int64(streamCount)
		n := float64(streamCount)
		o.DegradedRate = divSafe(float64(degraded), n)
		o.RestartRate = divSafe(float64(restarts), n)
		o.ErrorRate = divSafe(float64(errors), n)
		result = append(result, o)
	}
	return result, rows.Err()
}

// ListFailures returns a paginated log of individual failure events (REL-004).
func (r *Repo) ListFailures(ctx context.Context, p types.QueryParams) ([]types.FailureEvent, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)
	if limit > maxFailureLimit {
		limit = maxFailureLimit
	}

	var typeFilter string
	switch p.FailureType {
	case "no_orch_available":
		typeFilter = `failure_type = 'gateway_no_orchestrators_available'`
	case "orch_swap":
		typeFilter = `failure_type = 'orchestrator_swap'`
	case "inference_restart":
		typeFilter = `failure_type = 'inference_restart'`
	case "inference_error":
		typeFilter = `failure_type = 'inference_error'`
	default:
		typeFilter = `failure_type IN ('gateway_no_orchestrators_available', 'orchestrator_swap', 'inference_restart', 'inference_error')`
	}

	where := fmt.Sprintf("WHERE failure_ts >= ? AND failure_ts < ? AND (%s)", typeFilter)
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.OrchAddress != "" {
		where += " AND orch_address = ?"
		args = append(args, p.OrchAddress)
	}
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			failure_ts,
			multiIf(
				failure_type = 'gateway_no_orchestrators_available', 'no_orch_available',
				failure_type = 'orchestrator_swap', 'orch_swap',
				failure_type = 'inference_restart', 'inference_restart',
				'inference_error'
			)                                      AS failure_type,
			stream_id,
			request_id,
			org,
			gateway,
			detail
		FROM naap.serving_failure_events
		`+where+`
		ORDER BY failure_ts DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list failures: %w", err)
	}
	defer rows.Close()

	var result []types.FailureEvent
	for rows.Next() {
		var f types.FailureEvent
		if err := rows.Scan(&f.Timestamp, &f.FailureType, &f.StreamID, &f.RequestID, &f.Org, &f.Gateway, &f.Detail); err != nil {
			return nil, fmt.Errorf("clickhouse list failures scan: %w", err)
		}
		result = append(result, f)
	}
	return result, rows.Err()
}
