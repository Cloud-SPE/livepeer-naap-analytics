package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetReliabilitySummary returns network-level failure rates for a time window (REL-001).
func (r *Repo) GetReliabilitySummary(ctx context.Context, p types.QueryParams) (*types.ReliabilitySummary, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	// Stream-level counts from agg_stream_hourly.
	row := r.conn.QueryRow(ctx, `
		SELECT
			sum(started),
			sum(completed),
			sum(no_orch),
			sum(orch_swap)
		FROM naap.agg_stream_hourly FINAL
		`+where, args...)

	var started, completed, noOrch, orchSwap int64
	if err := row.Scan(&started, &completed, &noOrch, &orchSwap); err != nil {
		return nil, fmt.Errorf("clickhouse get reliability summary streams: %w", err)
	}

	// Inference-level counts from agg_orch_reliability_hourly.
	relRow := r.conn.QueryRow(ctx, `
		SELECT
			sum(ai_stream_count),
			sum(degraded_count),
			sum(restart_count),
			sum(error_count)
		FROM naap.agg_orch_reliability_hourly FINAL
		`+where, args...)

	var aiCount, degraded, restarts, errors int64
	if err := relRow.Scan(&aiCount, &degraded, &restarts, &errors); err != nil {
		return nil, fmt.Errorf("clickhouse get reliability summary inference: %w", err)
	}

	return &types.ReliabilitySummary{
		StartTime:            start,
		EndTime:              end,
		StreamSuccessRate:    divSafe(float64(completed), float64(started)),
		NoOrchAvailableRate:  divSafe(float64(noOrch), float64(started)),
		OrchSwapCount:        orchSwap,
		OrchSwapRate:         divSafe(float64(orchSwap), float64(started)),
		InferenceRestartRate: divSafe(float64(restarts), float64(aiCount)),
		DegradedStateRate:    divSafe(float64(degraded), float64(aiCount)),
		FailureBreakdown: types.FailureBreakdown{
			NoOrchAvailable:   noOrch,
			OrchSwap:          orchSwap,
			InferenceRestart:  restarts,
			InferenceError:    errors,
			DegradedInference: 0, // TODO: split degraded_count by state in agg table
			DegradedInput:     0,
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
		FROM naap.agg_stream_hourly FINAL
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
		var started, completed, noOrch int64
		if err := rows.Scan(&b.Timestamp, &started, &completed, &noOrch); err != nil {
			return nil, fmt.Errorf("clickhouse list reliability history scan: %w", err)
		}
		b.Started = started
		b.SuccessRate = rateOrNil(completed, started)
		b.NoOrchAvailableRate = rateOrNil(noOrch, started)
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
		FROM naap.agg_orch_reliability_hourly FINAL
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
		var degraded, restarts, errors int64
		if err := rows.Scan(&o.Address, &o.StreamsHandled, &degraded, &restarts, &errors); err != nil {
			return nil, fmt.Errorf("clickhouse list orch reliability scan: %w", err)
		}
		n := float64(o.StreamsHandled)
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

	// Build WHERE across two event types:
	// - stream_trace with failure subtypes
	// - ai_stream_status with restart/error signals
	var typeFilter string
	switch p.FailureType {
	case "no_orch_available":
		typeFilter = `event_type = 'stream_trace' AND JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available'`
	case "orch_swap":
		typeFilter = `event_type = 'stream_trace' AND JSONExtractString(data, 'type') = 'orchestrator_swap'`
	case "inference_restart":
		typeFilter = `event_type = 'ai_stream_status' AND JSONExtractUInt(data, 'inference_status', 'restart_count') > 0`
	case "inference_error":
		typeFilter = `event_type = 'ai_stream_status' AND JSONExtractString(data, 'inference_status', 'last_error') NOT IN ('', 'null')`
	default:
		typeFilter = `(
			(event_type = 'stream_trace' AND JSONExtractString(data, 'type') IN ('gateway_no_orchestrators_available','orchestrator_swap'))
			OR
			(event_type = 'ai_stream_status' AND (
				JSONExtractUInt(data, 'inference_status', 'restart_count') > 0
				OR JSONExtractString(data, 'inference_status', 'last_error') NOT IN ('', 'null')
			))
		)`
	}

	where := fmt.Sprintf("WHERE event_ts >= ? AND event_ts < ? AND (%s)", typeFilter)
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.OrchAddress != "" {
		where += " AND lower(JSONExtractString(data, 'orchestrator_info', 'address')) = ?"
		args = append(args, p.OrchAddress)
	}
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			event_ts,
			multiIf(
				event_type = 'stream_trace' AND JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available', 'no_orch_available',
				event_type = 'stream_trace' AND JSONExtractString(data, 'type') = 'orchestrator_swap', 'orch_swap',
				event_type = 'ai_stream_status' AND JSONExtractUInt(data, 'inference_status', 'restart_count') > 0, 'inference_restart',
				'inference_error'
			)                                                           AS failure_type,
			JSONExtractString(data, 'stream_id')                        AS stream_id,
			JSONExtractString(data, 'request_id')                       AS request_id,
			org,
			gateway,
			data
		FROM naap.events
		`+where+`
		ORDER BY event_ts DESC
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
