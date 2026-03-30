package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListStreamSamples returns recent per-stream telemetry samples (STR-EXT-001).
func (r *Repo) ListStreamSamples(ctx context.Context, p types.QueryParams) ([]types.StreamStatusSample, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE sample_ts >= ? AND sample_ts < ?"
	args := []any{start, end}

	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.StreamID != "" {
		where += " AND stream_id = ?"
		args = append(args, p.StreamID)
	}
	if p.Pipeline != "" {
		where += " AND pipeline = ?"
		args = append(args, p.Pipeline)
	}
	if p.OrchAddress != "" {
		where += " AND orch_address = ?"
		args = append(args, p.OrchAddress)
	}

	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			sample_ts, org, stream_id, gateway, orch_address,
			pipeline, state, output_fps, input_fps, e2e_latency_ms, is_attributed
		FROM naap.api_status_samples
		`+where+`
		ORDER BY sample_ts DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list stream samples: %w", err)
	}
	defer rows.Close()

	var result []types.StreamStatusSample
	for rows.Next() {
		var s types.StreamStatusSample
		var isAttributed uint8
		if err := rows.Scan(
			&s.SampleTS, &s.Org, &s.StreamID, &s.Gateway, &s.OrchAddress,
			&s.Pipeline, &s.State, &s.OutputFPS, &s.InputFPS, &s.E2ELatencyMS, &isAttributed,
		); err != nil {
			return nil, fmt.Errorf("clickhouse list stream samples scan: %w", err)
		}
		s.IsAttributed = isAttributed != 0
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetStreamDetail returns the state and full sample history for one stream (STR-EXT-002).
func (r *Repo) GetStreamDetail(ctx context.Context, streamID string) (*types.StreamDetail, error) {
	// Session lifecycle state from the canonical serving view.
	stateRow := r.conn.QueryRow(ctx, `
		SELECT
			stream_id,
			org,
			canonical_pipeline,
			ifNull(attributed_orch_address, '') AS orch_address,
			started_at,
			last_seen,
			completed,
			error_seen
		FROM naap.api_stream_sessions
		WHERE stream_id = ?
		ORDER BY last_seen DESC
		LIMIT 1
	`, streamID)

	var sd types.StreamDetail
	var isClosed, hasFailure uint8
	if err := stateRow.Scan(
		&sd.StreamID, &sd.Org, &sd.Pipeline, &sd.OrchAddress,
		&sd.StartedAt, &sd.LastSeen, &isClosed, &hasFailure,
	); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("clickhouse get stream detail: %w", err)
	}
	sd.IsClosed = isClosed != 0
	sd.HasFailure = hasFailure != 0
	// Resolve gateway from samples
	gwRow := r.conn.QueryRow(ctx, `
		SELECT any(gateway) FROM naap.api_status_samples WHERE stream_id = ?
	`, streamID)
	_ = gwRow.Scan(&sd.Gateway)

	latestStateRow := r.conn.QueryRow(ctx, `
		SELECT state
		FROM naap.api_status_samples
		WHERE stream_id = ?
		ORDER BY sample_ts DESC
		LIMIT 1
	`, streamID)
	_ = latestStateRow.Scan(&sd.State)

	// Samples from the canonical serving view.
	rows, err := r.conn.Query(ctx, `
		SELECT
			sample_ts, org, stream_id, gateway, orch_address,
			pipeline, state, output_fps, input_fps, e2e_latency_ms, is_attributed
		FROM naap.api_status_samples
		WHERE stream_id = ?
		ORDER BY sample_ts ASC
		LIMIT 1000
	`, streamID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get stream detail samples: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s types.StreamStatusSample
		var isAttributed uint8
		if err := rows.Scan(
			&s.SampleTS, &s.Org, &s.StreamID, &s.Gateway, &s.OrchAddress,
			&s.Pipeline, &s.State, &s.OutputFPS, &s.InputFPS, &s.E2ELatencyMS, &isAttributed,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get stream detail samples scan: %w", err)
		}
		s.IsAttributed = isAttributed != 0
		sd.Samples = append(sd.Samples, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get stream detail samples rows: %w", err)
	}
	if sd.Samples == nil {
		sd.Samples = []types.StreamStatusSample{}
	}

	return &sd, nil
}

// GetAttributionSummary returns attribution rate over a time window (STR-EXT-003).
func (r *Repo) GetAttributionSummary(ctx context.Context, p types.QueryParams) (*types.AttributionSummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE sample_ts >= ? AND sample_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	row := r.conn.QueryRow(ctx, `
		SELECT
			count()                        AS total_samples,
			countIf(is_attributed = 1)     AS attributed,
			countIf(is_attributed = 0)     AS unattributed
		FROM naap.api_status_samples
		`+where, args...)

	var total, attributed, unattributed uint64
	if err := row.Scan(&total, &attributed, &unattributed); err != nil {
		return nil, fmt.Errorf("clickhouse get attribution summary: %w", err)
	}

	rate := divSafe(float64(attributed), float64(total))
	return &types.AttributionSummary{
		StartTime:         start,
		EndTime:           end,
		TotalSamples:      int64(total),
		AttributedCount:   int64(attributed),
		UnattributedCount: int64(unattributed),
		AttributionRate:   rate,
	}, nil
}
