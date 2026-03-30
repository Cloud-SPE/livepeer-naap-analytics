package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetActiveStreams returns currently active stream counts by org, pipeline, state (STR-001).
func (r *Repo) GetActiveStreams(ctx context.Context, p types.QueryParams) (*types.ActiveStreamsSummary, error) {
	where := "WHERE " + activeStreamPredicate("last_seen") + " AND stream_id != ''"
	args := []any{}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT org, pipeline, state, count() AS n
		FROM naap.api_active_stream_state
		`+where+`
		GROUP BY org, pipeline, state
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get active streams: %w", err)
	}
	defer rows.Close()

	result := &types.ActiveStreamsSummary{
		ActiveThresholdSeconds: activeStreamSecs,
		ByOrg:                  map[string]int64{},
		ByPipeline:             map[string]int64{},
		ByState:                map[string]int64{},
	}
	for rows.Next() {
		var org, pipeline, state string
		var n uint64
		if err := rows.Scan(&org, &pipeline, &state, &n); err != nil {
			return nil, fmt.Errorf("clickhouse get active streams scan: %w", err)
		}
		result.TotalActive += int64(n)
		result.ByOrg[org] += int64(n)
		label := pipeline
		if label == "" {
			label = "other"
		}
		result.ByPipeline[label] += int64(n)
		if state == "" {
			state = "UNKNOWN"
		}
		result.ByState[state] += int64(n)
	}
	return result, rows.Err()
}

// GetStreamSummary returns aggregate stream lifecycle counts for a time window (STR-002).
func (r *Repo) GetStreamSummary(ctx context.Context, p types.QueryParams) (*types.StreamSummary, error) {
	start, end := effectiveWindow(p)
	where := "WHERE coalesce(started_at, last_seen) >= ? AND coalesce(started_at, last_seen) < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	row := r.conn.QueryRow(ctx, `
		SELECT
			countIf(requested_seen = 1) AS total_requested,
			countIf(startup_outcome = 'success') AS startup_success_count,
			countIf(selection_outcome = 'no_orch') AS no_orch_count,
			countIf(swap_count > 0) AS orch_swap_count
		FROM naap.api_stream_sessions
		`+where, args...)

	// sum() of UInt64 columns returns UInt64; scan into uint64 then cast.
	var requested, successes, noOrch, orchSwap uint64
	if err := row.Scan(&requested, &successes, &noOrch, &orchSwap); err != nil {
		return nil, fmt.Errorf("clickhouse get stream summary: %w", err)
	}

	return &types.StreamSummary{
		StartTime:          start,
		EndTime:            end,
		TotalRequested:     int64(requested),
		StartupSuccesses:   int64(successes),
		NoOrchSessionCount: int64(noOrch),
		OrchSwapCount:      int64(orchSwap),
		StartupSuccessRate: divSafe(float64(successes), float64(requested)),
		NoOrchSessionRate:  divSafe(float64(noOrch), float64(requested)),
	}, nil
}

// ListStreamHistory returns hourly stream lifecycle counts for charting (STR-003).
func (r *Repo) ListStreamHistory(ctx context.Context, p types.QueryParams) ([]types.StreamBucket, error) {
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
			sum(requested_sessions),
			sum(startup_success_sessions),
			sum(no_orch_sessions),
			sum(orch_swap_sessions)
		FROM naap.api_stream_hourly
		`+where+`
		GROUP BY ts
		ORDER BY ts ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list stream history: %w", err)
	}
	defer rows.Close()

	var result []types.StreamBucket
	for rows.Next() {
		var b types.StreamBucket
		var requested, successes, noOrch, orchSwap uint64
		if err := rows.Scan(&b.Timestamp, &requested, &successes, &noOrch, &orchSwap); err != nil {
			return nil, fmt.Errorf("clickhouse list stream history scan: %w", err)
		}
		b.RequestedSessions = int64(requested)
		b.StartupSuccessSessions = int64(successes)
		b.NoOrchSessions = int64(noOrch)
		b.OrchSwapSessions = int64(orchSwap)
		result = append(result, b)
	}
	return result, rows.Err()
}
