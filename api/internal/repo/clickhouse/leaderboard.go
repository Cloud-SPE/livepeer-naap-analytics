package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetLeaderboard returns orchestrators ranked by activity (LDR-001).
// Score is 0.0 in Phase 3; composite scoring is implemented in Phase 6.
func (r *Repo) GetLeaderboard(ctx context.Context, p types.QueryParams) ([]types.LeaderboardEntry, error) {
	start, end := effectiveWindow(p)
	where := "WHERE r.hour >= ? AND r.hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND r.org = ?"
		args = append(args, p.Org)
	}
	limit := effectiveLimit(p)
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			r.orch_address,
			any(s.name)                               AS name,
			sum(r.ai_stream_count)                    AS streams_handled,
			sum(r.degraded_count)                     AS degraded,
			any(s.last_seen)                          AS last_seen,
			last_seen > now() - INTERVAL ? MINUTE     AS is_active
		FROM naap.agg_orch_reliability_hourly AS r FINAL
		LEFT JOIN (
			SELECT orch_address, name, last_seen
			FROM naap.agg_orch_state FINAL
		) AS s ON r.orch_address = s.orch_address
		`+where+`
		GROUP BY r.orch_address
		HAVING streams_handled >= 10
		ORDER BY streams_handled DESC
		LIMIT ? OFFSET ?
	`, append([]any{activeOrchMinutes}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get leaderboard: %w", err)
	}
	defer rows.Close()

	var result []types.LeaderboardEntry
	for rows.Next() {
		var e types.LeaderboardEntry
		var degraded int64
		if err := rows.Scan(&e.Address, &e.Name, &e.StreamsHandled, &degraded, &e.LastSeen, &e.IsActive); err != nil {
			return nil, fmt.Errorf("clickhouse get leaderboard scan: %w", err)
		}
		e.DegradedRate = divSafe(float64(degraded), float64(e.StreamsHandled))
		// Score is 0.0 until Phase 6.
		result = append(result, e)
	}
	return result, rows.Err()
}

// GetOrchProfile returns the full profile for a single orchestrator (LDR-002).
func (r *Repo) GetOrchProfile(ctx context.Context, address string) (*types.OrchProfile, error) {
	row := r.conn.QueryRow(ctx, `
		SELECT
			orch_address,
			name,
			JSONExtractString(raw_capabilities, 'orch_uri') AS uri,
			version,
			org,
			last_seen,
			last_seen > now() - INTERVAL ? MINUTE           AS is_active,
			raw_capabilities
		FROM naap.agg_orch_state FINAL
		WHERE orch_address = ?
	`, activeOrchMinutes, address)

	var pr types.OrchProfile
	if err := row.Scan(&pr.Address, &pr.Name, &pr.URI, &pr.Version, &pr.Org, &pr.LastSeen, &pr.IsActive, &pr.RawCapabilities); err != nil {
		return nil, fmt.Errorf("clickhouse get orch profile: %w", err)
	}

	// Enrich with reliability stats (last 7 days).
	relRow := r.conn.QueryRow(ctx, `
		SELECT
			sum(ai_stream_count),
			sum(degraded_count)
		FROM naap.agg_orch_reliability_hourly FINAL
		WHERE orch_address = ?
		  AND hour >= now() - INTERVAL 7 DAY
	`, address)

	var streams, degraded int64
	if err := relRow.Scan(&streams, &degraded); err == nil {
		pr.StreamsHandled = streams
		pr.DegradedRate = divSafe(float64(degraded), float64(streams))
	}

	return &pr, nil
}
