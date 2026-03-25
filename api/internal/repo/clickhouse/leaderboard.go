package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetLeaderboard returns orchestrators with activity, FPS, and latency metrics (LDR-001).
// Composite scoring is applied in the service layer.
func (r *Repo) GetLeaderboard(ctx context.Context, p types.QueryParams) ([]types.LeaderboardEntry, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	mainWhere := "WHERE r.hour >= ? AND r.hour < ?"
	// args order: activeOrchMinutes, fps(start,end), lat(start,end), main(start,end), [org], limit, offset
	args := []any{activeOrchMinutes, start, end, start, end, start, end}
	if p.Org != "" {
		mainWhere += " AND r.org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			r.orch_address,
			any(s.name)                                                        AS name,
			sum(r.ai_stream_count)                                             AS streams_handled,
			sum(r.degraded_count)                                              AS degraded,
			any(s.last_seen)                                                   AS last_seen,
			any(s.last_seen) > now() - INTERVAL ? MINUTE                      AS is_active,
			any(f.avg_fps)                                                     AS avg_fps,
			any(l.avg_lat)                                                     AS avg_lat
		FROM naap.serving_orch_reliability_hourly AS r
		LEFT JOIN (
			SELECT orch_address, name, last_seen
			FROM naap.serving_latest_orchestrator_state
		) AS s ON r.orch_address = s.orch_address
		LEFT JOIN (
			SELECT orch_address,
			       sum(inference_fps_sum) / nullIf(sum(sample_count), 0) AS avg_fps
			FROM naap.serving_fps_hourly
			WHERE hour >= ? AND hour < ?
			GROUP BY orch_address
		) AS f ON r.orch_address = f.orch_address
		LEFT JOIN (
			SELECT orch_address,
			       avg(avg_latency_ms) AS avg_lat
			FROM naap.serving_discovery_latency_hourly
			WHERE hour >= ? AND hour < ?
			GROUP BY orch_address
		) AS l ON r.orch_address = l.orch_address
		`+mainWhere+`
		GROUP BY r.orch_address
		HAVING streams_handled >= 10
		ORDER BY streams_handled DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get leaderboard: %w", err)
	}
	defer rows.Close()

	var result []types.LeaderboardEntry
	for rows.Next() {
		var e types.LeaderboardEntry
		var streamsHandled, degraded uint64
		var avgFPS, avgLat *float64
		if err := rows.Scan(
			&e.Address, &e.Name,
			&streamsHandled, &degraded,
			&e.LastSeen, &e.IsActive,
			&avgFPS, &avgLat,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get leaderboard scan: %w", err)
		}
		e.StreamsHandled = int64(streamsHandled)
		e.DegradedRate = divSafe(float64(degraded), float64(streamsHandled))
		if avgFPS != nil {
			e.AvgInferenceFPS = *avgFPS
		}
		if avgLat != nil {
			e.AvgLatencyMS = *avgLat
		}
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
			uri,
			version,
			org,
			last_seen,
			last_seen > now() - INTERVAL ? MINUTE           AS is_active,
			raw_capabilities
		FROM naap.serving_latest_orchestrator_state
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
		FROM naap.serving_orch_reliability_hourly
		WHERE orch_address = ?
		  AND hour >= now() - INTERVAL 7 DAY
	`, address)

	var streams, degraded uint64
	if err := relRow.Scan(&streams, &degraded); err == nil {
		pr.StreamsHandled = int64(streams)
		pr.DegradedRate = divSafe(float64(degraded), float64(streams))
	}

	return &pr, nil
}
