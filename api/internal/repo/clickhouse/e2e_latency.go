package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetE2ELatencySummary returns session-edge E2E latency stats from canonical
// session rollups (E2E-001).
func (r *Repo) GetE2ELatencySummary(ctx context.Context, p types.QueryParams) (*types.E2ELatencySummary, error) {
	start, end := effectiveWindow(p)

	where := "WHERE last_seen >= ? AND coalesce(started_at, last_seen) < ? AND requested_seen = 1 AND e2e_latency_ms > 0"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.Pipeline != "" {
		where += " AND canonical_pipeline = ?"
		args = append(args, p.Pipeline)
	}

	// Overall stats
	overallRow := r.conn.QueryRow(ctx, `
		SELECT
			avg(e2e_latency_ms)              AS avg_ms,
			quantile(0.5)(e2e_latency_ms)    AS p50_ms,
			quantile(0.95)(e2e_latency_ms)   AS p95_ms,
			quantile(0.99)(e2e_latency_ms)   AS p99_ms,
			count()                          AS n
		FROM naap.canonical_session_current
		`+where, args...)

	var overall types.E2ELatencyStats
	var overallN uint64
	if err := overallRow.Scan(&overall.AvgMS, &overall.P50MS, &overall.P95MS, &overall.P99MS, &overallN); err != nil {
		return nil, fmt.Errorf("clickhouse get e2e latency overall: %w", err)
	}
	overall.SampleCount = int64(overallN)

	// By pipeline
	pipeRows, err := r.conn.Query(ctx, `
		SELECT
			canonical_pipeline AS pipeline,
			avg(e2e_latency_ms),
			quantile(0.5)(e2e_latency_ms),
			quantile(0.95)(e2e_latency_ms),
			quantile(0.99)(e2e_latency_ms),
			count()
		FROM naap.canonical_session_current
		`+where+`
		GROUP BY canonical_pipeline
		ORDER BY count() DESC
		LIMIT 50
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get e2e latency by pipeline: %w", err)
	}
	defer pipeRows.Close()

	var byPipeline []types.PipelineE2ELatency
	for pipeRows.Next() {
		var pe types.PipelineE2ELatency
		var pipeN uint64
		if err := pipeRows.Scan(&pe.Pipeline, &pe.AvgMS, &pe.P50MS, &pe.P95MS, &pe.P99MS, &pipeN); err != nil {
			return nil, fmt.Errorf("clickhouse get e2e latency by pipeline scan: %w", err)
		}
		pe.SampleCount = int64(pipeN)
		byPipeline = append(byPipeline, pe)
	}
	if err := pipeRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get e2e latency by pipeline rows: %w", err)
	}

	// By orchestrator (top 50 by sample count)
	orchRows, err := r.conn.Query(ctx, `
		SELECT
			attributed_orch_address AS orch_address,
			canonical_pipeline AS pipeline,
			avg(e2e_latency_ms),
			quantile(0.5)(e2e_latency_ms),
			quantile(0.95)(e2e_latency_ms),
			quantile(0.99)(e2e_latency_ms),
			count()
		FROM naap.canonical_session_current
		`+where+`
		  AND ifNull(attributed_orch_address, '') != ''
		GROUP BY attributed_orch_address, canonical_pipeline
		ORDER BY count() DESC
		LIMIT 50
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get e2e latency by orch: %w", err)
	}
	defer orchRows.Close()

	var byOrch []types.OrchE2ELatency
	for orchRows.Next() {
		var oe types.OrchE2ELatency
		var orchN uint64
		if err := orchRows.Scan(&oe.Address, &oe.Pipeline, &oe.AvgMS, &oe.P50MS, &oe.P95MS, &oe.P99MS, &orchN); err != nil {
			return nil, fmt.Errorf("clickhouse get e2e latency by orch scan: %w", err)
		}
		oe.SampleCount = int64(orchN)
		byOrch = append(byOrch, oe)
	}
	if err := orchRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get e2e latency by orch rows: %w", err)
	}

	if byPipeline == nil {
		byPipeline = []types.PipelineE2ELatency{}
	}
	if byOrch == nil {
		byOrch = []types.OrchE2ELatency{}
	}

	return &types.E2ELatencySummary{
		StartTime:      start,
		EndTime:        end,
		Overall:        overall,
		ByPipeline:     byPipeline,
		ByOrchestrator: byOrch,
	}, nil
}

// ListE2ELatencyHistory returns hourly session-edge E2E latency buckets from
// canonical status-hour rollups (E2E-002).
func (r *Repo) ListE2ELatencyHistory(ctx context.Context, p types.QueryParams) ([]types.E2ELatencyBucket, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE hour >= ? AND hour < ? AND is_terminal_tail_artifact = 0 AND avg_e2e_latency_ms > 0"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.Pipeline != "" {
		where += " AND canonical_pipeline = ?"
		args = append(args, p.Pipeline)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			hour,
			avg(avg_e2e_latency_ms)            AS avg_ms,
			quantile(0.5)(avg_e2e_latency_ms)  AS p50_ms,
			quantile(0.95)(avg_e2e_latency_ms) AS p95_ms,
			count()                        AS n
		FROM naap.canonical_status_hours
		`+where+`
		GROUP BY hour
		ORDER BY hour ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list e2e latency history: %w", err)
	}
	defer rows.Close()

	var result []types.E2ELatencyBucket
	for rows.Next() {
		var b types.E2ELatencyBucket
		var n uint64
		if err := rows.Scan(&b.Timestamp, &b.AvgMS, &b.P50MS, &b.P95MS, &n); err != nil {
			return nil, fmt.Errorf("clickhouse list e2e latency history scan: %w", err)
		}
		b.SampleCount = int64(n)
		result = append(result, b)
	}
	return result, rows.Err()
}
