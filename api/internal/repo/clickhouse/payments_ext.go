package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListPaymentsByGateway returns payment totals aggregated by gateway address (GPAY-001).
func (r *Repo) ListPaymentsByGateway(ctx context.Context, p types.QueryParams) ([]types.GatewayPayment, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			pl.gateway,
			coalesce(any(gm.name), '') AS name,
			sum(pl.face_value_wei) AS total_wei,
			count() AS event_count,
			uniqExact(pl.recipient_address) AS unique_orchs
		FROM naap.serving_payment_links pl
		LEFT JOIN naap.gateway_metadata gm ON lower(gm.eth_address) = lower(pl.gateway)
		`+where+`
		GROUP BY pl.gateway
		ORDER BY total_wei DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list payments by gateway: %w", err)
	}
	defer rows.Close()

	var result []types.GatewayPayment
	for rows.Next() {
		var gp types.GatewayPayment
		var wei, count, orchs uint64
		if err := rows.Scan(&gp.GatewayAddress, &gp.Name, &wei, &count, &orchs); err != nil {
			return nil, fmt.Errorf("clickhouse list payments by gateway scan: %w", err)
		}
		gp.TotalWEI = types.WEI(wei)
		gp.EventCount = int64(count)
		gp.UniqueOrchs = int64(orchs)
		result = append(result, gp)
	}
	return result, rows.Err()
}

// ListPaymentsByStream returns total payments per stream (GPAY-002).
func (r *Repo) ListPaymentsByStream(ctx context.Context, p types.QueryParams) ([]types.StreamPayment, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE p.event_ts >= ? AND p.event_ts < ? AND p.canonical_session_key IS NOT NULL"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND p.org = ?"
		args = append(args, p.Org)
	}
	if p.StreamID != "" {
		where += " AND fs.stream_id = ?"
		args = append(args, p.StreamID)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			fs.stream_id AS stream_id,
			p.org,
			coalesce(fs.canonical_pipeline, p.pipeline_hint) AS pipeline,
			sum(p.face_value_wei) AS total_wei,
			count() AS event_count
		FROM naap.fact_workflow_payment_links p
		LEFT JOIN naap.fact_workflow_sessions fs ON p.canonical_session_key = fs.canonical_session_key
		`+where+`
		GROUP BY stream_id, org, pipeline
		ORDER BY total_wei DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list payments by stream: %w", err)
	}
	defer rows.Close()

	var result []types.StreamPayment
	for rows.Next() {
		var sp types.StreamPayment
		var wei, count uint64
		if err := rows.Scan(&sp.StreamID, &sp.Org, &sp.Pipeline, &wei, &count); err != nil {
			return nil, fmt.Errorf("clickhouse list payments by stream scan: %w", err)
		}
		sp.TotalWEI = types.WEI(wei)
		sp.EventCount = int64(count)
		result = append(result, sp)
	}
	return result, rows.Err()
}
