package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListPaymentsByGateway returns payment totals aggregated by gateway address (GPAY-001).
// Reads from raw naap.events since agg_payment_hourly does not track gateway.
func (r *Repo) ListPaymentsByGateway(ctx context.Context, p types.QueryParams) ([]types.GatewayPayment, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE event_type = 'create_new_payment' AND event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			gateway,
			sum(toUInt64OrZero(JSONExtractString(data, 'face_value'))) AS total_wei,
			count()                                                     AS event_count,
			count(DISTINCT JSONExtractString(data, 'recipient'))        AS unique_orchs
		FROM naap.events
		`+where+`
		GROUP BY gateway
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
		if err := rows.Scan(&gp.GatewayAddress, &wei, &count, &orchs); err != nil {
			return nil, fmt.Errorf("clickhouse list payments by gateway scan: %w", err)
		}
		gp.TotalWEI = types.WEI(wei)
		gp.EventCount = int64(count)
		gp.UniqueOrchs = int64(orchs)

		// Resolve name from gateway_metadata
		nameRow := r.conn.QueryRow(ctx, `
			SELECT coalesce(name, '') FROM naap.gateway_metadata FINAL
			WHERE lower(eth_address) = lower(?)
		`, gp.GatewayAddress)
		_ = nameRow.Scan(&gp.Name)

		result = append(result, gp)
	}
	return result, rows.Err()
}

// ListPaymentsByStream returns total payments per stream (GPAY-002).
func (r *Repo) ListPaymentsByStream(ctx context.Context, p types.QueryParams) ([]types.StreamPayment, error) {
	start, end := effectiveWindow(p)
	limit := effectiveLimit(p)

	where := "WHERE event_type = 'create_new_payment' AND event_ts >= ? AND event_ts < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.StreamID != "" {
		where += " AND JSONExtractString(data, 'stream_id') = ?"
		args = append(args, p.StreamID)
	}
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, `
		SELECT
			JSONExtractString(data, 'stream_id')                              AS stream_id,
			org,
			JSONExtractString(data, 'pipeline')                               AS pipeline,
			sum(toUInt64OrZero(JSONExtractString(data, 'face_value')))        AS total_wei,
			count()                                                           AS event_count
		FROM naap.events
		`+where+`
		  AND JSONExtractString(data, 'stream_id') != ''
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
