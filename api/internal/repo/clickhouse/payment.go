package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetPaymentSummary returns aggregate payment totals for a time window (PAY-001).
func (r *Repo) GetPaymentSummary(ctx context.Context, p types.QueryParams) (*types.PaymentSummary, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	// Summary totals.
	row := r.conn.QueryRow(ctx, `
		SELECT
			sum(total_wei)                  AS total_wei,
			sum(event_count)                AS event_count
		FROM naap.api_payment_hourly
		`+where, args...)

	var totalWEI uint64
	var eventCount uint64
	if err := row.Scan(&totalWEI, &eventCount); err != nil {
		return nil, fmt.Errorf("clickhouse get payment summary: %w", err)
	}

	linksWhere := "WHERE event_ts >= ? AND event_ts < ?"
	linkArgs := []any{start, end}
	if p.Org != "" {
		linksWhere += " AND org = ?"
		linkArgs = append(linkArgs, p.Org)
	}
	uniqueRow := r.conn.QueryRow(ctx, `
		SELECT uniqExact(recipient_address)
		FROM naap.api_payment_links
		`+linksWhere, linkArgs...)
	var uniqueOrchs uint64
	if err := uniqueRow.Scan(&uniqueOrchs); err != nil {
		return nil, fmt.Errorf("clickhouse get payment summary unique orchs: %w", err)
	}

	// Breakdown by org.
	orgRows, err := r.conn.Query(ctx, `
		SELECT org, sum(total_wei), sum(event_count)
		FROM naap.api_payment_hourly
		`+where+`
		GROUP BY org
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get payment summary by org: %w", err)
	}
	defer orgRows.Close()

	byOrg := map[string]types.OrgPaymentTotal{}
	for orgRows.Next() {
		var org string
		var wei uint64
		var count uint64
		if err := orgRows.Scan(&org, &wei, &count); err != nil {
			return nil, fmt.Errorf("clickhouse get payment summary org scan: %w", err)
		}
		byOrg[org] = types.OrgPaymentTotal{TotalWEI: types.WEI(wei), Count: int64(count)}
	}
	if err := orgRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get payment summary org rows: %w", err)
	}

	return &types.PaymentSummary{
		StartTime:           start,
		EndTime:             end,
		TotalPaymentsWEI:    types.WEI(totalWEI),
		PaymentEventCount:   int64(eventCount),
		UniqueOrchestrators: int64(uniqueOrchs),
		ByOrg:               byOrg,
	}, nil
}

// ListPaymentHistory returns hourly payment totals for charting (PAY-002).
func (r *Repo) ListPaymentHistory(ctx context.Context, p types.QueryParams) ([]types.PaymentBucket, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(hour)             AS ts,
			sum(total_wei)                  AS total_wei,
			sum(event_count)                AS event_count,
			count(DISTINCT orch_address)    AS unique_orchs
		FROM naap.api_payment_hourly
		`+where+`
		GROUP BY ts
		ORDER BY ts ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list payment history: %w", err)
	}
	defer rows.Close()

	var result []types.PaymentBucket
	for rows.Next() {
		var b types.PaymentBucket
		var wei, eventCount, uniqueOrchs uint64
		if err := rows.Scan(&b.Timestamp, &wei, &eventCount, &uniqueOrchs); err != nil {
			return nil, fmt.Errorf("clickhouse list payment history scan: %w", err)
		}
		b.TotalWEI = types.WEI(wei)
		b.EventCount = int64(eventCount)
		b.UniqueOrchs = int64(uniqueOrchs)
		result = append(result, b)
	}
	return result, rows.Err()
}

// ListPaymentsByPipeline returns payment totals grouped by pipeline (PAY-003).
func (r *Repo) ListPaymentsByPipeline(ctx context.Context, p types.QueryParams) ([]types.PipelinePayment, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			sum(total_wei)      AS total_wei,
			sum(event_count)    AS event_count
		FROM naap.api_payment_hourly
		`+where+`
		GROUP BY pipeline
		ORDER BY total_wei DESC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list payments by pipeline: %w", err)
	}
	defer rows.Close()

	var result []types.PipelinePayment
	for rows.Next() {
		var pp types.PipelinePayment
		var wei, eventCount uint64
		if err := rows.Scan(&pp.Pipeline, &wei, &eventCount); err != nil {
			return nil, fmt.Errorf("clickhouse list payments by pipeline scan: %w", err)
		}
		pp.TotalWEI = types.WEI(wei)
		pp.EventCount = int64(eventCount)
		result = append(result, pp)
	}
	return result, rows.Err()
}

// ListPaymentsByOrch returns payment totals grouped by orchestrator (PAY-004).
func (r *Repo) ListPaymentsByOrch(ctx context.Context, p types.QueryParams) ([]types.OrchPayment, error) {
	start, end := effectiveWindow(p)
	where := "WHERE hour >= ? AND hour < ?"
	args := []any{start, end}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	limit := effectiveLimit(p)
	args = append(args, limit, p.Offset)

	rows, err := r.conn.Query(ctx, `
		SELECT
			orch_address,
			sum(total_wei)   AS total_wei,
			sum(event_count) AS payment_count
		FROM naap.api_payment_hourly
		`+where+`
		GROUP BY orch_address
		ORDER BY total_wei DESC
		LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list payments by orch: %w", err)
	}
	defer rows.Close()

	var result []types.OrchPayment
	for rows.Next() {
		var op types.OrchPayment
		var wei, paymentCount uint64
		if err := rows.Scan(&op.Address, &wei, &paymentCount); err != nil {
			return nil, fmt.Errorf("clickhouse list payments by orch scan: %w", err)
		}
		op.TotalReceivedWEI = types.WEI(wei)
		op.PaymentCount = int64(paymentCount)
		result = append(result, op)
	}
	return result, rows.Err()
}
