package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListGateways returns all known gateways with optional active stream count (GAT-001).
func (r *Repo) ListGateways(ctx context.Context, p types.QueryParams) ([]types.Gateway, error) {
	limit := effectiveLimit(p)

	// Base: gateway_metadata with active stream count from canonical serving samples.
	rows, err := r.conn.Query(ctx, fmt.Sprintf(`
		SELECT
			gm.eth_address                                                     AS address,
			gm.name,
			gm.avatar,
			toUInt64(gm.deposit)                                               AS deposit_wei,
			toUInt64(gm.reserve)                                               AS reserve_wei,
			gm.updated_at,
			uniqExactIf(ss.stream_id, ss.sample_ts > now() - INTERVAL %d SECOND) AS active_streams
		FROM naap.gateway_metadata AS gm FINAL
		LEFT JOIN naap.serving_status_samples AS ss
			ON lower(ss.gateway) = lower(gm.eth_address)
			AND ss.sample_ts > now() - INTERVAL %d SECOND
		GROUP BY gm.eth_address, gm.name, gm.avatar, gm.deposit, gm.reserve, gm.updated_at
		ORDER BY active_streams DESC, gm.updated_at DESC
		LIMIT ?
	`, activeStreamSecs, activeStreamSecs), limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list gateways: %w", err)
	}
	defer rows.Close()

	var result []types.Gateway
	for rows.Next() {
		var g types.Gateway
		var deposit, reserve uint64
		var activeStreams uint64
		if err := rows.Scan(&g.Address, &g.Name, &g.Avatar, &deposit, &reserve, &g.UpdatedAt, &activeStreams); err != nil {
			return nil, fmt.Errorf("clickhouse list gateways scan: %w", err)
		}
		g.DepositWEI = types.WEI(deposit)
		g.ReserveWEI = types.WEI(reserve)
		g.ActiveStreams = int64(activeStreams)
		result = append(result, g)
	}
	return result, rows.Err()
}

// GetGatewayProfile returns a gateway with aggregated stats (GAT-002).
func (r *Repo) GetGatewayProfile(ctx context.Context, address string) (*types.GatewayProfile, error) {
	// Base gateway info
	row := r.conn.QueryRow(ctx, `
		SELECT eth_address, name, avatar, deposit, reserve, updated_at
		FROM naap.gateway_metadata FINAL
		WHERE lower(eth_address) = lower(?)
	`, address)

	var gp types.GatewayProfile
	var deposit, reserve uint64
	if err := row.Scan(&gp.Address, &gp.Name, &gp.Avatar, &deposit, &reserve, &gp.UpdatedAt); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("clickhouse get gateway profile: %w", err)
	}
	gp.DepositWEI = types.WEI(deposit)
	gp.ReserveWEI = types.WEI(reserve)

	// Stream stats from samples (last 30 days — TTL window)
	statsRow := r.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT
			count(DISTINCT stream_id)                                              AS streams_routed,
			uniqExactIf(stream_id, sample_ts > now() - INTERVAL %d SECOND AND state = 'ONLINE') AS active_streams
		FROM naap.serving_status_samples
		WHERE lower(gateway) = lower(?)
	`, activeStreamSecs), address)

	var streamsRouted, activeStreams uint64
	if err := statsRow.Scan(&streamsRouted, &activeStreams); err != nil {
		return nil, fmt.Errorf("clickhouse get gateway profile stats: %w", err)
	}
	gp.StreamsRouted = int64(streamsRouted)
	gp.ActiveStreams = int64(activeStreams)

	// Total payments (raw events — gateway is top-level column)
	payRow := r.conn.QueryRow(ctx, `
		SELECT sum(face_value_wei)
		FROM naap.serving_payment_links
		WHERE lower(gateway) = lower(?)
	`, address)
	var totalWEI uint64
	if err := payRow.Scan(&totalWEI); err != nil {
		// Non-fatal: payment data may not exist for all gateways
		totalWEI = 0
	}
	gp.TotalPaymentsWEI = types.WEI(totalWEI)

	// Orchs used
	orchRows, err := r.conn.Query(ctx, `
		SELECT DISTINCT orch_address
		FROM naap.serving_status_samples
		WHERE lower(gateway) = lower(?) AND orch_address != ''
		LIMIT 100
	`, address)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get gateway profile orchs: %w", err)
	}
	defer orchRows.Close()

	for orchRows.Next() {
		var addr string
		if err := orchRows.Scan(&addr); err != nil {
			return nil, fmt.Errorf("clickhouse get gateway profile orchs scan: %w", err)
		}
		gp.OrchsUsed = append(gp.OrchsUsed, addr)
	}
	if err := orchRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get gateway profile orchs rows: %w", err)
	}
	if gp.OrchsUsed == nil {
		gp.OrchsUsed = []string{}
	}

	return &gp, nil
}

// ListGatewayOrchestrators returns orchs that handled traffic for a given gateway (GAT-003).
func (r *Repo) ListGatewayOrchestrators(ctx context.Context, address string, p types.QueryParams) ([]types.GatewayOrch, error) {
	limit := effectiveLimit(p)

	rows, err := r.conn.Query(ctx, `
		SELECT
			ss.orch_address,
			any(os.name)                   AS name,
			count(DISTINCT ss.stream_id)   AS stream_count,
			max(ss.sample_ts)              AS last_seen
		FROM naap.serving_status_samples AS ss
		LEFT JOIN naap.serving_latest_orchestrator_state AS os
			ON ss.orch_address = os.orch_address
		WHERE lower(ss.gateway) = lower(?)
		  AND ss.orch_address != ''
		GROUP BY ss.orch_address
		ORDER BY stream_count DESC
		LIMIT ?
	`, address, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list gateway orchs: %w", err)
	}
	defer rows.Close()

	var result []types.GatewayOrch
	for rows.Next() {
		var g types.GatewayOrch
		var streamCount uint64
		if err := rows.Scan(&g.OrchAddress, &g.Name, &streamCount, &g.LastSeen); err != nil {
			return nil, fmt.Errorf("clickhouse list gateway orchs scan: %w", err)
		}
		g.StreamCount = int64(streamCount)
		result = append(result, g)
	}
	return result, rows.Err()
}
