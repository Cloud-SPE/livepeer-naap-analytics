package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetCapacitySummary returns GPU supply vs. active stream demand per (pipeline, model) (CAP-001).
func (r *Repo) GetCapacitySummary(ctx context.Context, p types.QueryParams) (*types.CapacitySummary, error) {
	warmWhere := fmt.Sprintf("WHERE last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	warmArgs := []any{}
	if p.Org != "" {
		warmWhere += " AND org = ?"
		warmArgs = append(warmArgs, p.Org)
	}

	warmRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline,
			model_id,
			count(DISTINCT orch_address) AS warm_orchs,
			sum(memory_bytes)            AS total_vram
		FROM naap.api_gpu_inventory
		`+warmWhere+`
		GROUP BY pipeline, model_id
		ORDER BY warm_orchs DESC
	`, warmArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get capacity warm: %w", err)
	}
	defer warmRows.Close()

	type warmEntry struct {
		warmOrchs int64
		totalVRAM uint64
	}
	type pipeModelKey struct{ pipeline, modelID string }
	warmMap := map[pipeModelKey]warmEntry{}
	var keys []pipeModelKey

	for warmRows.Next() {
		var pipeline, modelID string
		var warmOrchs, totalVRAM uint64
		if err := warmRows.Scan(&pipeline, &modelID, &warmOrchs, &totalVRAM); err != nil {
			return nil, fmt.Errorf("clickhouse get capacity warm scan: %w", err)
		}
		k := pipeModelKey{pipeline, modelID}
		warmMap[k] = warmEntry{int64(warmOrchs), totalVRAM}
		keys = append(keys, k)
	}
	if err := warmRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get capacity warm rows: %w", err)
	}

	// Active streams per pipeline
	activeWhere := "WHERE state = 'ONLINE' AND " + activeStreamPredicate("last_seen") + " AND stream_id != ''"
	activeArgs := []any{}
	if p.Org != "" {
		activeWhere += " AND org = ?"
		activeArgs = append(activeArgs, p.Org)
	}
	activeRows, err := r.conn.Query(ctx, `
		SELECT pipeline, count() AS active
		FROM naap.api_active_stream_state
		`+activeWhere+`
		GROUP BY pipeline
	`, activeArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get capacity active: %w", err)
	}
	defer activeRows.Close()

	activeMap := map[string]int64{}
	for activeRows.Next() {
		var pipeline string
		var active uint64
		if err := activeRows.Scan(&pipeline, &active); err != nil {
			return nil, fmt.Errorf("clickhouse get capacity active scan: %w", err)
		}
		activeMap[pipeline] = int64(active)
	}
	if err := activeRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get capacity active rows: %w", err)
	}

	entries := make([]types.CapacityEntry, 0, len(keys))
	for _, k := range keys {
		w := warmMap[k]
		active := activeMap[k.pipeline]
		util := divSafe(float64(active), float64(w.warmOrchs))
		entries = append(entries, types.CapacityEntry{
			Pipeline:       k.pipeline,
			ModelID:        k.modelID,
			WarmOrchCount:  w.warmOrchs,
			ActiveStreams:  active,
			UtilizationPct: util,
			TotalVRAMBytes: w.totalVRAM,
		})
	}

	return &types.CapacitySummary{
		SnapshotTime: time.Now().UTC(),
		Entries:      entries,
	}, nil
}
