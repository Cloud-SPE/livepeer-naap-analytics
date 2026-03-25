package enrichment

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"
)

// gpuInfoEntry mirrors raw_capabilities.hardware[].gpu_info[slot].
// Duplicated from repo/clickhouse/network.go so the enrichment package
// has no import dependency on the repo layer.
type gpuInfoEntry struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	MemoryTotal uint64 `json:"memory_total"`
}

// hardwareEntry mirrors raw_capabilities.hardware[].
type hardwareEntry struct {
	Pipeline string                  `json:"pipeline"`
	ModelID  string                  `json:"model_id"`
	GPUInfo  map[string]gpuInfoEntry `json:"gpu_info"`
}

// rawCaps is the subset of orch capabilities JSON used for GPU parsing.
type rawCaps struct {
	Hardware []hardwareEntry `json:"hardware"`
}

// orchRow holds one row from the agg_orch_state query.
type orchRow struct {
	OrchAddress    string
	Org            string
	RawCapabilities string
	LastSeen       time.Time
}

// gpuInventoryRow is one row to be inserted into naap.agg_gpu_inventory.
// Column order matches the table definition in migration 014.
type gpuInventoryRow struct {
	GPUID       string
	OrchAddress string
	Org         string
	GPUModel    string
	MemoryBytes uint64
	Pipeline    string
	ModelID     string
	LastSeen    time.Time
}

// parseGPURows extracts GPU inventory rows from a slice of orch state rows.
// Malformed raw_capabilities JSON silently skips that orch.
// GPU slots with an empty ID are silently skipped.
// This is a pure function — no I/O — to make it directly unit-testable.
func parseGPURows(orchs []orchRow) []gpuInventoryRow {
	var out []gpuInventoryRow
	for _, o := range orchs {
		var caps rawCaps
		if err := json.Unmarshal([]byte(o.RawCapabilities), &caps); err != nil {
			continue
		}
		for _, hw := range caps.Hardware {
			for _, gpu := range hw.GPUInfo {
				if gpu.ID == "" {
					continue
				}
				out = append(out, gpuInventoryRow{
					GPUID:       gpu.ID,
					OrchAddress: o.OrchAddress,
					Org:         o.Org,
					GPUModel:    gpu.Name,
					MemoryBytes: gpu.MemoryTotal,
					Pipeline:    hw.Pipeline,
					ModelID:     hw.ModelID,
					LastSeen:    o.LastSeen,
				})
			}
		}
	}
	return out
}

func (w *Worker) syncGPUInventory(ctx context.Context) error {
	rows, err := w.conn.Query(ctx,
		"SELECT orch_address, org, raw_capabilities, last_seen FROM naap.agg_orch_state FINAL")
	if err != nil {
		return err
	}
	defer rows.Close()

	var orchs []orchRow
	for rows.Next() {
		var r orchRow
		if err := rows.Scan(&r.OrchAddress, &r.Org, &r.RawCapabilities, &r.LastSeen); err != nil {
			return err
		}
		orchs = append(orchs, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	gpuRows := parseGPURows(orchs)
	if len(gpuRows) == 0 {
		return nil
	}

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO naap.agg_gpu_inventory")
	if err != nil {
		return err
	}

	for _, g := range gpuRows {
		if err := batch.Append(
			g.GPUID,
			g.OrchAddress,
			g.Org,
			g.GPUModel,
			g.MemoryBytes,
			g.Pipeline,
			g.ModelID,
			g.LastSeen,
		); err != nil {
			return err
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}
	w.log.Info("enrichment: gpu inventory synced", zap.Int("rows", len(gpuRows)), zap.Int("orchs", len(orchs)))
	return nil
}
