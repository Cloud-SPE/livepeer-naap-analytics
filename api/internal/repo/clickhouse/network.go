package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetNetworkSummary returns legacy R1 network-snapshot fields for internal and
// lower-layer use. It is not a current routed API surface.
func (r *Repo) GetNetworkSummary(ctx context.Context, p types.QueryParams) (*types.NetworkSummary, error) {
	query := `
		SELECT
			count()                                                         AS total_registered,
			countIf(last_seen > now() - INTERVAL ? MINUTE)                  AS total_active
		FROM naap.api_latest_orchestrator_state
	`
	args := []any{activeOrchMinutes}

	if p.Org != "" {
		query += " WHERE org = ?"
		args = append(args, p.Org)
	}

	row := r.conn.QueryRow(ctx, query, args...)
	// count() returns UInt64 in ClickHouse; scan into uint64 then cast.
	var total, active uint64
	if err := row.Scan(&total, &active); err != nil {
		return nil, fmt.Errorf("clickhouse get network summary: %w", err)
	}

	return &types.NetworkSummary{
		Org:                    p.Org,
		SnapshotTime:           time.Now().UTC(),
		TotalRegistered:        int64(total),
		TotalActive:            int64(active),
		ActiveThresholdMinutes: activeOrchMinutes,
	}, nil
}

// ListOrchestrators returns active NET-001 orchestrator rows for
// GET /v1/net/orchestrators using stable cursor pagination ordered by
// (last_seen DESC, orch_address DESC).
func (r *Repo) ListOrchestrators(ctx context.Context, p types.QueryParams) ([]types.Orchestrator, types.CursorPageInfo, error) {
	where := "WHERE 1=1"
	args := []any{}

	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}
	if p.ActiveOnly {
		where += fmt.Sprintf(" AND last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	}

	limit := effectiveLimit(p)
	if values, err := decodeCursorValues(p.Cursor, 2); err != nil {
		return nil, types.CursorPageInfo{}, err
	} else if len(values) == 2 {
		cursorLastSeen, parseErr := time.Parse(time.RFC3339Nano, values[0])
		if parseErr != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("%w: parse last_seen", types.ErrInvalidCursor)
		}
		where += " AND (last_seen < ? OR (last_seen = ? AND orch_address < ?))"
		args = append(args, cursorLastSeen.UTC(), cursorLastSeen.UTC(), values[1])
	}

	query := fmt.Sprintf(`
		SELECT
			orch_address,
			org,
			uri,
			version,
			last_seen,
			last_seen > now() - INTERVAL %d MINUTE                        AS is_active,
			raw_capabilities
		FROM naap.api_latest_orchestrator_state
		%s
		ORDER BY last_seen DESC, orch_address DESC
		LIMIT ?
	`, activeOrchMinutes, where)

	rows, err := r.conn.Query(ctx, query, append(args, limit+1)...)
	if err != nil {
		return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list orchestrators: %w", err)
	}
	defer rows.Close()

	var orchs []types.Orchestrator
	for rows.Next() {
		var o types.Orchestrator
		var uri string
		if err := rows.Scan(&o.Address, &o.Org, &uri, &o.Version, &o.LastSeen, &o.IsActive, &o.RawCapabilities); err != nil {
			return nil, types.CursorPageInfo{}, fmt.Errorf("clickhouse list orchestrators scan: %w", err)
		}
		o.URI = uri
		o.Name = hostnameFromURI(uri)
		if o.Name == "" {
			o.Name = o.Address
		}
		orchs = append(orchs, o)
	}
	if err := rows.Err(); err != nil {
		return nil, types.CursorPageInfo{}, err
	}
	hasMore := len(orchs) > limit
	if hasMore {
		orchs = orchs[:limit]
	}
	if orchs == nil {
		orchs = []types.Orchestrator{}
	}
	page := types.CursorPageInfo{HasMore: hasMore, PageSize: len(orchs)}
	if hasMore {
		last := orchs[len(orchs)-1]
		page.NextCursor = encodeCursorValues(last.LastSeen.UTC().Format(time.RFC3339Nano), last.Address)
	}
	return orchs, page, nil
}

// GetGPUSummary aggregates the legacy R1 GPU inventory summary shape.
// It is retained for lower-layer use and is not a current routed API surface.
// GPU data is extracted from raw_capabilities JSON in Go (not SQL) because
// gpu_info uses dict keys (slot indices) that are complex to handle in ClickHouse SQL.
func (r *Repo) GetGPUSummary(ctx context.Context, p types.QueryParams) (*types.GPUSummary, error) {
	where := fmt.Sprintf("WHERE last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	args := []any{}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx,
		"SELECT raw_capabilities FROM naap.api_latest_orchestrator_state "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get gpu summary: %w", err)
	}
	defer rows.Close()

	seen := map[string]gpuEntry{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("clickhouse get gpu summary scan: %w", err)
		}
		parseGPUsFromCapabilities(raw, seen)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get gpu summary rows: %w", err)
	}

	modelMap := map[string]*types.GPUModel{}
	var totalGPUs int64
	var totalVRAMBytes uint64
	for _, g := range seen {
		totalGPUs++
		totalVRAMBytes += g.memoryBytes
		m := modelMap[g.name]
		if m == nil {
			m = &types.GPUModel{Model: g.name}
			modelMap[g.name] = m
		}
		m.Count++
		m.TotalVRAMGB += float64(g.memoryBytes) / (1024 * 1024 * 1024)
	}

	byModel := make([]types.GPUModel, 0, len(modelMap))
	for _, m := range modelMap {
		if m.Count > 0 {
			m.VRAMPerGPUGB = m.TotalVRAMGB / float64(m.Count)
		}
		byModel = append(byModel, *m)
	}

	return &types.GPUSummary{
		TotalGPUs:   totalGPUs,
		TotalVRAMGB: float64(totalVRAMBytes) / (1024 * 1024 * 1024),
		ByModel:     byModel,
	}, nil
}

// ListModels returns active NET-002 model-availability rows for
// GET /v1/net/models.
func (r *Repo) ListModels(ctx context.Context, p types.QueryParams) ([]types.ModelAvailability, error) {
	where := fmt.Sprintf("WHERE last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	args := []any{}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx,
		"SELECT raw_capabilities FROM naap.api_latest_orchestrator_state "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list models: %w", err)
	}
	defer rows.Close()

	agg := map[modelKey]*modelAgg{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("clickhouse list models scan: %w", err)
		}
		parseModelsFromCapabilities(raw, agg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list models rows: %w", err)
	}

	result := make([]types.ModelAvailability, 0, len(agg))
	for k, a := range agg {
		ma := types.ModelAvailability{
			Pipeline:      k.pipeline,
			Model:         k.model,
			WarmOrchCount: a.warmCount,
			TotalCapacity: a.capacity,
		}
		if len(a.prices) > 0 {
			var sum int64
			min, max := a.prices[0], a.prices[0]
			for _, pr := range a.prices {
				sum += pr
				if pr < min {
					min = pr
				}
				if pr > max {
					max = pr
				}
			}
			ma.PriceMinWeiPerPixel = min
			ma.PriceMaxWeiPerPixel = max
			ma.PriceAvgWeiPerPixel = float64(sum) / float64(len(a.prices))
		}
		result = append(result, ma)
	}

	return result, nil
}

// hostnameFromURI extracts the hostname from a URL like "https://host:port".
func hostnameFromURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Hostname()
}

// --- JSON parsing types for raw_capabilities ---

// gpuEntry holds deduplicated GPU info keyed by GPU ID.
type gpuEntry struct {
	name        string
	memoryBytes uint64
}

// gpuInfoEntry mirrors raw_capabilities.hardware[].gpu_info[slot].
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

// capPriceEntry mirrors raw_capabilities.capabilities_prices[].
type capPriceEntry struct {
	PricePerUnit  int64  `json:"pricePerUnit"`
	PixelsPerUnit int64  `json:"pixelsPerUnit"`
	Capability    int    `json:"capability"`
	Constraint    string `json:"constraint"` // model name
}

// rawCaps is the full orch capabilities JSON stored in raw_capabilities.
type rawCaps struct {
	Hardware           []hardwareEntry `json:"hardware"`
	CapabilitiesPrices []capPriceEntry `json:"capabilities_prices"`
}

// modelKey identifies a (pipeline, model) pair for aggregation.
type modelKey struct {
	pipeline string
	model    string
}

// modelAgg accumulates per-model stats across orchestrators.
type modelAgg struct {
	warmCount int64
	capacity  int64
	prices    []int64
}

func parseGPUsFromCapabilities(raw string, seen map[string]gpuEntry) {
	var caps rawCaps
	if err := json.Unmarshal([]byte(raw), &caps); err != nil {
		return
	}
	for _, hw := range caps.Hardware {
		for _, gpu := range hw.GPUInfo {
			if _, ok := seen[gpu.ID]; !ok && gpu.ID != "" {
				seen[gpu.ID] = gpuEntry{name: gpu.Name, memoryBytes: gpu.MemoryTotal}
			}
		}
	}
}

func parseModelsFromCapabilities(raw string, agg map[modelKey]*modelAgg) {
	var caps rawCaps
	if err := json.Unmarshal([]byte(raw), &caps); err != nil {
		return
	}

	warmModels := map[string]bool{}
	for _, hw := range caps.Hardware {
		warmModels[hw.Pipeline+"|"+hw.ModelID] = true
	}

	for _, pr := range caps.CapabilitiesPrices {
		if pr.Constraint == "" {
			continue
		}
		pipeline := ""
		for _, hw := range caps.Hardware {
			if hw.ModelID == pr.Constraint {
				pipeline = hw.Pipeline
				break
			}
		}
		if pipeline == "" {
			continue
		}

		key := modelKey{pipeline, pr.Constraint}
		a := agg[key]
		if a == nil {
			a = &modelAgg{}
			agg[key] = a
		}
		if warmModels[pipeline+"|"+pr.Constraint] {
			a.warmCount++
		}
		a.capacity += pr.PixelsPerUnit
		if pr.PricePerUnit > 0 {
			a.prices = append(a.prices, pr.PricePerUnit)
		}
	}
}
