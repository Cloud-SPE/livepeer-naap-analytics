package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ListPricing returns flat pricing rows across active orchestrators (PRICE-001).
func (r *Repo) ListPricing(ctx context.Context, p types.QueryParams) ([]types.OrchPricingEntry, error) {
	where := fmt.Sprintf("WHERE last_seen > now() - INTERVAL %d MINUTE", activeOrchMinutes)
	args := []any{}
	if p.Org != "" {
		where += " AND org = ?"
		args = append(args, p.Org)
	}

	rows, err := r.conn.Query(ctx,
		"SELECT orch_address, name, raw_capabilities FROM naap.agg_orch_state FINAL "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse list pricing: %w", err)
	}
	defer rows.Close()

	var result []types.OrchPricingEntry
	for rows.Next() {
		var address, name, raw string
		if err := rows.Scan(&address, &name, &raw); err != nil {
			return nil, fmt.Errorf("clickhouse list pricing scan: %w", err)
		}
		entries := parsePricingFromCapabilities(address, name, raw)
		for _, e := range entries {
			if p.Pipeline != "" && e.Pipeline != p.Pipeline {
				continue
			}
			result = append(result, e)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse list pricing rows: %w", err)
	}
	return result, nil
}

// GetOrchPricingProfile returns structured pipeline/model pricing for one orch (PRICE-002).
func (r *Repo) GetOrchPricingProfile(ctx context.Context, address string) (*types.OrchPricingProfile, error) {
	row := r.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT orch_address, name, raw_capabilities,
		       last_seen > now() - INTERVAL %d MINUTE AS is_active
		FROM naap.agg_orch_state FINAL
		WHERE orch_address = ?
	`, activeOrchMinutes), address)

	var addr, name, raw string
	var isActive uint8
	if err := row.Scan(&addr, &name, &raw, &isActive); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}
		return nil, fmt.Errorf("clickhouse get orch pricing profile: %w", err)
	}

	entries := parsePricingFromCapabilities(addr, name, raw)

	// Group by pipeline
	pipeMap := map[string]map[string]*types.ModelPricing{}
	for _, e := range entries {
		if pipeMap[e.Pipeline] == nil {
			pipeMap[e.Pipeline] = map[string]*types.ModelPricing{}
		}
		pipeMap[e.Pipeline][e.ModelID] = &types.ModelPricing{
			ModelID:       e.ModelID,
			PricePerUnit:  e.PricePerUnit,
			PixelsPerUnit: e.PixelsPerUnit,
			IsWarm:        e.IsWarm,
		}
	}

	pipelines := make([]types.PipelinePricing, 0, len(pipeMap))
	for pipe, models := range pipeMap {
		pp := types.PipelinePricing{Pipeline: pipe}
		for _, m := range models {
			pp.Models = append(pp.Models, *m)
		}
		pipelines = append(pipelines, pp)
	}

	return &types.OrchPricingProfile{
		OrchAddress: addr,
		Name:        name,
		IsActive:    isActive != 0,
		Pipelines:   pipelines,
	}, nil
}

// parsePricingFromCapabilities extracts flat pricing entries from raw_capabilities JSON.
// rawCaps struct is defined in network.go.
func parsePricingFromCapabilities(orchAddress, orchName, raw string) []types.OrchPricingEntry {
	var caps rawCaps
	if err := json.Unmarshal([]byte(raw), &caps); err != nil {
		return nil
	}

	// Build warm model set
	warmModels := map[string]struct{}{}
	for _, hw := range caps.Hardware {
		warmModels[hw.Pipeline+"|"+hw.ModelID] = struct{}{}
	}

	// Build pipeline lookup for constraint→pipeline
	constraintPipeline := map[string]string{}
	for _, hw := range caps.Hardware {
		if hw.ModelID != "" && hw.Pipeline != "" {
			constraintPipeline[hw.ModelID] = hw.Pipeline
		}
	}

	var result []types.OrchPricingEntry
	for _, pr := range caps.CapabilitiesPrices {
		if pr.Constraint == "" || pr.PricePerUnit == 0 {
			continue
		}
		pipeline := constraintPipeline[pr.Constraint]
		if pipeline == "" {
			continue
		}
		_, isWarm := warmModels[pipeline+"|"+pr.Constraint]
		result = append(result, types.OrchPricingEntry{
			OrchAddress:   orchAddress,
			Name:          orchName,
			Pipeline:      pipeline,
			ModelID:       pr.Constraint,
			PricePerUnit:  pr.PricePerUnit,
			PixelsPerUnit: pr.PixelsPerUnit,
			IsWarm:        isWarm,
		})
	}
	return result
}
