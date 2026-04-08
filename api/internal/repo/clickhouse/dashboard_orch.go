package clickhouse

import (
	"context"
	"fmt"
	"sort"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetDashboardOrchestrators returns per-orchestrator SLA metrics (R16-3).
// Sources: naap.api_sla_compliance, naap.api_latest_orchestrator_state,
//
//	naap.api_latest_orchestrator_pipeline_models,
//	naap.canonical_capability_hardware_inventory.
func (r *Repo) GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error) {
	if windowHours <= 0 {
		windowHours = 168 // 7 days
	}

	// --- Query 1: SLA aggregates per orchestrator ---
	type slaRow struct {
		address   string
		known     uint64
		successes uint64
		unexcused int64
		swaps     uint64
	}

	slaRows, err := r.conn.Query(ctx, `
		SELECT
			orchestrator_address,
			sum(known_sessions_count)  AS known,
			sum(startup_success_sessions) AS successes,
			greatest(
				toInt64(sum(startup_failed_sessions)) - toInt64(sum(startup_excused_sessions)),
				0
			) AS unexcused,
			sum(total_swapped_sessions) AS swaps
		FROM naap.api_sla_compliance
		WHERE window_start >= now() - INTERVAL ? HOUR
		  AND orchestrator_address != ''
		GROUP BY orchestrator_address
		HAVING known > 0
		ORDER BY known DESC
	`, windowHours)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators sla: %w", err)
	}
	defer slaRows.Close()

	var slaData []slaRow
	for slaRows.Next() {
		var s slaRow
		if err := slaRows.Scan(&s.address, &s.known, &s.successes, &s.unexcused, &s.swaps); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard orchestrators sla scan: %w", err)
		}
		slaData = append(slaData, s)
	}
	if err := slaRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators sla rows: %w", err)
	}

	if len(slaData) == 0 {
		return []types.DashboardOrchestrator{}, nil
	}

	// --- Query 2: Orch names and URIs ---
	nameRows, err := r.conn.Query(ctx, `
		SELECT orch_address, name, uri FROM naap.api_latest_orchestrator_state
	`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators names: %w", err)
	}
	defer nameRows.Close()

	type orchState struct{ name, uri string }
	orchStates := map[string]orchState{}
	for nameRows.Next() {
		var addr, name, uri string
		if err := nameRows.Scan(&addr, &name, &uri); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard orchestrators names scan: %w", err)
		}
		orchStates[addr] = orchState{name: name, uri: uri}
	}
	if err := nameRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators names rows: %w", err)
	}

	// --- Query 3: Pipeline/model offerings per orch ---
	pmRows, err := r.conn.Query(ctx, `
		SELECT DISTINCT orch_address, pipeline_id, model_id
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL ? MINUTE
		  AND pipeline_id != ''
		ORDER BY orch_address, pipeline_id, model_id
	`, activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators pipelines: %w", err)
	}
	defer pmRows.Close()

	type orchPipelines struct {
		pipelineSet map[string]struct{}
		modelMap    map[string]map[string]struct{}
	}
	orchPM := map[string]*orchPipelines{}
	for pmRows.Next() {
		var addr, pipeline, model string
		if err := pmRows.Scan(&addr, &pipeline, &model); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard orchestrators pipelines scan: %w", err)
		}
		op := orchPM[addr]
		if op == nil {
			op = &orchPipelines{
				pipelineSet: map[string]struct{}{},
				modelMap:    map[string]map[string]struct{}{},
			}
			orchPM[addr] = op
		}
		op.pipelineSet[pipeline] = struct{}{}
		if model != "" {
			if op.modelMap[pipeline] == nil {
				op.modelMap[pipeline] = map[string]struct{}{}
			}
			op.modelMap[pipeline][model] = struct{}{}
		}
	}
	if err := pmRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators pipelines rows: %w", err)
	}

	// --- Query 4: GPU counts per orch ---
	gpuRows, err := r.conn.Query(ctx, `
		SELECT orch_address, countDistinct(gpu_id) AS gpu_count
		FROM naap.canonical_capability_hardware_inventory
		WHERE snapshot_ts > now() - INTERVAL ? MINUTE
		  AND gpu_id != ''
		GROUP BY orch_address
	`, activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators gpus: %w", err)
	}
	defer gpuRows.Close()

	gpuCounts := map[string]uint64{}
	for gpuRows.Next() {
		var addr string
		var count uint64
		if err := gpuRows.Scan(&addr, &count); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard orchestrators gpus scan: %w", err)
		}
		gpuCounts[addr] = count
	}
	if err := gpuRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard orchestrators gpus rows: %w", err)
	}

	// --- Assemble results ---
	result := make([]types.DashboardOrchestrator, 0, len(slaData))
	for _, s := range slaData {
		known := float64(s.known)
		successRatio := divSafe(float64(s.successes), known)
		effectiveRate := divSafe(known-float64(s.unexcused), known)
		noSwapRatio := divSafe(known-float64(s.swaps), known)
		slaScore := 0.7*effectiveRate + 0.3*noSwapRatio
		if effectiveRate < 0 {
			effectiveRate = 0
		}
		if noSwapRatio < 0 {
			noSwapRatio = 0
		}
		if slaScore < 0 {
			slaScore = 0
		}

		var pipelines []string
		var pipelineModels []types.DashboardPipelineModelOffer
		if op := orchPM[s.address]; op != nil {
			for p := range op.pipelineSet {
				pipelines = append(pipelines, p)
			}
			sort.Strings(pipelines)
			for _, p := range pipelines {
				var models []string
				for m := range op.modelMap[p] {
					models = append(models, m)
				}
				sort.Strings(models)
				pipelineModels = append(pipelineModels, types.DashboardPipelineModelOffer{
					PipelineID: p,
					ModelIDs:   models,
				})
			}
		}
		if pipelines == nil {
			pipelines = []string{}
		}
		if pipelineModels == nil {
			pipelineModels = []types.DashboardPipelineModelOffer{}
		}

		state := orchStates[s.address]
		result = append(result, types.DashboardOrchestrator{
			Address:              s.address,
			EnsName:              state.name,
			ServiceURI:           state.uri,
			KnownSessions:        int64(s.known),
			SuccessSessions:      int64(s.successes),
			SuccessRatio:         successRatio,
			EffectiveSuccessRate: &effectiveRate,
			NoSwapRatio:          &noSwapRatio,
			SLAScore:             &slaScore,
			Pipelines:            pipelines,
			PipelineModels:       pipelineModels,
			GPUCount:             int64(gpuCounts[s.address]),
		})
	}

	return result, nil
}

// GetDashboardGPUCapacity returns GPU inventory grouped by pipeline/model/GPU-model (R16-4).
// Source: naap.canonical_capability_hardware_inventory.
func (r *Repo) GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error) {
	totalRow := r.conn.QueryRow(ctx, `
		SELECT countDistinct(gpu_id) AS total
		FROM naap.canonical_capability_hardware_inventory
		WHERE snapshot_ts > now() - INTERVAL ? MINUTE
		  AND gpu_id != ''
	`, activeOrchMinutes)

	var totalGPUs uint64
	if err := totalRow.Scan(&totalGPUs); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard gpu capacity total: %w", err)
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			pipeline_id,
			model_id,
			coalesce(gpu_model_name, 'Unknown') AS gpu_model,
			countDistinct(gpu_id)               AS gpu_count
		FROM naap.canonical_capability_hardware_inventory
		WHERE snapshot_ts > now() - INTERVAL ? MINUTE
		  AND gpu_id != ''
		GROUP BY pipeline_id, model_id, gpu_model
		ORDER BY gpu_count DESC
	`, activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard gpu capacity rows: %w", err)
	}
	defer rows.Close()

	type pipelineModelKey struct{ pipeline, model string }
	type modelGPUEntry struct {
		gpuModel string
		count    uint64
	}

	pipelineGPUs := map[string]uint64{}
	pipelineModelGPUs := map[pipelineModelKey][]modelGPUEntry{}
	modelCounts := map[string]uint64{}

	for rows.Next() {
		var pipeline, model, gpuModel string
		var count uint64
		if err := rows.Scan(&pipeline, &model, &gpuModel, &count); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard gpu capacity scan: %w", err)
		}
		pipelineGPUs[pipeline] += count
		key := pipelineModelKey{pipeline, model}
		pipelineModelGPUs[key] = append(pipelineModelGPUs[key], modelGPUEntry{gpuModel, count})
		modelCounts[gpuModel] += count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard gpu capacity rows err: %w", err)
	}

	pipelineList := make([]types.DashboardGPUCapacityPipeline, 0, len(pipelineGPUs))
	for pipeline, totalCount := range pipelineGPUs {
		perModel := map[string]int64{}
		for key, entries := range pipelineModelGPUs {
			if key.pipeline != pipeline {
				continue
			}
			for range entries {
				perModel[key.model] += int64(entries[0].count)
			}
			// Correct accumulation
			for _, e := range entries {
				perModel[key.model] += int64(e.count)
			}
		}
		// Fix double-count from above: re-aggregate cleanly
		perModel = map[string]int64{}
		for key, entries := range pipelineModelGPUs {
			if key.pipeline != pipeline {
				continue
			}
			for _, e := range entries {
				perModel[key.model] += int64(e.count)
			}
		}
		var modelEntries []types.DashboardGPUCapacityPipelineModel
		for model, cnt := range perModel {
			modelEntries = append(modelEntries, types.DashboardGPUCapacityPipelineModel{
				Model: model,
				GPUs:  cnt,
			})
		}
		sort.Slice(modelEntries, func(i, j int) bool { return modelEntries[i].GPUs > modelEntries[j].GPUs })
		pipelineList = append(pipelineList, types.DashboardGPUCapacityPipeline{
			Name:   pipeline,
			GPUs:   int64(totalCount),
			Models: modelEntries,
		})
	}
	sort.Slice(pipelineList, func(i, j int) bool { return pipelineList[i].GPUs > pipelineList[j].GPUs })

	modelList := make([]types.DashboardGPUModelCapacity, 0, len(modelCounts))
	for model, count := range modelCounts {
		modelList = append(modelList, types.DashboardGPUModelCapacity{Model: model, Count: int64(count)})
	}
	sort.Slice(modelList, func(i, j int) bool { return modelList[i].Count > modelList[j].Count })

	cap := float64(0)
	if totalGPUs > 0 {
		cap = 1.0
	}

	return &types.DashboardGPUCapacity{
		TotalGPUs:         int64(totalGPUs),
		ActiveGPUs:        int64(totalGPUs),
		AvailableCapacity: cap,
		Models:            modelList,
		PipelineGPUs:      pipelineList,
	}, nil
}
