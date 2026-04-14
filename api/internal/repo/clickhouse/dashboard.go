package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetDashboardKPI serves GET /v1/dashboard/kpi (streaming portion).
// Runs 3 parallel ClickHouse queries: orchestrator count, hourly sessions, hourly usage.
func (r *Repo) GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error) {
	now := time.Now().UTC()
	windowStart := now.Add(-time.Duration(windowHours) * time.Hour)

	// Query 1: orchestrator count
	var activeOrch, totalOrch uint64
	if err := r.conn.QueryRow(ctx, `
		SELECT countIf(last_seen > now() - INTERVAL ? MINUTE) AS active,
		       count() AS total
		FROM naap.api_latest_orchestrator_state
	`, activeOrchMinutes).Scan(&activeOrch, &totalOrch); err != nil {
		return nil, fmt.Errorf("dashboard kpi orch count: %w", err)
	}

	// Query 2: hourly session counts
	sessionRows, err := r.conn.Query(ctx, `
		SELECT hour, sum(requested_sessions) AS sessions, sum(startup_success_sessions) AS successes
		FROM naap.api_stream_hourly
		WHERE hour >= ?
		GROUP BY hour ORDER BY hour
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard kpi hourly sessions: %w", err)
	}
	defer sessionRows.Close()

	type sessionBucket struct {
		hour      time.Time
		sessions  uint64
		successes uint64
	}
	var sessionBuckets []sessionBucket
	for sessionRows.Next() {
		var b sessionBucket
		if err := sessionRows.Scan(&b.hour, &b.sessions, &b.successes); err != nil {
			return nil, fmt.Errorf("dashboard kpi scan sessions: %w", err)
		}
		sessionBuckets = append(sessionBuckets, b)
	}

	// Query 3: hourly usage minutes
	usageRows, err := r.conn.Query(ctx, `
		SELECT window_start, sum(total_minutes) AS mins
		FROM naap.api_network_demand
		WHERE window_start >= ?
		GROUP BY window_start ORDER BY window_start
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard kpi hourly usage: %w", err)
	}
	defer usageRows.Close()

	type usageBucket struct {
		windowStart time.Time
		mins        float64
	}
	var usageBuckets []usageBucket
	for usageRows.Next() {
		var b usageBucket
		if err := usageRows.Scan(&b.windowStart, &b.mins); err != nil {
			return nil, fmt.Errorf("dashboard kpi scan usage: %w", err)
		}
		usageBuckets = append(usageBuckets, b)
	}

	// Build hourly timeseries
	hourlySessions := make([]types.HourlyBucket, 0, len(sessionBuckets))
	var totalSessions, totalSuccesses uint64
	for _, b := range sessionBuckets {
		hourlySessions = append(hourlySessions, types.HourlyBucket{
			Hour:  b.hour.Format(time.RFC3339),
			Value: float64(b.sessions),
		})
		totalSessions += b.sessions
		totalSuccesses += b.successes
	}

	hourlyUsage := make([]types.HourlyBucket, 0, len(usageBuckets))
	var totalMins float64
	for _, b := range usageBuckets {
		hourlyUsage = append(hourlyUsage, types.HourlyBucket{
			Hour:  b.windowStart.Format(time.RFC3339),
			Value: b.mins,
		})
		totalMins += b.mins
	}

	// Compute period-over-period deltas
	successRate := divSafe(float64(totalSuccesses), float64(totalSessions)) * 100
	sessionDelta := computeDelta(hourlySessions)
	usageDelta := computeDelta(hourlyUsage)

	kpi := &types.DashboardKPI{
		SuccessRate:         types.MetricDelta{Value: successRate, Delta: 0},
		OrchestratorsOnline: types.MetricDelta{Value: float64(activeOrch), Delta: 0},
		DailyUsageMins:      types.MetricDelta{Value: totalMins, Delta: usageDelta},
		DailySessionCount:   types.MetricDelta{Value: float64(totalSessions), Delta: sessionDelta},
		DailyNetworkFeesEth: types.MetricDelta{Value: 0, Delta: 0}, // Phase 4
		TimeframeHours:      windowHours,
		HourlySessions:      hourlySessions,
		HourlyUsage:         hourlyUsage,
	}

	return kpi, nil
}

// computeDelta splits hourly buckets at the midpoint and returns the % change.
func computeDelta(buckets []types.HourlyBucket) float64 {
	if len(buckets) < 2 {
		return 0
	}
	mid := len(buckets) / 2
	var first, second float64
	for _, b := range buckets[:mid] {
		first += b.Value
	}
	for _, b := range buckets[mid:] {
		second += b.Value
	}
	if first == 0 {
		return 0
	}
	return ((second - first) / first) * 100
}

// GetDashboardPipelines serves GET /v1/dashboard/pipelines (streaming portion).
func (r *Repo) GetDashboardPipelines(ctx context.Context, limit int, windowHours int) ([]types.DashboardPipelineUsage, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	rows, err := r.conn.Query(ctx, `
		SELECT pipeline_id, ifNull(model_id, '') AS model_id,
		       sum(requested_sessions) AS sessions,
		       sum(total_minutes) AS mins,
		       sum(avg_output_fps * requested_sessions) / nullIf(sum(requested_sessions), 0) AS avg_fps
		FROM naap.api_network_demand
		WHERE window_start >= ? AND pipeline_id != ''
		GROUP BY pipeline_id, model_id
		ORDER BY sessions DESC
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard pipelines: %w", err)
	}
	defer rows.Close()

	// Group by pipeline
	pipelineMap := map[string]*types.DashboardPipelineUsage{}
	var pipelineOrder []string

	for rows.Next() {
		var pipelineID, modelID string
		var sessions uint64
		var mins, avgFps float64
		if err := rows.Scan(&pipelineID, &modelID, &sessions, &mins, &avgFps); err != nil {
			return nil, fmt.Errorf("dashboard pipelines scan: %w", err)
		}

		p, ok := pipelineMap[pipelineID]
		if !ok {
			p = &types.DashboardPipelineUsage{Name: pipelineID}
			pipelineMap[pipelineID] = p
			pipelineOrder = append(pipelineOrder, pipelineID)
		}
		p.Sessions += int64(sessions)
		p.Mins += mins
		p.ModelMins = append(p.ModelMins, types.DashboardPipelineModelMins{
			Model:    modelID,
			Mins:     mins,
			Sessions: int64(sessions),
			AvgFps:   avgFps,
		})
	}

	// Compute weighted average FPS per pipeline
	for _, p := range pipelineMap {
		var totalWeighted float64
		var totalSessions int64
		for _, m := range p.ModelMins {
			totalWeighted += m.AvgFps * float64(m.Sessions)
			totalSessions += m.Sessions
		}
		p.AvgFps = divSafe(totalWeighted, float64(totalSessions))
	}

	// Build result sorted by sessions DESC, limited
	result := make([]types.DashboardPipelineUsage, 0, limit)
	for _, name := range pipelineOrder {
		if len(result) >= limit {
			break
		}
		result = append(result, *pipelineMap[name])
	}

	return result, nil
}

// GetDashboardOrchestrators serves GET /v1/dashboard/orchestrators.
func (r *Repo) GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	// SLA compliance data
	slaRows, err := r.conn.Query(ctx, `
		SELECT orchestrator_address, pipeline_id, ifNull(model_id, '') AS m_id, ifNull(gpu_id, '') AS g_id,
		       sum(requested_sessions) AS total_requested,
		       sum(ifNull(effective_success_rate, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS eff_rate,
		       sum(ifNull(no_swap_rate, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS ns_rate,
		       sum(ifNull(sla_score, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS sla
		FROM naap.api_sla_compliance
		WHERE window_start >= ? AND orchestrator_address != ''
		GROUP BY orchestrator_address, pipeline_id, m_id, g_id
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators sla: %w", err)
	}
	defer slaRows.Close()

	type orchData struct {
		orch             types.DashboardOrchestrator
		totalRequested   int64
		weightedSuccess  float64
		weightedNoSwap   float64
		pipelineModelMap map[string]map[string]bool // pipeline -> set of models
	}
	orchMap := map[string]*orchData{}

	for slaRows.Next() {
		var addr, pipelineID, modelID, gpuID string
		var requested uint64
		var effRate, noSwap, sla *float64
		if err := slaRows.Scan(&addr, &pipelineID, &modelID, &gpuID, &requested, &effRate, &noSwap, &sla); err != nil {
			return nil, fmt.Errorf("dashboard orchestrators sla scan: %w", err)
		}

		o, ok := orchMap[addr]
		if !ok {
			o = &orchData{
				orch:             types.DashboardOrchestrator{Address: addr},
				pipelineModelMap: map[string]map[string]bool{},
			}
			orchMap[addr] = o
		}
		o.totalRequested += int64(requested)
		if effRate != nil {
			o.weightedSuccess += *effRate * float64(requested)
		}
		if noSwap != nil {
			o.weightedNoSwap += *noSwap * float64(requested)
		}
		o.orch.KnownSessions += int64(requested)

		if _, ok := o.pipelineModelMap[pipelineID]; !ok {
			o.pipelineModelMap[pipelineID] = map[string]bool{}
		}
		o.pipelineModelMap[pipelineID][modelID] = true
	}

	latestSLARows, err := r.conn.Query(ctx, `
		SELECT orchestrator_address,
		       window_start,
		       sum(ifNull(sla_score, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS sla
		FROM naap.api_sla_compliance
		WHERE window_start >= ?
		  AND orchestrator_address != ''
		  AND sla_score IS NOT NULL
		GROUP BY orchestrator_address, window_start
		ORDER BY orchestrator_address ASC, window_start DESC
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators latest sla: %w", err)
	}
	defer latestSLARows.Close()

	seenLatestSLA := map[string]bool{}
	for latestSLARows.Next() {
		var addr string
		var slaWindowStart time.Time
		var sla *float64
		if err := latestSLARows.Scan(&addr, &slaWindowStart, &sla); err != nil {
			return nil, fmt.Errorf("dashboard orchestrators latest sla scan: %w", err)
		}
		if seenLatestSLA[addr] {
			continue
		}
		seenLatestSLA[addr] = true

		o, ok := orchMap[addr]
		if !ok {
			o = &orchData{
				orch:             types.DashboardOrchestrator{Address: addr},
				pipelineModelMap: map[string]map[string]bool{},
			}
			orchMap[addr] = o
		}
		if sla != nil {
			slaValue := *sla
			slaWindowStartText := slaWindowStart.UTC().Format(time.RFC3339)
			o.orch.SLAScore = &slaValue
			o.orch.SLAWindowStart = &slaWindowStartText
		}
	}

	// Enrich with orchestrator state (URI, pipeline models)
	stateRows, err := r.conn.Query(ctx, `
		SELECT orch_address, uri
		FROM naap.api_latest_orchestrator_state
		WHERE last_seen > now() - INTERVAL 30 MINUTE
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators state: %w", err)
	}
	defer stateRows.Close()

	for stateRows.Next() {
		var addr, uri string
		if err := stateRows.Scan(&addr, &uri); err != nil {
			return nil, fmt.Errorf("dashboard orchestrators state scan: %w", err)
		}
		o, ok := orchMap[addr]
		if !ok {
			o = &orchData{
				orch:             types.DashboardOrchestrator{Address: addr},
				pipelineModelMap: map[string]map[string]bool{},
			}
			orchMap[addr] = o
		}
		o.orch.ServiceURI = uri
	}

	// Pipeline models from live announcements
	pmRows, err := r.conn.Query(ctx, `
		SELECT orch_address, pipeline_id, model_id, toInt64(count(DISTINCT nullIf(ifNull(gpu_id, ''), ''))) AS gpu_count
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL 30 MINUTE
		  AND pipeline_id != '' AND model_id != ''
		GROUP BY orch_address, pipeline_id, model_id
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators pipeline models: %w", err)
	}
	defer pmRows.Close()

	orchGPUs := map[string]int64{}
	for pmRows.Next() {
		var addr, pipelineID, modelID string
		var gpuCount int64
		if err := pmRows.Scan(&addr, &pipelineID, &modelID, &gpuCount); err != nil {
			return nil, fmt.Errorf("dashboard orchestrators pm scan: %w", err)
		}
		o, ok := orchMap[addr]
		if !ok {
			o = &orchData{
				orch:             types.DashboardOrchestrator{Address: addr},
				pipelineModelMap: map[string]map[string]bool{},
			}
			orchMap[addr] = o
		}
		if _, ok := o.pipelineModelMap[pipelineID]; !ok {
			o.pipelineModelMap[pipelineID] = map[string]bool{}
		}
		o.pipelineModelMap[pipelineID][modelID] = true
		orchGPUs[addr] += gpuCount
	}

	// Build result
	result := make([]types.DashboardOrchestrator, 0, len(orchMap))
	for addr, o := range orchMap {
		orch := o.orch

		if o.totalRequested > 0 {
			eff := divSafe(o.weightedSuccess, float64(o.totalRequested))
			noSwap := divSafe(o.weightedNoSwap, float64(o.totalRequested))
			orch.EffectiveSuccessRate = &eff
			orch.NoSwapRatio = &noSwap
			orch.SuccessRatio = eff * 100
		}

		// Build pipeline and pipelineModels
		pipelines := make([]string, 0, len(o.pipelineModelMap))
		pipelineModels := make([]types.DashboardPipelineModelOffer, 0, len(o.pipelineModelMap))
		for p, models := range o.pipelineModelMap {
			pipelines = append(pipelines, p)
			modelIDs := make([]string, 0, len(models))
			for m := range models {
				modelIDs = append(modelIDs, m)
			}
			sort.Strings(modelIDs)
			pipelineModels = append(pipelineModels, types.DashboardPipelineModelOffer{
				PipelineID: p,
				ModelIDs:   modelIDs,
			})
		}
		sort.Strings(pipelines)
		orch.Pipelines = pipelines
		orch.PipelineModels = pipelineModels
		orch.GPUCount = orchGPUs[addr]

		result = append(result, orch)
	}

	// Sort by known sessions DESC
	sort.Slice(result, func(i, j int) bool {
		return result[i].KnownSessions > result[j].KnownSessions
	})

	return result, nil
}

// GetDashboardGPUCapacity serves GET /v1/dashboard/gpu-capacity.
// Uses a 30-minute window (not activeOrchMinutes=10) because non-streaming
// orchestrators advertise less frequently than live-video-to-video ones.
//
// Three views of the inventory:
//   - TotalGPUs: distinct (orch, gpu_id) pairs across the whole network
//   - Models[]: top-level GPU hardware breakdown (e.g. "RTX 4090: 22 GPUs")
//   - PipelineGPUs[].Models[]: per-pipeline breakdown where each entry is
//     the number of distinct GPUs serving a specific AI model on that pipeline
//     (e.g. "live-video-to-video → streamdiffusion-sdxl: 27 GPUs").
//     A GPU serving multiple AI models within the same pipeline is counted
//     once per AI model.
func (r *Repo) GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT orch_address, pipeline_id, model_id, ifNull(gpu_id, '') AS gpu_id,
		       ifNull(anyLast(gpu_model_name), 'Unknown') AS gpu_model
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL 30 MINUTE
		  AND pipeline_id != '' AND model_id != ''
		GROUP BY orch_address, pipeline_id, model_id, gpu_id
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard gpu capacity: %w", err)
	}
	defer rows.Close()

	// uniqueGPUs: dedup for TotalGPUs and the top-level Models[] (GPU hardware)
	uniqueGPUs := map[string]string{} // "orch|gpu" -> gpu hardware model
	// pipelineAIModelGPUs: pipeline -> ai_model -> set of "orch|gpu"
	pipelineAIModelGPUs := map[string]map[string]map[string]bool{}
	// pipelineGPUKeys: pipeline -> set of "orch|gpu" (for the pipeline-level total)
	pipelineGPUKeys := map[string]map[string]bool{}

	for rows.Next() {
		var orchAddr, pipelineID, aiModel, gpuID, gpuHardware string
		if err := rows.Scan(&orchAddr, &pipelineID, &aiModel, &gpuID, &gpuHardware); err != nil {
			return nil, fmt.Errorf("dashboard gpu capacity scan: %w", err)
		}
		if gpuHardware == "" {
			gpuHardware = "Unknown"
		}
		gpuKey := orchAddr + "|" + gpuID
		uniqueGPUs[gpuKey] = gpuHardware

		if _, ok := pipelineGPUKeys[pipelineID]; !ok {
			pipelineGPUKeys[pipelineID] = map[string]bool{}
			pipelineAIModelGPUs[pipelineID] = map[string]map[string]bool{}
		}
		pipelineGPUKeys[pipelineID][gpuKey] = true

		if _, ok := pipelineAIModelGPUs[pipelineID][aiModel]; !ok {
			pipelineAIModelGPUs[pipelineID][aiModel] = map[string]bool{}
		}
		pipelineAIModelGPUs[pipelineID][aiModel][gpuKey] = true
	}

	// Total GPUs and top-level Models[] (GPU hardware breakdown)
	gpuHardwareCounts := map[string]int64{}
	for _, hw := range uniqueGPUs {
		gpuHardwareCounts[hw]++
	}
	totalGPUs := int64(len(uniqueGPUs))

	models := make([]types.DashboardGPUModelCapacity, 0, len(gpuHardwareCounts))
	for model, count := range gpuHardwareCounts {
		models = append(models, types.DashboardGPUModelCapacity{Model: model, Count: count})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].Count > models[j].Count })

	// PipelineGPUs[]: one entry per pipeline; Models[] is per-AI-model GPU count
	pipelineGPUs := make([]types.DashboardGPUCapacityPipeline, 0, len(pipelineAIModelGPUs))
	for pipeline, aiModels := range pipelineAIModelGPUs {
		pModels := make([]types.DashboardGPUCapacityPipelineModel, 0, len(aiModels))
		for aiModel, gpuKeys := range aiModels {
			pModels = append(pModels, types.DashboardGPUCapacityPipelineModel{
				Model: aiModel,
				GPUs:  int64(len(gpuKeys)),
			})
		}
		sort.Slice(pModels, func(i, j int) bool {
			if pModels[i].GPUs != pModels[j].GPUs {
				return pModels[i].GPUs > pModels[j].GPUs
			}
			return pModels[i].Model < pModels[j].Model
		})
		pipelineGPUs = append(pipelineGPUs, types.DashboardGPUCapacityPipeline{
			Name:   pipeline,
			GPUs:   int64(len(pipelineGPUKeys[pipeline])),
			Models: pModels,
		})
	}
	sort.Slice(pipelineGPUs, func(i, j int) bool { return pipelineGPUs[i].GPUs > pipelineGPUs[j].GPUs })

	return &types.DashboardGPUCapacity{
		TotalGPUs:         totalGPUs,
		ActiveGPUs:        totalGPUs,
		AvailableCapacity: 1.0,
		Models:            models,
		PipelineGPUs:      pipelineGPUs,
	}, nil
}

// GetDashboardPipelineCatalog serves GET /v1/dashboard/pipeline-catalog.
func (r *Repo) GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT pipeline_id, model_id
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL 30 MINUTE
		  AND pipeline_id != '' AND model_id != ''
		ORDER BY pipeline_id, model_id
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard pipeline catalog: %w", err)
	}
	defer rows.Close()

	catalogMap := map[string]*types.DashboardPipelineCatalogEntry{}
	var order []string

	for rows.Next() {
		var pipelineID, modelID string
		if err := rows.Scan(&pipelineID, &modelID); err != nil {
			return nil, fmt.Errorf("dashboard pipeline catalog scan: %w", err)
		}
		entry, ok := catalogMap[pipelineID]
		if !ok {
			entry = &types.DashboardPipelineCatalogEntry{
				ID:      pipelineID,
				Name:    pipelineID,
				Regions: []string{},
			}
			catalogMap[pipelineID] = entry
			order = append(order, pipelineID)
		}
		entry.Models = append(entry.Models, modelID)
	}

	result := make([]types.DashboardPipelineCatalogEntry, 0, len(order))
	for _, id := range order {
		result = append(result, *catalogMap[id])
	}
	return result, nil
}

// GetDashboardPricing serves GET /v1/dashboard/pricing.
func (r *Repo) GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error) {
	// Step 1: Build capability_id → pipeline_id mapping from pipeline models.
	// The capabilities_prices array uses numeric capability IDs; the pipeline
	// models table knows which pipeline each capability+model corresponds to.
	pmRows, err := r.conn.Query(ctx, `
		SELECT DISTINCT orch_address, pipeline_id, model_id
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL 30 MINUTE
		  AND pipeline_id != '' AND model_id != ''
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard pricing pipeline models: %w", err)
	}
	defer pmRows.Close()

	// orchAddress+model → pipeline
	orchModelPipeline := map[string]string{}
	for pmRows.Next() {
		var addr, pipelineID, modelID string
		if err := pmRows.Scan(&addr, &pipelineID, &modelID); err != nil {
			return nil, fmt.Errorf("dashboard pricing pm scan: %w", err)
		}
		orchModelPipeline[addr+"|"+modelID] = pipelineID
	}

	// Step 2: Read orchestrator state and parse pricing
	rows, err := r.conn.Query(ctx, `
		SELECT orch_address, uri, raw_capabilities
		FROM naap.api_latest_orchestrator_state
		WHERE last_seen > now() - INTERVAL 30 MINUTE
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard pricing: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardPipelinePricing

	for rows.Next() {
		var addr, uri, rawCaps string
		if err := rows.Scan(&addr, &uri, &rawCaps); err != nil {
			return nil, fmt.Errorf("dashboard pricing scan: %w", err)
		}

		entries := parsePricingFromCapabilities(addr, uri, rawCaps, orchModelPipeline)
		result = append(result, entries...)
	}

	return result, nil
}

// parsePricingFromCapabilities extracts pricing entries from the raw_capabilities JSON.
// Handles two formats:
//   - capabilities_prices[]: per-capability pricing with {pricePerUnit, pixelsPerUnit, capability, constraint}
//   - price_info: global fallback pricing with {pricePerUnit, pixelsPerUnit}
//
// Pipeline names are resolved from capabilities.constraints.PerCapability when available,
// otherwise the numeric capability ID is used as-is.
func parsePricingFromCapabilities(addr, uri, rawCaps string, orchModelPipeline map[string]string) []types.DashboardPipelinePricing {
	if rawCaps == "" {
		return nil
	}

	type capPriceEntry struct {
		PricePerUnit  int64  `json:"pricePerUnit"`
		PixelsPerUnit int64  `json:"pixelsPerUnit"`
		Capability    int    `json:"capability"`
		Constraint    string `json:"constraint"` // model name
	}

	type priceInfo struct {
		PricePerUnit  int64 `json:"pricePerUnit"`
		PixelsPerUnit int64 `json:"pixelsPerUnit"`
	}

	type rawCapsShape struct {
		CapabilitiesPrices []capPriceEntry `json:"capabilities_prices"`
		PriceInfo          *priceInfo      `json:"price_info"`
	}

	var caps rawCapsShape
	if err := json.Unmarshal([]byte(rawCaps), &caps); err != nil {
		return nil
	}

	orchName := hostnameFromURI(uri)

	// Per-capability pricing entries
	if len(caps.CapabilitiesPrices) > 0 {
		result := make([]types.DashboardPipelinePricing, 0, len(caps.CapabilitiesPrices))
		for _, cp := range caps.CapabilitiesPrices {
			// Resolve pipeline from the orch+model → pipeline mapping
			pipeline := orchModelPipeline[addr+"|"+cp.Constraint]
			if pipeline == "" {
				pipeline = fmt.Sprintf("capability-%d", cp.Capability)
			}
			result = append(result, types.DashboardPipelinePricing{
				OrchAddress:     addr,
				OrchName:        orchName,
				Pipeline:        pipeline,
				Model:           cp.Constraint,
				PriceWeiPerUnit: cp.PricePerUnit,
				PixelsPerUnit:   cp.PixelsPerUnit,
				IsWarm:          true,
			})
		}
		return result
	}

	// Global fallback pricing
	if caps.PriceInfo != nil && caps.PriceInfo.PricePerUnit > 0 {
		return []types.DashboardPipelinePricing{{
			OrchAddress:     addr,
			OrchName:        orchName,
			Pipeline:        "",
			PriceWeiPerUnit: caps.PriceInfo.PricePerUnit,
			PixelsPerUnit:   caps.PriceInfo.PixelsPerUnit,
			IsWarm:          true,
		}}
	}

	return nil
}

// GetDashboardJobFeed serves GET /v1/dashboard/job-feed.
func (r *Repo) GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT event_id, pipeline, ifNull(model_id, '') AS model_id,
		       gateway, ifNull(orch_address, '') AS orch_address, state,
		       output_fps, input_fps, ifNull(started_at, last_seen) AS started_at, last_seen
		FROM naap.api_active_stream_state
		ORDER BY last_seen DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("dashboard job feed: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	result := make([]types.DashboardJobFeedItem, 0, limit)
	for rows.Next() {
		var eventID, pipeline, modelID, gateway, orchAddr, state string
		var outputFPS, inputFPS float64
		var startedAt, lastSeen time.Time
		if err := rows.Scan(&eventID, &pipeline, &modelID, &gateway, &orchAddr, &state,
			&outputFPS, &inputFPS, &startedAt, &lastSeen); err != nil {
			return nil, fmt.Errorf("dashboard job feed scan: %w", err)
		}
		dur := now.Sub(startedAt).Seconds()
		result = append(result, types.DashboardJobFeedItem{
			ID:              eventID,
			Pipeline:        pipeline,
			Model:           modelID,
			Gateway:         gateway,
			OrchestratorURL: orchAddr,
			State:           state,
			InputFPS:        inputFPS,
			OutputFPS:       outputFPS,
			FirstSeen:       startedAt.Format(time.RFC3339),
			LastSeen:        lastSeen.Format(time.RFC3339),
			DurationSeconds: &dur,
		})
	}
	return result, nil
}

// GetDashboardJobsOverview serves the requests portion of dashboard KPI.
func (r *Repo) GetDashboardJobsOverview(ctx context.Context, windowHours int) (*types.DashboardJobsOverview, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	var aiTotal, aiSelected, aiNoOrch uint64
	var aiSuccessRate, aiAvgDur float64
	err := r.conn.QueryRow(ctx, `
		SELECT count() AS total,
		       countIf(selection_outcome = 'selected') AS selected,
		       countIf(selection_outcome = 'no_orch') AS no_orch,
		       if(count() > 0, countIf(ifNull(success, 0) = 1) / toFloat64(count()), 0.0) AS success_rate,
		       if(count() > 0, avg(toFloat64(duration_ms)), 0.0) AS avg_duration_ms
		FROM naap.api_ai_batch_jobs
		WHERE completed_at >= ?
	`, windowStart).Scan(&aiTotal, &aiSelected, &aiNoOrch, &aiSuccessRate, &aiAvgDur)
	if err != nil {
		return nil, fmt.Errorf("dashboard jobs overview ai-batch: %w", err)
	}
	aiBatch := types.DashboardJobsStats{
		TotalJobs: int64(aiTotal), SelectedJobs: int64(aiSelected), NoOrchJobs: int64(aiNoOrch),
		SuccessRate: aiSuccessRate, AvgDurationMs: aiAvgDur,
	}

	var byocTotal, byocSelected, byocNoOrch uint64
	var byocSuccessRate, byocAvgDur float64
	err = r.conn.QueryRow(ctx, `
		SELECT count() AS total,
		       countIf(selection_outcome = 'selected') AS selected,
		       countIf(selection_outcome = 'no_orch') AS no_orch,
		       if(count() > 0, countIf(ifNull(success, 0) = 1) / toFloat64(count()), 0.0) AS success_rate,
		       if(count() > 0, avg(toFloat64(duration_ms)), 0.0) AS avg_duration_ms
		FROM naap.api_byoc_jobs
		WHERE completed_at >= ?
	`, windowStart).Scan(&byocTotal, &byocSelected, &byocNoOrch, &byocSuccessRate, &byocAvgDur)
	byoc := types.DashboardJobsStats{
		TotalJobs: int64(byocTotal), SelectedJobs: int64(byocSelected), NoOrchJobs: int64(byocNoOrch),
		SuccessRate: byocSuccessRate, AvgDurationMs: byocAvgDur,
	}
	if err != nil {
		return nil, fmt.Errorf("dashboard jobs overview byoc: %w", err)
	}

	return &types.DashboardJobsOverview{AIBatch: aiBatch, BYOC: byoc}, nil
}

// GetDashboardJobsByPipeline returns AI Batch job stats grouped by pipeline.
func (r *Repo) GetDashboardJobsByPipeline(ctx context.Context, windowHours int) ([]types.DashboardJobsByPipelineRow, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	rows, err := r.conn.Query(ctx, `
		SELECT pipeline,
		       count() AS total,
		       countIf(selection_outcome = 'selected') AS selected,
		       countIf(selection_outcome = 'no_orch') AS no_orch,
		       if(count() > 0, countIf(ifNull(success, 0) = 1) / toFloat64(count()), 0.0) AS success_rate,
		       if(count() > 0, avg(toFloat64(duration_ms)), 0.0) AS avg_duration_ms
		FROM naap.api_ai_batch_jobs
		WHERE completed_at >= ?
		GROUP BY pipeline ORDER BY total DESC
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard jobs by pipeline: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardJobsByPipelineRow
	for rows.Next() {
		var pipeline string
		var total, selected, noOrch uint64
		var successRate, avgDur float64
		if err := rows.Scan(&pipeline, &total, &selected, &noOrch, &successRate, &avgDur); err != nil {
			return nil, fmt.Errorf("dashboard jobs by pipeline scan: %w", err)
		}
		result = append(result, types.DashboardJobsByPipelineRow{
			Pipeline: pipeline, TotalJobs: int64(total), SelectedJobs: int64(selected),
			NoOrchJobs: int64(noOrch), SuccessRate: successRate, AvgDurationMs: avgDur,
		})
	}
	return result, nil
}

// GetDashboardJobsByCapability returns BYOC job stats grouped by capability.
func (r *Repo) GetDashboardJobsByCapability(ctx context.Context, windowHours int) ([]types.DashboardJobsByCapabilityRow, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	rows, err := r.conn.Query(ctx, `
		SELECT capability,
		       count() AS total,
		       countIf(selection_outcome = 'selected') AS selected,
		       countIf(selection_outcome = 'no_orch') AS no_orch,
		       if(count() > 0, countIf(ifNull(success, 0) = 1) / toFloat64(count()), 0.0) AS success_rate,
		       if(count() > 0, avg(toFloat64(duration_ms)), 0.0) AS avg_duration_ms
		FROM naap.api_byoc_jobs
		WHERE completed_at >= ?
		GROUP BY capability ORDER BY total DESC
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard jobs by capability: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardJobsByCapabilityRow
	for rows.Next() {
		var capability string
		var total, selected, noOrch uint64
		var successRate, avgDur float64
		if err := rows.Scan(&capability, &total, &selected, &noOrch, &successRate, &avgDur); err != nil {
			return nil, fmt.Errorf("dashboard jobs by capability scan: %w", err)
		}
		result = append(result, types.DashboardJobsByCapabilityRow{
			Capability: capability, TotalJobs: int64(total), SelectedJobs: int64(selected),
			NoOrchJobs: int64(noOrch), SuccessRate: successRate, AvgDurationMs: avgDur,
		})
	}
	return result, nil
}
