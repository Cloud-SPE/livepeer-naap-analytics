package clickhouse

import (
	"context"
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

	// Query 1: observed orchestrator count in the selected window.
	var activeOrch uint64
	if pipeline != "" || modelID != "" {
		orchCountSQL := `
			SELECT countDistinct(s.orch_address) AS observed
			FROM naap.api_observed_capability_offer s
			WHERE s.last_seen >= ? AND s.last_seen < ?
			  AND s.capability_family = 'builtin'
			  AND (? = '' OR s.canonical_pipeline = ?)
			  AND (? = '' OR s.model_id = ?)
		`
		if err := r.conn.QueryRow(ctx, orchCountSQL,
			windowStart, now,
			pipeline, pipeline, modelID, modelID,
		).Scan(&activeOrch); err != nil {
			return nil, fmt.Errorf("dashboard kpi orch count: %w", err)
		}
	} else {
		if err := r.conn.QueryRow(ctx, `
			SELECT countDistinct(orch_address) AS observed
			FROM naap.canonical_capability_snapshots_store
			WHERE snapshot_ts >= ? AND snapshot_ts < ?
		`, windowStart, now).Scan(&activeOrch); err != nil {
			return nil, fmt.Errorf("dashboard kpi orch count: %w", err)
		}
	}

	// Query 2: hourly session counts from the additive streaming demand spine.
	sessionRows, err := r.conn.Query(ctx, `
		SELECT window_start, sum(requested_sessions) AS sessions, sum(startup_success_sessions) AS successes
		FROM naap.api_hourly_streaming_demand
		WHERE window_start >= ?
		  AND (? = '' OR pipeline_id = ?)
		  AND (? = '' OR model_id = ?)
		GROUP BY window_start ORDER BY window_start
	`, windowStart, pipeline, pipeline, modelID, modelID)
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
		FROM naap.api_hourly_streaming_demand
		WHERE window_start >= ?
		  AND (? = '' OR pipeline_id = ?)
		  AND (? = '' OR model_id = ?)
		GROUP BY window_start ORDER BY window_start
	`, windowStart, pipeline, pipeline, modelID, modelID)
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
		SuccessRate:         types.MetricValue{Value: successRate},
		OrchestratorsOnline: types.MetricValue{Value: float64(activeOrch)},
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
		       sum(ifNull(output_fps_sum, 0)) AS fps_sum,
		       toFloat64(sum(ifNull(status_samples, 0))) AS fps_samples
		FROM naap.api_hourly_streaming_demand
		WHERE window_start >= ? AND pipeline_id != ''
		GROUP BY pipeline_id, model_id
		ORDER BY sessions DESC
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard pipelines: %w", err)
	}
	defer rows.Close()

	// Group by pipeline. Recompute avg FPS from additive fields (output_fps_sum /
	// status_samples) rather than averaging model-level averages.
	pipelineMap := map[string]*types.DashboardPipelineUsage{}
	pipelineFpsSum := map[string]float64{}
	pipelineFpsSamples := map[string]float64{}
	var pipelineOrder []string

	for rows.Next() {
		var pipelineID, modelID string
		var sessions uint64
		var mins, fpsSum, fpsSamples float64
		if err := rows.Scan(&pipelineID, &modelID, &sessions, &mins, &fpsSum, &fpsSamples); err != nil {
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
			AvgFps:   divSafe(fpsSum, fpsSamples),
		})
		pipelineFpsSum[pipelineID] += fpsSum
		pipelineFpsSamples[pipelineID] += fpsSamples
	}

	for name, p := range pipelineMap {
		p.AvgFps = divSafe(pipelineFpsSum[name], pipelineFpsSamples[name])
	}

	// Re-sort by aggregated pipeline session total DESC before truncating to limit.
	sort.SliceStable(pipelineOrder, func(i, j int) bool {
		return pipelineMap[pipelineOrder[i]].Sessions > pipelineMap[pipelineOrder[j]].Sessions
	})

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
		       sum(ifNull(known_sessions_count, 0)) AS known_sessions,
		       sum(ifNull(startup_success_sessions, 0)) AS success_sessions,
		       sum(ifNull(effective_success_rate, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS eff_rate,
		       sum(ifNull(no_swap_rate, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS ns_rate,
		       sum(ifNull(sla_score, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS sla
		FROM naap.api_hourly_streaming_sla
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
		var requested, knownSessions, successSessions uint64
		var effRate, noSwap, sla *float64
		if err := slaRows.Scan(&addr, &pipelineID, &modelID, &gpuID, &requested, &knownSessions, &successSessions, &effRate, &noSwap, &sla); err != nil {
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
		o.orch.KnownSessions += int64(knownSessions)
		o.orch.SuccessSessions += int64(successSessions)

		if _, ok := o.pipelineModelMap[pipelineID]; !ok {
			o.pipelineModelMap[pipelineID] = map[string]bool{}
		}
		o.pipelineModelMap[pipelineID][modelID] = true
	}

	latestSLARows, err := r.conn.Query(ctx, `
		SELECT orchestrator_address,
		       window_start,
		       sum(ifNull(sla_score, 0) * requested_sessions) / nullIf(sum(requested_sessions), 0) AS sla
		FROM naap.api_hourly_streaming_sla
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

	// Resolve display URI from the latest orchestrator identity helper rather
	// than rescanning observed inventory for each request.
	stateRows, err := r.conn.Query(ctx, `
		SELECT orch_address, ifNull(orchestrator_uri, '') AS orchestrator_uri
		FROM naap.api_orchestrator_identity
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

	// Pipeline/model membership is derived from observed offers inside the
	// selected window; per-orchestrator GPU totals are computed separately with a
	// single DISTINCT so GPUs shared across models are not double-counted.
	pmRows, err := r.conn.Query(ctx, `
		SELECT orch_address,
		       ifNull(nullIf(canonical_pipeline, ''), ifNull(nullIf(offered_name, ''), 'unknown')) AS pipeline_id,
		       model_id
		FROM naap.api_observed_capability_offer
		WHERE last_seen >= ? AND last_seen < ?
		  AND model_id != ''
		GROUP BY orch_address, pipeline_id, model_id
	`, windowStart, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators pipeline models: %w", err)
	}
	defer pmRows.Close()

	for pmRows.Next() {
		var addr, pipelineID, modelID string
		if err := pmRows.Scan(&addr, &pipelineID, &modelID); err != nil {
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
	}

	gpuRows, err := r.conn.Query(ctx, `
		SELECT orch_address, toInt64(count(DISTINCT gpu_id)) AS gpu_count
		FROM naap.api_observed_capability_offer
		WHERE last_seen >= ? AND last_seen < ?
		  AND hardware_present = 1
		  AND gpu_id IS NOT NULL AND gpu_id != ''
		GROUP BY orch_address
	`, windowStart, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators gpu count: %w", err)
	}
	defer gpuRows.Close()

	orchGPUs := map[string]int64{}
	for gpuRows.Next() {
		var addr string
		var gpuCount int64
		if err := gpuRows.Scan(&addr, &gpuCount); err != nil {
			return nil, fmt.Errorf("dashboard orchestrators gpu count scan: %w", err)
		}
		orchGPUs[addr] = gpuCount
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

// GetDashboardGPUCapacity serves GET /v1/dashboard/gpu-capacity using observed
// 24h offer inventory rows that carry GPU identity and metadata.
func (r *Repo) GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error) {
	end := time.Now().UTC()
	start := end.Add(-observedInventoryHours * time.Hour)
	rows, err := r.conn.Query(ctx, `
		SELECT orch_address, canonical_pipeline, model_id, gpu_id,
		       ifNull(anyLast(gpu_model_name), 'Unknown') AS gpu_model
		FROM naap.api_observed_capability_offer
		WHERE last_seen >= ? AND last_seen < ?
		  AND hardware_present = 1
		  AND canonical_pipeline != '' AND model_id != ''
		  AND gpu_id IS NOT NULL AND gpu_id != ''
		GROUP BY orch_address, canonical_pipeline, model_id, gpu_id
	`, start, end)
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
		TotalGPUs:    totalGPUs,
		Models:       models,
		PipelineGPUs: pipelineGPUs,
	}, nil
}

// GetDashboardPipelineCatalog serves GET /v1/dashboard/pipeline-catalog.
func (r *Repo) GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	end := time.Now().UTC()
	start := end.Add(-observedInventoryHours * time.Hour)
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT canonical_pipeline, model_id
		FROM naap.api_observed_capability_offer
		WHERE last_seen >= ? AND last_seen < ?
		  AND capability_family = 'builtin'
		  AND canonical_pipeline != '' AND model_id != ''
		ORDER BY canonical_pipeline, model_id
	`, start, end)
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
	end := time.Now().UTC()
	start := end.Add(-observedInventoryHours * time.Hour)
	rows, err := r.conn.Query(ctx, `
		WITH latest_pricing AS (
		    SELECT *
		    FROM (
		        SELECT
		            p.*,
		            row_number() OVER (
		                PARTITION BY p.orch_address,
		                             ifNull(p.canonical_pipeline, p.external_capability_name),
		                             ifNull(p.model_id, '')
		                ORDER BY p.last_seen DESC, p.capability_version_id DESC
		            ) AS rn
		        FROM naap.api_observed_capability_pricing p
		        WHERE p.last_seen >= ? AND p.last_seen < ?
		          AND p.price_per_unit > 0
		          AND p.pixels_per_unit > 0
		          AND ifNull(p.canonical_pipeline, p.external_capability_name) != ''
		    )
		    WHERE rn = 1
		),
		builtin_offer AS (
		    SELECT orch_address,
		           ifNull(canonical_pipeline, '') AS canonical_pipeline,
		           model_id,
		           max(toUInt8(hardware_present = 1 OR ifNull(warm, toUInt8(0)) = 1)) AS is_warm
		    FROM naap.api_observed_capability_offer
		    WHERE last_seen >= ? AND last_seen < ?
		    GROUP BY orch_address, canonical_pipeline, model_id
		),
		byoc_offer AS (
		    SELECT orch_address,
		           offered_name,
		           any(model_id) AS model_id,
		           max(toUInt8(hardware_present = 1 OR ifNull(warm, toUInt8(0)) = 1)) AS is_warm
		    FROM naap.api_observed_capability_offer
		    WHERE offered_name != ''
		      AND last_seen >= ? AND last_seen < ?
		    GROUP BY orch_address, offered_name
		),
		orchestrators AS (
		    SELECT orch_address, ifNull(orchestrator_uri, '') AS orchestrator_uri
		    FROM naap.api_orchestrator_identity
		)
		SELECT
		    p.orch_address,
		    ifNull(o.orchestrator_uri, '') AS orchestrator_uri,
		    ifNull(p.canonical_pipeline, p.external_capability_name) AS pipeline,
		    ifNull(nullIf(p.model_id, ''), ifNull(byoc_offer.model_id, ifNull(p.external_capability_name, ''))) AS model_id,
		    p.price_per_unit,
		    p.pixels_per_unit,
		    greatest(ifNull(builtin_offer.is_warm, toUInt8(0)), ifNull(byoc_offer.is_warm, toUInt8(0))) AS is_warm
		FROM latest_pricing p
		LEFT JOIN orchestrators o
		  ON o.orch_address = p.orch_address
		LEFT JOIN builtin_offer
		  ON builtin_offer.orch_address = p.orch_address
		 AND builtin_offer.model_id = p.model_id
		 AND builtin_offer.canonical_pipeline = ifNull(p.canonical_pipeline, '')
		LEFT JOIN byoc_offer
		  ON byoc_offer.orch_address = p.orch_address
		 AND byoc_offer.offered_name = ifNull(p.external_capability_name, '')
		ORDER BY p.orch_address, pipeline, model_id
	`, start, end, start, end, start, end)
	if err != nil {
		return nil, fmt.Errorf("dashboard pricing: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardPipelinePricing

	for rows.Next() {
		var addr, uri, pipeline, model string
		var priceWeiPerUnit, pixelsPerUnit int64
		var isWarm uint8
		if err := rows.Scan(&addr, &uri, &pipeline, &model, &priceWeiPerUnit, &pixelsPerUnit, &isWarm); err != nil {
			return nil, fmt.Errorf("dashboard pricing scan: %w", err)
		}
		result = append(result, types.DashboardPipelinePricing{
			OrchAddress:     addr,
			OrchName:        hostnameFromURI(uri),
			Pipeline:        pipeline,
			Model:           model,
			PriceWeiPerUnit: priceWeiPerUnit,
			PixelsPerUnit:   pixelsPerUnit,
			IsWarm:          isWarm == 1,
		})
	}

	return result, nil
}

// GetDashboardJobFeed serves GET /v1/dashboard/job-feed.
func (r *Repo) GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error) {
	// Resolve orch_address -> URI via the latest orchestrator identity helper so
	// callers receive a stable URL without scanning observed inventory.
	orchURIRows, err := r.conn.Query(ctx, `
		SELECT orch_address, ifNull(orchestrator_uri, '') AS uri
		FROM naap.api_orchestrator_identity
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard job feed orch uri: %w", err)
	}
	defer orchURIRows.Close()

	orchURI := map[string]string{}
	for orchURIRows.Next() {
		var addr, uri string
		if err := orchURIRows.Scan(&addr, &uri); err != nil {
			return nil, fmt.Errorf("dashboard job feed orch uri scan: %w", err)
		}
		orchURI[addr] = uri
	}

	rows, err := r.conn.Query(ctx, `
		SELECT event_id, pipeline, ifNull(model_id, '') AS model_id,
		       gateway, ifNull(orch_address, '') AS orch_address, state,
		       output_fps, input_fps, ifNull(started_at, last_seen) AS started_at, last_seen
		FROM naap.api_current_active_stream_state
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
			ID:                  eventID,
			Pipeline:            pipeline,
			Model:               modelID,
			Gateway:             gateway,
			OrchestratorAddress: orchAddr,
			OrchestratorURL:     orchURI[orchAddr],
			State:               state,
			InputFPS:            inputFPS,
			OutputFPS:           outputFPS,
			FirstSeen:           startedAt.Format(time.RFC3339),
			LastSeen:            lastSeen.Format(time.RFC3339),
			DurationSeconds:     &dur,
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
		FROM naap.api_fact_ai_batch_job
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
		FROM naap.api_fact_byoc_job
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
		FROM naap.api_fact_ai_batch_job
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
		FROM naap.api_fact_byoc_job
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
