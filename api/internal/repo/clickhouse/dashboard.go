package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// sessionBucket holds per-hour session counts used by KPI delta computations.
type sessionBucket struct {
	hour      time.Time
	sessions  uint64
	successes uint64
}

// GetDashboardKPI serves GET /v1/dashboard/kpi (streaming portion).
// Reads dashboard KPI inputs from ClickHouse and computes period-over-period deltas.
func (r *Repo) GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error) {
	now := time.Now().UTC()
	windowDuration := time.Duration(windowHours) * time.Hour
	windowStart := now.Add(-windowDuration)
	previousWindowStart := windowStart.Add(-windowDuration)

	// Phase 6.5: consolidate sessions+usage into one scan over
	// api_hourly_streaming_demand, and current+previous orch counts into one
	// scan over capability observations. KPI intentionally spans two data
	// sources (demand time-series + capability inventory), so 2 queries is
	// the natural floor — collapsing further would require a UNION with a
	// row-type discriminator that just moves the branching into SQL.
	activeOrch, previousActiveOrch, err := r.countActiveAndPrevious(ctx, windowStart, now, previousWindowStart, pipeline, modelID)
	if err != nil {
		return nil, err
	}

	rows, err := r.conn.Query(ctx, `
		SELECT window_start,
		       sum(requested_sessions)        AS sessions,
		       sum(startup_success_sessions)  AS successes,
		       sum(total_minutes)             AS mins
		FROM naap.api_hourly_streaming_demand
		WHERE window_start >= ?
		  AND (? = '' OR pipeline_id = ?)
		  AND (? = '' OR model_id = ?)
		GROUP BY window_start
		ORDER BY window_start
	`, windowStart, pipeline, pipeline, modelID, modelID)
	if err != nil {
		return nil, fmt.Errorf("dashboard kpi hourly series: %w", err)
	}
	defer rows.Close()

	var sessionBuckets []sessionBucket
	hourlySessions := make([]types.HourlyBucket, 0)
	hourlyUsage := make([]types.HourlyBucket, 0)
	var totalSessions, totalSuccesses uint64
	var totalMins float64
	for rows.Next() {
		var hour time.Time
		var sessions, successes uint64
		var mins float64
		if err := rows.Scan(&hour, &sessions, &successes, &mins); err != nil {
			return nil, fmt.Errorf("dashboard kpi scan: %w", err)
		}
		sessionBuckets = append(sessionBuckets, sessionBucket{hour: hour, sessions: sessions, successes: successes})
		hourlySessions = append(hourlySessions, types.HourlyBucket{Hour: hour.Format(time.RFC3339), Value: float64(sessions)})
		hourlyUsage = append(hourlyUsage, types.HourlyBucket{Hour: hour.Format(time.RFC3339), Value: mins})
		totalSessions += sessions
		totalSuccesses += successes
		totalMins += mins
	}

	// Compute period-over-period deltas
	successRate := divSafe(float64(totalSuccesses), float64(totalSessions)) * 100
	successRateDelta := computeSuccessRateDelta(sessionBuckets)
	orchDelta := computeDeltaFromValues(float64(previousActiveOrch), float64(activeOrch))
	sessionDelta := computeDelta(hourlySessions)
	usageDelta := computeDelta(hourlyUsage)

	kpi := &types.DashboardKPI{
		SuccessRate:         types.MetricDelta{Value: successRate, Delta: successRateDelta},
		OrchestratorsObserved: types.MetricDelta{Value: float64(activeOrch), Delta: orchDelta},
		DailyUsageMins:      types.MetricDelta{Value: totalMins, Delta: usageDelta},
		DailySessionCount:   types.MetricDelta{Value: float64(totalSessions), Delta: sessionDelta},
		DailyNetworkFeesEth: types.MetricDelta{Value: 0, Delta: 0}, // Phase 4
		TimeframeHours:      windowHours,
		HourlySessions:      hourlySessions,
		HourlyUsage:         hourlyUsage,
	}

	return kpi, nil
}

// countActiveAndPrevious returns the distinct orch_address counts for the
// current [windowStart, now] and prior [previousWindowStart, windowStart]
// windows in a single pass. Scope is streaming-capable offers only
// (supports_stream = 1) so the count is coherent with the rest of the
// streaming KPI panel, which sources from api_hourly_streaming_demand.
// Pipeline/model filters apply as optional predicates; empty strings
// disable them.
func (r *Repo) countActiveAndPrevious(ctx context.Context, windowStart, now, previousWindowStart time.Time, pipeline, modelID string) (uint64, uint64, error) {
	var current, previous uint64
	if err := r.conn.QueryRow(ctx, `
		SELECT
			countDistinctIf(orch_address, last_seen >= ? AND last_seen < ?) AS current_count,
			countDistinctIf(orch_address, last_seen >= ? AND last_seen < ?) AS previous_count
		FROM naap.api_observed_capability_offer
		WHERE last_seen >= ? AND last_seen < ?
		  AND supports_stream = 1
		  AND (? = '' OR canonical_pipeline = ?)
		  AND (? = '' OR model_id = ?)
	`,
		windowStart, now,
		previousWindowStart, windowStart,
		previousWindowStart, now,
		pipeline, pipeline, modelID, modelID,
	).Scan(&current, &previous); err != nil {
		return 0, 0, fmt.Errorf("dashboard kpi orch counts: %w", err)
	}
	return current, previous, nil
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
	return computeDeltaFromValues(first, second)
}

func computeDeltaFromValues(first, second float64) float64 {
	if first == 0 {
		return 0
	}
	return ((second - first) / first) * 100
}

// computeSuccessRateDelta splits session buckets at the midpoint and returns
// the absolute percentage-point difference between the two halves' success rates.
func computeSuccessRateDelta(buckets []sessionBucket) float64 {
	if len(buckets) < 2 {
		return 0
	}
	mid := len(buckets) / 2
	var firstSessions, firstSuccesses, secondSessions, secondSuccesses uint64
	for _, b := range buckets[:mid] {
		firstSessions += b.sessions
		firstSuccesses += b.successes
	}
	for _, b := range buckets[mid:] {
		secondSessions += b.sessions
		secondSuccesses += b.successes
	}
	firstRate := divSafe(float64(firstSuccesses), float64(firstSessions)) * 100
	secondRate := divSafe(float64(secondSuccesses), float64(secondSessions)) * 100
	return secondRate - firstRate
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
//
// Identity, capability membership, and observed GPU UUID count come from the
// resolver-written api_current_orchestrator snapshot (grain: one row per
// orch_address). Reliability and SLA metrics are aggregated live over
// the caller's requested window from api_hourly_streaming_sla, so
// passing `window=1h` vs `window=168h` produces materially different
// response bodies. Weighted-aggregate pattern:
// SUM(metric * requested_sessions) / NULLIF(SUM(requested_sessions), 0)
// matches the additive-primitives contract.
func (r *Repo) GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error) {
	if windowHours <= 0 {
		windowHours = 24
	}

	type orchData struct {
		addr               string
		uri                string
		gpuCount           int64
		pipelines          []string
		pipelineModelPairs [][]any

		totalRequested  int64
		knownSessions   int64
		successSessions int64
		weightedSuccess float64
		weightedNoSwap  float64

		slaScore       *float64
		slaWindowStart *time.Time
	}

	orchs := map[string]*orchData{}
	var order []string

	idRows, err := r.conn.Query(ctx, `
		SELECT
		    orch_address,
		    orchestrator_uri,
		    toInt64(gpu_count) AS gpu_count,
		    pipelines,
		    pipeline_model_pairs
		FROM naap.api_current_orchestrator
		ORDER BY orch_address ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators identity: %w", err)
	}
	for idRows.Next() {
		var addr, uri string
		var gpuCount int64
		var pipelines []string
		var pipelineModelPairs [][]any
		if err := idRows.Scan(&addr, &uri, &gpuCount, &pipelines, &pipelineModelPairs); err != nil {
			idRows.Close()
			return nil, fmt.Errorf("dashboard orchestrators identity scan: %w", err)
		}
		orchs[addr] = &orchData{
			addr:               addr,
			uri:                uri,
			gpuCount:           gpuCount,
			pipelines:          pipelines,
			pipelineModelPairs: pipelineModelPairs,
		}
		order = append(order, addr)
	}
	idRows.Close()

	slaRows, err := r.conn.Query(ctx, `
		SELECT
		    orchestrator_address,
		    toInt64(sum(requested_sessions))                              AS total_requested,
		    toInt64(sum(ifNull(known_sessions_count, 0)))                 AS known_sessions,
		    toInt64(sum(ifNull(startup_success_sessions, 0)))             AS success_sessions,
		    sum(ifNull(effective_success_rate, 0) * requested_sessions)   AS weighted_success,
		    sum(ifNull(no_swap_rate, 0) * requested_sessions)             AS weighted_no_swap
		FROM naap.api_hourly_streaming_sla
		WHERE window_start >= now() - INTERVAL ? HOUR
		  AND orchestrator_address != ''
		GROUP BY orchestrator_address
	`, windowHours)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators sla aggregate: %w", err)
	}
	for slaRows.Next() {
		var addr string
		var totalRequested, known, success int64
		var weightedSuccess, weightedNoSwap float64
		if err := slaRows.Scan(&addr, &totalRequested, &known, &success, &weightedSuccess, &weightedNoSwap); err != nil {
			slaRows.Close()
			return nil, fmt.Errorf("dashboard orchestrators sla aggregate scan: %w", err)
		}
		o, ok := orchs[addr]
		if !ok {
			o = &orchData{addr: addr}
			orchs[addr] = o
			order = append(order, addr)
		}
		o.totalRequested = totalRequested
		o.knownSessions = known
		o.successSessions = success
		o.weightedSuccess = weightedSuccess
		o.weightedNoSwap = weightedNoSwap
	}
	slaRows.Close()

	latestRows, err := r.conn.Query(ctx, `
		SELECT
		    orchestrator_address,
		    window_start,
		    sum(ifNull(sla_score, 0) * requested_sessions)
		      / nullIf(toFloat64(sum(requested_sessions)), 0) AS sla
		FROM naap.api_hourly_streaming_sla
		WHERE window_start >= now() - INTERVAL ? HOUR
		  AND orchestrator_address != ''
		  AND sla_score IS NOT NULL
		GROUP BY orchestrator_address, window_start
		ORDER BY orchestrator_address ASC, window_start DESC
	`, windowHours)
	if err != nil {
		return nil, fmt.Errorf("dashboard orchestrators sla latest: %w", err)
	}
	for latestRows.Next() {
		var addr string
		var windowStart time.Time
		var sla *float64
		if err := latestRows.Scan(&addr, &windowStart, &sla); err != nil {
			latestRows.Close()
			return nil, fmt.Errorf("dashboard orchestrators sla latest scan: %w", err)
		}
		o, ok := orchs[addr]
		if !ok {
			continue
		}
		if o.slaWindowStart != nil {
			continue
		}
		o.slaScore = sla
		ws := windowStart
		o.slaWindowStart = &ws
	}
	latestRows.Close()

	result := make([]types.DashboardOrchestrator, 0, len(order))
	for _, addr := range order {
		o := orchs[addr]

		orch := types.DashboardOrchestrator{
			Address:         o.addr,
			ServiceURI:      o.uri,
			KnownSessions:   o.knownSessions,
			SuccessSessions: o.successSessions,
			GPUCount:        o.gpuCount,
		}

		if o.totalRequested > 0 {
			eff := divSafe(o.weightedSuccess, float64(o.totalRequested))
			ns := divSafe(o.weightedNoSwap, float64(o.totalRequested))
			orch.EffectiveSuccessRate = &eff
			orch.NoSwapRatio = &ns
			orch.SuccessRatio = eff * 100
		}
		if o.slaScore != nil {
			orch.SLAScore = o.slaScore
		}
		if o.slaWindowStart != nil {
			txt := o.slaWindowStart.UTC().Format(time.RFC3339)
			orch.SLAWindowStart = &txt
		}

		pmMap := map[string]map[string]bool{}
		for _, pair := range o.pipelineModelPairs {
			if len(pair) != 2 {
				continue
			}
			pipeline, _ := pair[0].(string)
			model, _ := pair[1].(string)
			if pipeline == "" {
				continue
			}
			if _, ok := pmMap[pipeline]; !ok {
				pmMap[pipeline] = map[string]bool{}
			}
			if model != "" {
				pmMap[pipeline][model] = true
			}
		}
		pipelineModels := make([]types.DashboardPipelineModelOffer, 0, len(pmMap))
		for p, models := range pmMap {
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
		sort.Slice(pipelineModels, func(i, j int) bool {
			return pipelineModels[i].PipelineID < pipelineModels[j].PipelineID
		})
		pipelines := append([]string(nil), o.pipelines...)
		sort.Strings(pipelines)
		orch.Pipelines = pipelines
		orch.PipelineModels = pipelineModels

		result = append(result, orch)
	}

	sort.SliceStable(result, func(i, j int) bool {
		if result[i].KnownSessions != result[j].KnownSessions {
			return result[i].KnownSessions > result[j].KnownSessions
		}
		return result[i].Address < result[j].Address
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

	// PipelineGPUs[]: one entry per pipeline; Models[] is per-AI-model GPU UUID count.
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

	// Count distinct GPUs currently serving at least one active stream.
	var gpusInUse uint64
	if err := r.conn.QueryRow(ctx, `
		SELECT count(DISTINCT concat(ifNull(attributed_orch_address, orch_address), '|', gpu_id))
		FROM naap.api_current_active_stream_state
		WHERE completed = 0
		  AND last_seen > now() - INTERVAL 30 MINUTE
		  AND gpu_id IS NOT NULL AND gpu_id != ''
	`).Scan(&gpusInUse); err != nil {
		return nil, fmt.Errorf("dashboard gpu capacity active gpus: %w", err)
	}

	activeGPUs := int64(gpusInUse)
	availableCapacity := totalGPUs - activeGPUs
	if availableCapacity < 0 {
		availableCapacity = 0
	}

	return &types.DashboardGPUCapacity{
		TotalGPUs:         totalGPUs,
		ActiveGPUs:        activeGPUs,
		AvailableCapacity: availableCapacity,
		Models:            models,
		PipelineGPUs:      pipelineGPUs,
	}, nil
}

// GetDashboardPipelineCatalog serves GET /v1/dashboard/pipeline-catalog.
func (r *Repo) GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	end := time.Now().UTC()
	start := end.Add(-observedInventoryHours * time.Hour)
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT canonical_pipeline, model_id
		FROM naap.api_current_capability
		WHERE last_seen >= ? AND last_seen < ?
		  AND hardware_present = 1
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
		)
		SELECT
		    p.orch_address,
		    ifNull(p.orchestrator_uri, '') AS orchestrator_uri,
		    ifNull(p.canonical_pipeline, p.external_capability_name) AS pipeline,
		    ifNull(nullIf(p.model_id, ''), ifNull(byoc_offer.model_id, ifNull(p.external_capability_name, ''))) AS model_id,
		    p.price_per_unit,
		    p.pixels_per_unit,
		    greatest(ifNull(builtin_offer.is_warm, toUInt8(0)), ifNull(byoc_offer.is_warm, toUInt8(0))) AS is_warm
		FROM latest_pricing p
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
	// Phase 3: api_current_active_stream_state exposes orchestrator_uri
	// without an identity lookup. The resolver writes it when available, and
	// the API view falls back to the session-current attributed URI.
	rows, err := r.conn.Query(ctx, `
		SELECT event_id, pipeline, ifNull(model_id, '') AS model_id,
		       gateway, ifNull(orch_address, '') AS orch_address,
		       ifNull(orchestrator_uri, '') AS orchestrator_uri, state,
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
		var eventID, pipeline, modelID, gateway, orchAddr, orchURI, state string
		var outputFPS, inputFPS float64
		var startedAt, lastSeen time.Time
		if err := rows.Scan(&eventID, &pipeline, &modelID, &gateway, &orchAddr, &orchURI, &state,
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
			OrchestratorURL:     orchURI,
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
// Reads pre-aggregated rows from api_hourly_request_demand split by
// capability_family ('builtin' = AI Batch, 'byoc' = BYOC), replacing
// per-request scans over api_fact_ai_batch_job and api_fact_byoc_job.
// AvgDurationMs treats NULL source durations as 0ms (duration_ms_sum /
// job_count); the legacy per-fact query ignored NULLs via CH's avg().
func (r *Repo) GetDashboardJobsOverview(ctx context.Context, windowHours int) (*types.DashboardJobsOverview, error) {
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	rows, err := r.conn.Query(ctx, `
		SELECT capability_family,
		       sum(job_count)       AS total,
		       sum(selected_count)  AS selected,
		       sum(no_orch_count)   AS no_orch,
		       sum(success_count)   AS success,
		       sum(duration_ms_sum) AS duration_sum
		FROM naap.api_hourly_request_demand
		WHERE window_start >= toStartOfHour(?)
		  AND capability_family IN ('builtin', 'byoc')
		GROUP BY capability_family
	`, windowStart)
	if err != nil {
		return nil, fmt.Errorf("dashboard jobs overview: %w", err)
	}
	defer rows.Close()

	var aiBatch, byoc types.DashboardJobsStats
	for rows.Next() {
		var family string
		var total, selected, noOrch, success uint64
		var durationSum int64
		if err := rows.Scan(&family, &total, &selected, &noOrch, &success, &durationSum); err != nil {
			return nil, fmt.Errorf("dashboard jobs overview scan: %w", err)
		}
		stats := types.DashboardJobsStats{
			TotalJobs:     int64(total),
			SelectedJobs:  int64(selected),
			NoOrchJobs:    int64(noOrch),
			SuccessRate:   divSafe(float64(success), float64(total)),
			AvgDurationMs: divSafe(float64(durationSum), float64(total)),
		}
		switch family {
		case "builtin":
			aiBatch = stats
		case "byoc":
			byoc = stats
		}
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
