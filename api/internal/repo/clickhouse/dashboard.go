package clickhouse

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// kpiBucket holds one hourly stream-activity row used for KPI delta computation.
type kpiBucket struct {
	ts        time.Time
	sessions  uint64
	successes uint64
	mins      float64
}

// GetDashboardKPI returns top-level KPI metrics for the dashboard (R16-1).
// Sources: naap.api_latest_orchestrator_state, naap.api_stream_hourly,
//
//	naap.api_network_demand.
func (r *Repo) GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error) {
	if windowHours <= 0 {
		windowHours = 24
	}

	// --- Active / total orchestrators ---
	netRow := r.conn.QueryRow(ctx, `
		SELECT
			countIf(last_seen > now() - INTERVAL ? MINUTE) AS active,
			count()                                         AS total
		FROM naap.api_latest_orchestrator_state
	`, activeOrchMinutes)

	var activeOrch, totalOrch uint64
	if err := netRow.Scan(&activeOrch, &totalOrch); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi net: %w", err)
	}

	// --- Hourly stream + usage-minutes buckets (joined in Go by hour) ---
	// Query 1: session counts from api_stream_hourly
	// api_stream_hourly has a `pipeline` column but no model_id; model_id filter
	// is applied only to the usage-minutes query below.
	streamWhere := "WHERE hour >= now() - INTERVAL ? HOUR"
	streamArgs := []any{windowHours}
	if pipeline != "" {
		streamWhere += " AND pipeline = ?"
		streamArgs = append(streamArgs, pipeline)
	}

	streamRows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(hour)           AS ts,
			sum(requested_sessions)       AS sessions,
			sum(startup_success_sessions) AS successes
		FROM naap.api_stream_hourly
		`+streamWhere+`
		GROUP BY ts
		ORDER BY ts ASC
	`, streamArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi history: %w", err)
	}
	defer streamRows.Close()

	bucketMap := map[time.Time]*kpiBucket{}
	var orderedTS []time.Time
	for streamRows.Next() {
		var b kpiBucket
		if err := streamRows.Scan(&b.ts, &b.sessions, &b.successes); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard kpi history scan: %w", err)
		}
		b.ts = b.ts.UTC().Truncate(time.Hour)
		if _, exists := bucketMap[b.ts]; !exists {
			orderedTS = append(orderedTS, b.ts)
		}
		bCopy := b
		bucketMap[b.ts] = &bCopy
	}
	if err := streamRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi history rows: %w", err)
	}

	// Query 2: usage minutes from api_network_demand
	// api_network_demand has pipeline_id and model_id columns.
	minsWhere := "WHERE window_start >= now() - INTERVAL ? HOUR"
	minsArgs := []any{windowHours}
	if pipeline != "" {
		minsWhere += " AND pipeline_id = ?"
		minsArgs = append(minsArgs, pipeline)
	}
	if modelID != "" {
		minsWhere += " AND model_id = ?"
		minsArgs = append(minsArgs, modelID)
	}

	minsRows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(window_start) AS ts,
			sum(total_minutes)          AS mins
		FROM naap.api_network_demand
		`+minsWhere+`
		GROUP BY ts
		ORDER BY ts ASC
	`, minsArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi mins: %w", err)
	}
	defer minsRows.Close()

	for minsRows.Next() {
		var ts time.Time
		var mins float64
		if err := minsRows.Scan(&ts, &mins); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard kpi mins scan: %w", err)
		}
		ts = ts.UTC().Truncate(time.Hour)
		if b, ok := bucketMap[ts]; ok {
			b.mins = mins
		} else {
			// Hour present in demand but not stream_hourly — add it
			nb := &kpiBucket{ts: ts, mins: mins}
			bucketMap[ts] = nb
			orderedTS = append(orderedTS, ts)
		}
	}
	if err := minsRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi mins rows: %w", err)
	}

	// Sort by timestamp
	sortTimeSlice(orderedTS)

	var totalSessions, totalSuccesses uint64
	var totalMins float64
	buckets := make([]kpiBucket, 0, len(orderedTS))
	hourlySessions := make([]types.DashboardHourlyBucket, 0, len(orderedTS))
	hourlyUsage := make([]types.DashboardHourlyBucket, 0, len(orderedTS))
	for _, ts := range orderedTS {
		b := bucketMap[ts]
		totalSessions += b.sessions
		totalSuccesses += b.successes
		totalMins += b.mins
		tsStr := ts.Format(time.RFC3339)
		hourlySessions = append(hourlySessions, types.DashboardHourlyBucket{Hour: tsStr, Value: float64(b.sessions)})
		hourlyUsage = append(hourlyUsage, types.DashboardHourlyBucket{Hour: tsStr, Value: b.mins})
		buckets = append(buckets, *b)
	}

	successRatePct := divSafe(float64(totalSuccesses), float64(totalSessions)) * 100
	sessionDelta, successRateDelta, minsDelta := computeKPIDeltas(buckets)

	return &types.DashboardKPI{
		SuccessRate: types.DashboardMetricDelta{
			Value: math.Round(successRatePct*10) / 10,
			Delta: successRateDelta,
		},
		OrchestratorsOnline: types.DashboardMetricDelta{
			Value: float64(activeOrch),
		},
		DailyUsageMins: types.DashboardMetricDelta{
			Value: math.Round(totalMins*10) / 10,
			Delta: minsDelta,
		},
		DailySessionCount: types.DashboardMetricDelta{
			Value: float64(totalSessions),
			Delta: sessionDelta,
		},
		DailyNetworkFeesEth: types.DashboardMetricDelta{}, // Phase 4: The Graph
		TimeframeHours:      windowHours,
		HourlySessions:      hourlySessions,
		HourlyUsage:         hourlyUsage,
	}, nil
}

// sortTimeSlice sorts a []time.Time in ascending order.
func sortTimeSlice(ts []time.Time) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
}

func computeKPIDeltas(buckets []kpiBucket) (sessionDelta, successRateDelta, minsDelta float64) {
	if len(buckets) < 2 {
		return 0, 0, 0
	}
	mid := len(buckets) / 2
	prev := buckets[:mid]
	curr := buckets[mid:]

	var prevSessions, currSessions, prevSuccesses, currSuccesses uint64
	var prevMins, currMins float64
	for _, b := range prev {
		prevSessions += b.sessions
		prevSuccesses += b.successes
		prevMins += b.mins
	}
	for _, b := range curr {
		currSessions += b.sessions
		currSuccesses += b.successes
		currMins += b.mins
	}

	if prevSessions > 0 {
		sessionDelta = math.Round((float64(currSessions)-float64(prevSessions))/float64(prevSessions)*1000) / 10
	}
	prevRate := divSafe(float64(prevSuccesses), float64(prevSessions))
	currRate := divSafe(float64(currSuccesses), float64(currSessions))
	successRateDelta = math.Round((currRate-prevRate)*1000) / 10

	if prevMins > 0 {
		minsDelta = math.Round((currMins-prevMins)/prevMins*1000) / 10
	}

	return sessionDelta, successRateDelta, minsDelta
}

// GetDashboardPipelines returns per-pipeline usage stats for the last 24 h (R16-2).
// Source: naap.api_network_demand (sessions, minutes, weighted avg FPS).
func (r *Repo) GetDashboardPipelines(ctx context.Context, limit int) ([]types.DashboardPipelineUsage, error) {
	if limit <= 0 {
		limit = 5
	}

	rows, err := r.conn.Query(ctx, `
		SELECT
			pipeline_id,
			sum(sessions_count)           AS sessions,
			sum(total_minutes)            AS total_mins,
			sum(output_fps_sum) / nullIf(sum(status_samples), 0) AS avg_fps
		FROM naap.api_network_demand
		WHERE window_start >= now() - INTERVAL 24 HOUR
		  AND pipeline_id != ''
		GROUP BY pipeline_id
		ORDER BY sessions DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipelines: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardPipelineUsage
	for rows.Next() {
		var pipeline string
		var sessions uint64
		var mins, avgFps float64
		if err := rows.Scan(&pipeline, &sessions, &mins, &avgFps); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pipelines scan: %w", err)
		}
		result = append(result, types.DashboardPipelineUsage{
			Name:     pipeline,
			Sessions: int64(sessions),
			Mins:     math.Round(mins*10) / 10,
			AvgFps:   math.Round(avgFps*100) / 100,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipelines rows: %w", err)
	}

	// Sort by sessions DESC and apply limit
	sort.Slice(result, func(i, j int) bool { return result[i].Sessions > result[j].Sessions })
	if len(result) > limit {
		result = result[:limit]
	}

	if result == nil {
		result = []types.DashboardPipelineUsage{}
	}
	return result, nil
}

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
	// Dashboard `slaScore` is the latest available contracted hourly score
	// within the requested window. It now comes from the precomputed final SLA
	// serving rows, while supporting counts and reliability ratios still
	// aggregate across the full dashboard window.
	type slaRow struct {
		address              string
		known                uint64
		successes            uint64
		slaScore             *float64
		slaWindowStart       *time.Time
		effectiveSuccessRate float64
		noSwapRate           float64
	}

	slaRows, err := r.conn.Query(ctx, `
		WITH windowed AS (
			SELECT
				orchestrator_address,
				window_start,
				known_sessions_count,
				startup_success_sessions,
				sla_score,
				requested_sessions,
				effective_success_rate,
				no_swap_rate,
				pipeline_id,
				ifNull(model_id, '') AS model_id,
				ifNull(gpu_id, '') AS gpu_id
			FROM naap.api_sla_compliance
			WHERE window_start >= now() - INTERVAL ? HOUR
			  AND orchestrator_address != ''
		),
		latest_hour AS (
			SELECT
				orchestrator_address,
				max(window_start) AS latest_window_start
			FROM windowed
			GROUP BY orchestrator_address
		)
		SELECT
			w.orchestrator_address,
			sum(w.known_sessions_count) AS known,
			sum(w.startup_success_sessions) AS successes,
			argMaxIf(
				w.sla_score,
				tuple(w.window_start, w.requested_sessions, w.pipeline_id, w.model_id, w.gpu_id),
				w.window_start = h.latest_window_start
			) AS sla_score,
			max(h.latest_window_start) AS sla_window_start,
			sum(w.effective_success_rate * w.requested_sessions)
				/ nullIf(sum(w.requested_sessions), 0) AS effective_success_rate,
			sum(w.no_swap_rate * w.requested_sessions)
				/ nullIf(sum(w.requested_sessions), 0) AS no_swap_rate
		FROM windowed w
		INNER JOIN latest_hour h
			ON w.orchestrator_address = h.orchestrator_address
		GROUP BY w.orchestrator_address
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
		if err := slaRows.Scan(&s.address, &s.known, &s.successes, &s.slaScore, &s.slaWindowStart, &s.effectiveSuccessRate, &s.noSwapRate); err != nil {
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
		modelMap    map[string]map[string]struct{} // pipelineId -> set of modelIds
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
		effectiveRate := s.effectiveSuccessRate
		noSwapRatio := s.noSwapRate

		// Clamp to [0, 1]
		if effectiveRate < 0 {
			effectiveRate = 0
		}
		if noSwapRatio < 0 {
			noSwapRatio = 0
		}
		if effectiveRate > 1 {
			effectiveRate = 1
		}
		if noSwapRatio > 1 {
			noSwapRatio = 1
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
		var slaWindowStart *string
		if s.slaWindowStart != nil {
			formatted := s.slaWindowStart.UTC().Format(time.RFC3339)
			slaWindowStart = &formatted
		}
		result = append(result, types.DashboardOrchestrator{
			Address:              s.address,
			EnsName:              state.name,
			ServiceURI:           state.uri,
			KnownSessions:        int64(s.known),
			SuccessSessions:      int64(s.successes),
			SuccessRatio:         successRatio,
			EffectiveSuccessRate: &effectiveRate,
			NoSwapRatio:          &noSwapRatio,
			SLAScore:             s.slaScore,
			SLAWindowStart:       slaWindowStart,
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
	// --- Total unique GPUs ---
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

	// --- Per pipeline/model/gpu_model breakdown ---
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

	// Accumulate into pipeline → model → gpu_model structure
	type pipelineModelKey struct{ pipeline, model string }
	type modelGPUEntry struct {
		gpuModel string
		count    uint64
	}

	// pipelineGPUs: pipeline → total GPU count
	// pipelineModelGPUs: pipeline → model → []gpu entries
	pipelineGPUs := map[string]uint64{}
	pipelineModelGPUs := map[pipelineModelKey][]modelGPUEntry{}

	// modelCounts: gpu_model_name → total count
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

	// Build pipelineGPUs output sorted by gpu count desc
	pipelineList := make([]types.DashboardGPUCapacityPipeline, 0, len(pipelineGPUs))
	for pipeline, totalCount := range pipelineGPUs {
		// Collect all (model, gpu_model, count) for this pipeline and build per-model totals
		modelTotals := map[string]int64{}
		for key, entries := range pipelineModelGPUs {
			if key.pipeline != pipeline {
				continue
			}
			for _, e := range entries {
				modelTotals[key.model+"::"+e.gpuModel] += int64(e.count)
			}
		}
		var modelEntries []types.DashboardGPUCapacityPipelineModel
		// Aggregate per model_id across GPU models
		perModel := map[string]int64{}
		for key, entries := range pipelineModelGPUs {
			if key.pipeline != pipeline {
				continue
			}
			for _, e := range entries {
				_ = e
				perModel[key.model] += int64(e.count)
			}
		}
		for model, cnt := range perModel {
			modelEntries = append(modelEntries, types.DashboardGPUCapacityPipelineModel{
				Model: model,
				GPUs:  cnt,
			})
		}
		sort.Slice(modelEntries, func(i, j int) bool { return modelEntries[i].GPUs > modelEntries[j].GPUs })
		_ = modelTotals

		pipelineList = append(pipelineList, types.DashboardGPUCapacityPipeline{
			Name:   pipeline,
			GPUs:   int64(totalCount),
			Models: modelEntries,
		})
	}
	sort.Slice(pipelineList, func(i, j int) bool { return pipelineList[i].GPUs > pipelineList[j].GPUs })

	// Build overall GPU model counts
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

// GetDashboardPipelineCatalog returns the set of pipeline+model combinations
// currently offered by at least one warm orchestrator (R16-5).
// Source: naap.api_latest_orchestrator_pipeline_models.
func (r *Repo) GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT pipeline_id, model_id
		FROM naap.api_latest_orchestrator_pipeline_models
		WHERE last_seen > now() - INTERVAL ? MINUTE
		  AND pipeline_id != ''
		  AND model_id    != ''
		ORDER BY pipeline_id, model_id
	`, activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog: %w", err)
	}
	defer rows.Close()

	catalogMap := map[string][]string{}
	for rows.Next() {
		var pipeline, model string
		if err := rows.Scan(&pipeline, &model); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog scan: %w", err)
		}
		catalogMap[pipeline] = append(catalogMap[pipeline], model)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog rows: %w", err)
	}

	result := make([]types.DashboardPipelineCatalogEntry, 0, len(catalogMap))
	for pipeline, models := range catalogMap {
		result = append(result, types.DashboardPipelineCatalogEntry{
			ID:      pipeline,
			Name:    pipeline, // display name mapping stays in the UI
			Models:  models,
			Regions: []string{},
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// GetDashboardPricing returns per-orchestrator pricing rows (R16-6).
// One row per (orch_address, pipeline, model_id) with real PixelsPerUnit.
// Source: naap.api_latest_orchestrator_state via parsePricingFromCapabilities.
func (r *Repo) GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error) {
	rows, err := r.conn.Query(ctx,
		"SELECT orch_address, name, raw_capabilities FROM naap.api_latest_orchestrator_state WHERE last_seen > now() - INTERVAL ? MINUTE",
		activeOrchMinutes)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing: %w", err)
	}
	defer rows.Close()

	var result []types.DashboardPipelinePricing
	for rows.Next() {
		var address, name, raw string
		if err := rows.Scan(&address, &name, &raw); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pricing scan: %w", err)
		}
		for _, e := range parsePricingFromCapabilities(address, name, raw) {
			result = append(result, types.DashboardPipelinePricing{
				OrchAddress:     address,
				OrchName:        name,
				Pipeline:        e.Pipeline,
				Model:           e.ModelID,
				PriceWeiPerUnit: e.PricePerUnit,
				PixelsPerUnit:   e.PixelsPerUnit,
				IsWarm:          e.IsWarm,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pricing rows: %w", err)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Pipeline != result[j].Pipeline {
			return result[i].Pipeline < result[j].Pipeline
		}
		if result[i].Model != result[j].Model {
			return result[i].Model < result[j].Model
		}
		return result[i].PriceWeiPerUnit < result[j].PriceWeiPerUnit
	})
	return result, nil
}

// GetDashboardJobFeed returns currently active streams for the live job feed (R16-7).
// Sources: naap.canonical_status_samples_recent_store, naap.api_latest_orchestrator_state.
//
// Reads the underlying store directly (bypassing the api_status_samples view) so the
// dedup CTE only processes events in the active window (~200 rows) instead of the full
// 5 M+ event history the view scans on every call.
func (r *Repo) GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error) {
	if limit <= 0 {
		limit = 50
	}

	// --- Active stream samples ---
	// Push the time filter into the dedup CTE so it deduplicates only the ~200
	// events in the active window rather than the full 5 M+ event history.
	rows, err := r.conn.Query(ctx, `
		WITH recent_events AS (
			SELECT event_id, argMax(refreshed_at, refreshed_at) AS refreshed_at
			FROM naap.canonical_status_samples_recent_store
			WHERE sample_ts > now() - INTERVAL ? SECOND
			GROUP BY event_id
		)
		SELECT
			s.canonical_session_key,
			s.stream_id,
			s.pipeline,
			coalesce(anyLast(s.model_id), '')     AS model,
			s.gateway,
			coalesce(anyLast(s.orch_address), '') AS orch_address,
			anyLast(s.state)                       AS state,
			round(avg(s.output_fps), 2)            AS avg_output_fps,
			round(avg(s.input_fps),  2)            AS avg_input_fps,
			min(s.sample_ts)                       AS first_seen,
			max(s.sample_ts)                       AS last_seen,
			toFloat64(dateDiff('second', min(s.sample_ts), max(s.sample_ts))) AS duration_secs
		FROM naap.canonical_status_samples_recent_store AS s
		INNER JOIN recent_events AS r ON s.event_id = r.event_id AND s.refreshed_at = r.refreshed_at
		WHERE s.sample_ts > now() - INTERVAL ? SECOND
		GROUP BY s.canonical_session_key, s.stream_id, s.pipeline, s.gateway
		ORDER BY last_seen DESC
		LIMIT ?
	`, activeStreamSecs, activeStreamSecs, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed: %w", err)
	}
	defer rows.Close()

	type rawItem struct {
		sessionKey  string
		streamID    string
		pipeline    string
		model       string
		gateway     string
		orchAddress string
		state       string
		outputFPS   float64
		inputFPS    float64
		firstSeen   time.Time
		lastSeen    time.Time
		durationSec float64
	}

	var items []rawItem
	orchAddrs := map[string]struct{}{}
	for rows.Next() {
		var it rawItem
		if err := rows.Scan(
			&it.sessionKey, &it.streamID, &it.pipeline, &it.model,
			&it.gateway, &it.orchAddress, &it.state,
			&it.outputFPS, &it.inputFPS,
			&it.firstSeen, &it.lastSeen, &it.durationSec,
		); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard job feed scan: %w", err)
		}
		items = append(items, it)
		if it.orchAddress != "" {
			orchAddrs[it.orchAddress] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed rows: %w", err)
	}

	// --- Orch URIs for orchestratorUrl field ---
	orchURIs := map[string]string{}
	if len(orchAddrs) > 0 {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(orchAddrs)), ",")
		args := make([]any, 0, len(orchAddrs))
		for addr := range orchAddrs {
			args = append(args, addr)
		}
		uriRows, err := r.conn.Query(ctx,
			`SELECT orch_address, uri FROM naap.api_latest_orchestrator_state WHERE orch_address IN (`+placeholders+`)`,
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard job feed uris: %w", err)
		}
		defer uriRows.Close()
		for uriRows.Next() {
			var addr, uri string
			if err := uriRows.Scan(&addr, &uri); err != nil {
				return nil, fmt.Errorf("clickhouse get dashboard job feed uris scan: %w", err)
			}
			orchURIs[addr] = uri
		}
		if err := uriRows.Err(); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard job feed uris rows: %w", err)
		}
	}

	result := make([]types.DashboardJobFeedItem, 0, len(items))
	for _, it := range items {
		dur := it.durationSec
		var durPtr *float64
		if dur > 0 {
			durPtr = &dur
		}
		result = append(result, types.DashboardJobFeedItem{
			ID:              it.sessionKey,
			Pipeline:        it.pipeline,
			Model:           it.model,
			Gateway:         hostnameFromURI("https://" + it.gateway),
			OrchestratorURL: orchURIs[it.orchAddress],
			State:           it.state,
			JobType:         "stream",
			InputFPS:        it.inputFPS,
			OutputFPS:       it.outputFPS,
			FirstSeen:       it.firstSeen.UTC().Format(time.RFC3339),
			LastSeen:        it.lastSeen.UTC().Format(time.RFC3339),
			DurationSeconds: durPtr,
		})
	}

	result, err = appendByocJobFeed(ctx, r, result, limit)
	if err != nil {
		return nil, err
	}
	result, err = appendAIBatchJobFeed(ctx, r, result, limit)
	if err != nil {
		return nil, err
	}

	sort.Slice(result, func(i, j int) bool { return result[i].LastSeen > result[j].LastSeen })
	if len(result) > limit {
		result = result[:limit]
	}

	if result == nil {
		result = []types.DashboardJobFeedItem{}
	}
	return result, nil
}

func appendByocJobFeed(ctx context.Context, r *Repo, result []types.DashboardJobFeedItem, limit int) ([]types.DashboardJobFeedItem, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			capability,
			ifNull(model, '') AS model,
			ifNull(gateway, '') AS gateway,
			ifNull(orch_url_norm, '') AS orch_url,
			ifNull(toString(success), '') AS state,
			submitted_at,
			completed_at,
			toFloat64(ifNull(duration_ms, 0)) / 1000.0 AS duration_secs
		FROM naap.canonical_byoc_jobs
		WHERE completed_at > now() - INTERVAL ? SECOND
		ORDER BY completed_at DESC
		LIMIT ?
	`, activeStreamSecs, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed byoc: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, pipeline, model, gateway, orchURL, state string
		var submittedAt, completedAt time.Time
		var durSecs float64
		if err := rows.Scan(&id, &pipeline, &model, &gateway, &orchURL, &state, &submittedAt, &completedAt, &durSecs); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard job feed byoc scan: %w", err)
		}
		var durPtr *float64
		if durSecs > 0 {
			durPtr = &durSecs
		}
		result = append(result, types.DashboardJobFeedItem{
			ID:              id,
			Pipeline:        pipeline,
			Model:           model,
			Gateway:         hostnameFromURI("https://" + gateway),
			OrchestratorURL: orchURL,
			State:           state,
			JobType:         "byoc",
			FirstSeen:       submittedAt.UTC().Format(time.RFC3339),
			LastSeen:        completedAt.UTC().Format(time.RFC3339),
			DurationSeconds: durPtr,
		})
	}
	return result, rows.Err()
}

func appendAIBatchJobFeed(ctx context.Context, r *Repo, result []types.DashboardJobFeedItem, limit int) ([]types.DashboardJobFeedItem, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
			request_id,
			ifNull(pipeline, '') AS pipeline,
			ifNull(model_id, '') AS model,
			ifNull(gateway, '') AS gateway,
			ifNull(orch_url_norm, '') AS orch_url,
			ifNull(toString(success), '') AS state,
			received_at,
			completed_at,
			toFloat64(ifNull(duration_ms, 0)) / 1000.0 AS duration_secs
		FROM naap.canonical_ai_batch_jobs
		WHERE completed_at > now() - INTERVAL ? SECOND
		ORDER BY completed_at DESC
		LIMIT ?
	`, activeStreamSecs, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed ai_batch: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, pipeline, model, gateway, orchURL, state string
		var receivedAt, completedAt time.Time
		var durSecs float64
		if err := rows.Scan(&id, &pipeline, &model, &gateway, &orchURL, &state, &receivedAt, &completedAt, &durSecs); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard job feed ai_batch scan: %w", err)
		}
		var durPtr *float64
		if durSecs > 0 {
			durPtr = &durSecs
		}
		result = append(result, types.DashboardJobFeedItem{
			ID:              id,
			Pipeline:        pipeline,
			Model:           model,
			Gateway:         hostnameFromURI("https://" + gateway),
			OrchestratorURL: orchURL,
			State:           state,
			JobType:         "ai-batch",
			FirstSeen:       receivedAt.UTC().Format(time.RFC3339),
			LastSeen:        completedAt.UTC().Format(time.RFC3339),
			DurationSeconds: durPtr,
		})
	}
	return result, rows.Err()
}
