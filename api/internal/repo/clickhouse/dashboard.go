package clickhouse

import (
	"context"
	"fmt"
	"math"
	"sort"
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
func (r *Repo) GetDashboardKPI(ctx context.Context, windowHours int) (*types.DashboardKPI, error) {
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
	streamRows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(hour)           AS ts,
			sum(requested_sessions)       AS sessions,
			sum(startup_success_sessions) AS successes
		FROM naap.api_stream_hourly
		WHERE hour >= now() - INTERVAL ? HOUR
		GROUP BY ts
		ORDER BY ts ASC
	`, windowHours)
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
	minsRows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(window_start) AS ts,
			sum(total_minutes)          AS mins
		FROM naap.api_network_demand
		WHERE window_start >= now() - INTERVAL ? HOUR
		GROUP BY ts
		ORDER BY ts ASC
	`, windowHours)
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

	// Query 3: non-streaming job counts and minutes from api_unified_demand
	unifiedRows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfHour(window_start) AS ts,
			sum(job_count)              AS jobs,
			sum(success_count)          AS successes,
			sum(total_minutes)          AS mins
		FROM naap.api_unified_demand
		WHERE window_start >= now() - INTERVAL ? HOUR
		GROUP BY ts
		ORDER BY ts ASC
	`, windowHours)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi unified: %w", err)
	}
	defer unifiedRows.Close()

	for unifiedRows.Next() {
		var ts time.Time
		var jobs, successes uint64
		var mins float64
		if err := unifiedRows.Scan(&ts, &jobs, &successes, &mins); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard kpi unified scan: %w", err)
		}
		ts = ts.UTC().Truncate(time.Hour)
		if b, ok := bucketMap[ts]; ok {
			b.sessions += jobs
			b.successes += successes
			b.mins += mins
		} else {
			nb := &kpiBucket{ts: ts, sessions: jobs, successes: successes, mins: mins}
			bucketMap[ts] = nb
			orderedTS = append(orderedTS, ts)
		}
	}
	if err := unifiedRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard kpi unified rows: %w", err)
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
			sum(avg_output_fps * sessions_count) / nullIf(sum(sessions_count), 0) AS avg_fps
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

	// Second query: non-streaming pipelines from api_unified_demand (last 24h)
	uRows, err := r.conn.Query(ctx, `
		SELECT
			pipeline_id,
			job_type,
			sum(job_count)    AS jobs,
			sum(total_minutes) AS total_mins
		FROM naap.api_unified_demand
		WHERE window_start >= now() - INTERVAL 24 HOUR
		  AND pipeline_id != ''
		GROUP BY pipeline_id, job_type
		ORDER BY jobs DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipelines unified: %w", err)
	}
	defer uRows.Close()

	// Build a map keyed by pipeline+job_type to merge or append
	type pipelineJobKey struct{ pipeline, jobType string }
	pipelineMap := map[pipelineJobKey]*types.DashboardPipelineUsage{}
	for _, entry := range result {
		key := pipelineJobKey{entry.Name, "stream"}
		pipelineMap[key] = &result[len(result)-1]
	}
	// Rebuild pipelineMap properly
	pipelineMap = map[pipelineJobKey]*types.DashboardPipelineUsage{}
	resultPtrs := make([]*types.DashboardPipelineUsage, len(result))
	for i := range result {
		resultPtrs[i] = &result[i]
		pipelineMap[pipelineJobKey{result[i].Name, "stream"}] = resultPtrs[i]
	}

	var nonStreamEntries []types.DashboardPipelineUsage
	for uRows.Next() {
		var pipeline, jobType string
		var jobs uint64
		var mins float64
		if err := uRows.Scan(&pipeline, &jobType, &jobs, &mins); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pipelines unified scan: %w", err)
		}
		nonStreamEntries = append(nonStreamEntries, types.DashboardPipelineUsage{
			Name:     pipeline,
			Sessions: int64(jobs),
			Mins:     math.Round(mins*10) / 10,
			AvgFps:   0,
			JobType:  jobType,
		})
	}
	if err := uRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipelines unified rows: %w", err)
	}
	result = append(result, nonStreamEntries...)

	// Sort all by sessions DESC and apply limit
	sort.Slice(result, func(i, j int) bool { return result[i].Sessions > result[j].Sessions })
	if len(result) > limit {
		result = result[:limit]
	}

	if result == nil {
		result = []types.DashboardPipelineUsage{}
	}
	return result, nil
}

// GetDashboardOrchestrators, GetDashboardGPUCapacity are implemented in dashboard_orch.go.
// GetDashboardPipelineCatalog, GetDashboardPricing, GetDashboardJobFeed are in dashboard_ext.go.
