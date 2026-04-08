package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetDashboardPipelineCatalog returns the set of pipeline+model combinations
// currently offered by at least one warm orchestrator (R16-5), merged with
// non-streaming jobs from the last 7 days.
// Sources: naap.api_latest_orchestrator_pipeline_models, naap.api_unified_demand.
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

	// Second query: non-streaming (pipeline_id, model_id) pairs from api_unified_demand (last 7 days)
	uCatalogRows, err := r.conn.Query(ctx, `
		SELECT DISTINCT pipeline_id, model_id
		FROM naap.api_unified_demand
		WHERE window_start >= now() - INTERVAL 168 HOUR
		  AND pipeline_id != ''
		  AND model_id    != ''
		ORDER BY pipeline_id, model_id
	`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog unified: %w", err)
	}
	defer uCatalogRows.Close()

	for uCatalogRows.Next() {
		var pipeline, model string
		if err := uCatalogRows.Scan(&pipeline, &model); err != nil {
			return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog unified scan: %w", err)
		}
		existing := catalogMap[pipeline]
		found := false
		for _, m := range existing {
			if m == model {
				found = true
				break
			}
		}
		if !found {
			catalogMap[pipeline] = append(existing, model)
		}
	}
	if err := uCatalogRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard pipeline catalog unified rows: %w", err)
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

// GetDashboardPricing is implemented in dashboard_pricing.go.

// GetDashboardJobFeed returns recently active jobs for the live job feed (R16-7).
// Merges streaming sessions, BYOC jobs, and AI-batch jobs.
// Sources: naap.api_status_samples, naap.canonical_byoc_jobs, naap.canonical_ai_batch_jobs.
func (r *Repo) GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error) {
	if limit <= 0 {
		limit = 50
	}

	// --- Active stream samples ---
	streamRows, err := r.conn.Query(ctx, `
		SELECT
			canonical_session_key,
			stream_id,
			pipeline,
			coalesce(anyLast(model_id), '')     AS model,
			gateway,
			coalesce(anyLast(orch_address), '') AS orch_address,
			anyLast(state)                       AS state,
			round(avg(output_fps), 2)            AS avg_output_fps,
			round(avg(input_fps),  2)            AS avg_input_fps,
			min(sample_ts)                       AS first_seen,
			max(sample_ts)                       AS last_seen,
			toFloat64(dateDiff('second', min(sample_ts), max(sample_ts))) AS duration_secs
		FROM naap.api_status_samples
		WHERE sample_ts > now() - INTERVAL ? SECOND
		GROUP BY canonical_session_key, stream_id, pipeline, gateway
		ORDER BY last_seen DESC
		LIMIT ?
	`, activeStreamSecs, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed: %w", err)
	}
	defer streamRows.Close()

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
	for streamRows.Next() {
		var it rawItem
		if err := streamRows.Scan(
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
	if err := streamRows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse get dashboard job feed rows: %w", err)
	}

	// --- Orch URIs for orchestratorUrl field ---
	orchURIs := map[string]string{}
	if len(orchAddrs) > 0 {
		uriRows, err := r.conn.Query(ctx, `
			SELECT orch_address, uri FROM naap.api_latest_orchestrator_state
		`)
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
			ifNull(model, '')         AS model,
			ifNull(gateway, '')       AS gateway,
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
			ifNull(pipeline, '')      AS pipeline,
			ifNull(model_id, '')      AS model,
			ifNull(gateway, '')       AS gateway,
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
