package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// DiscoverOrchestrators serves GET /v1/discover/orchestrators.
//
// Emits one row per (orchestrator, pipeline) — an orch advertising N pipelines
// produces N rows that share the same `address` and timestamps but each carry
// their own per-pipeline `score` and `capabilities` list. Score uses the SLA
// source matching the row's pipeline (last 2-hour window):
//   - live-video-to-video → api_sla_compliance.sla_score / 100
//   - any other non-BYOC pipeline → api_requests_sla.sla_score / 100
//
// When there's no recent SLA activity for an (orch, pipeline), score is set to
// 0.0 and `recent_work` is false (signaling the score is a placeholder, not a
// measured value). BYOC capabilities (pipeline_id LIKE 'openai-%') are
// excluded entirely.
//
// `caps` filter (OR semantics): when non-empty, only emit rows whose pipeline
// has at least one matching `pipeline/model` in the caps list. The returned
// `capabilities` array still includes all of the orch's models for that pipeline.
func (r *Repo) DiscoverOrchestrators(ctx context.Context, p types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	capsFilter := ""
	args := []any{}
	if len(p.Caps) > 0 {
		// Pipelines mentioned in any cap → restrict the (orch, pipeline) rows we emit
		// to those pipelines, then within each row include only orchs that advertise
		// at least one matching cap. Two-stage filter via subquery on pipeline_models.
		capsFilter = `
		  AND (p.pipeline_id, o.orch_address) IN (
		    SELECT pipeline_id, orch_address
		    FROM naap.api_latest_orchestrator_pipeline_models
		    WHERE last_seen > now() - INTERVAL 30 MINUTE
		      AND pipeline_id != ''
		      AND model_id != ''
		      AND (pipeline_id || '/' || model_id) IN (?)
		  )
		`
		args = append(args, p.Caps)
	}

	query := `
		WITH
		  -- Streaming SLA per (orch_address, pipeline_id), last 2 hours
		  streaming_sla AS (
		    SELECT
		      orchestrator_address AS orch_address,
		      pipeline_id,
		      sum(ifNull(sla_score, 0) * requested_sessions)
		        / nullIf(sum(requested_sessions), 0) / 100.0 AS score
		    FROM naap.api_sla_compliance
		    WHERE window_start >= now() - INTERVAL 2 HOUR
		      AND orchestrator_address != ''
		      AND pipeline_id = 'live-video-to-video'
		    GROUP BY orchestrator_address, pipeline_id
		  ),
		  -- AI-batch SLA per (orch_uri, pipeline_id), last 2 hours. Note URI not address.
		  ai_batch_sla AS (
		    SELECT
		      orchestrator_uri AS orch_uri,
		      pipeline_id,
		      sum(ifNull(sla_score, 0) * job_count)
		        / nullIf(sum(job_count), 0) / 100.0 AS score
		    FROM naap.api_requests_sla
		    WHERE window_start >= now() - INTERVAL 2 HOUR
		      AND orchestrator_uri != ''
		      AND job_type != 'byoc'
		    GROUP BY orchestrator_uri, pipeline_id
		  )
		SELECT
		  anyLast(o.uri) AS address,
		  round(coalesce(s.score, a.score, 0.0), 3) AS score,
		  arrayDistinct(groupArray(p.pipeline_id || '/' || p.model_id)) AS capabilities,
		  toInt64(toUnixTimestamp64Milli(max(o.last_seen))) AS last_seen_ms,
		  max(o.last_seen) AS last_seen,
		  (s.score IS NOT NULL OR a.score IS NOT NULL) AS recent_work
		FROM naap.api_latest_orchestrator_state o
		INNER JOIN naap.api_latest_orchestrator_pipeline_models p
		  ON o.orch_address = p.orch_address
		LEFT JOIN streaming_sla s
		  ON s.orch_address = o.orch_address AND s.pipeline_id = p.pipeline_id
		LEFT JOIN ai_batch_sla a
		  ON a.orch_uri = o.uri AND a.pipeline_id = p.pipeline_id
		WHERE o.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.last_seen > now() - INTERVAL 30 MINUTE
		  AND p.pipeline_id != ''
		  AND p.model_id != ''
		  AND p.pipeline_id NOT LIKE 'openai-%'
		  ` + capsFilter + `
		GROUP BY o.orch_address, p.pipeline_id, s.score, a.score
		ORDER BY score DESC, length(capabilities) DESC, address ASC
	`

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("discover orchestrators: %w", err)
	}
	defer rows.Close()

	var result []types.DiscoverOrchestratorRow
	for rows.Next() {
		var row types.DiscoverOrchestratorRow
		var lastSeen time.Time
		var recentWork uint8
		if err := rows.Scan(&row.Address, &row.Score, &row.Capabilities, &row.LastSeenMs, &lastSeen, &recentWork); err != nil {
			return nil, fmt.Errorf("discover orchestrators scan: %w", err)
		}
		row.LastSeen = lastSeen.UTC().Format("2006-01-02T15:04:05.000Z")
		row.RecentWork = recentWork == 1
		if row.Capabilities == nil {
			row.Capabilities = []string{}
		}
		result = append(result, row)
	}
	if result == nil {
		result = []types.DiscoverOrchestratorRow{}
	}
	return result, nil
}
