package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// DiscoverOrchestrators serves GET /v1/discover/orchestrators.
//
// Emits one row per (orchestrator, pipeline) across all capability families —
// builtin streaming (e.g. live-video-to-video), builtin request (e.g. llm,
// text-to-image), and BYOC (e.g. openai-chat-completions). Routing to the SLA
// source is catalog-driven rather than pipeline-name driven:
//
//   - supports_stream = 1 → naap.api_hourly_streaming_sla (sla_score/100)
//   - otherwise           → naap.api_hourly_request_demand (success/job ratio)
//
// When no SLA/demand samples exist in the last 2 hours for that (orch, pipeline),
// score is 0.0 and recent_work is false (placeholder). When samples exist,
// recent_work is true and the score is a measured value.
//
// Pipeline key resolution:
//   - builtin capabilities use canonical_pipeline
//   - BYOC capabilities use capability_name verbatim (no openai-* literal)
//
// BYOC offers are drawn from two sources UNION'd together — observed capability
// offers and observed BYOC worker registrations — so workers that registered
// without a matching capability offer still surface.
//
// The optional caps filter uses OR semantics: a row is emitted whenever its
// pipeline has at least one matching pipeline/model in the caps list. The
// returned capabilities array still includes every pipeline/model the orch
// advertises for that pipeline.
func (r *Repo) DiscoverOrchestrators(ctx context.Context, p types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	capsFilter := ""
	end := time.Now().UTC()
	start := end.Add(-observedInventoryHours * time.Hour)
	args := []any{start, end, start, end}
	if len(p.Caps) > 0 {
		capsFilter = `
		  AND (p.pipeline_id, p.orch_address) IN (
		    SELECT pipeline_id, orch_address FROM offers
		    WHERE (pipeline_id || '/' || ifNull(model_id, '')) IN (?)
		  )
		`
		args = append(args, p.Caps)
	}

	query := `
		WITH
		  offers AS (
		    SELECT
		      orch_address,
		      if(capability_family = 'byoc', capability_name, ifNull(canonical_pipeline, capability_name)) AS pipeline_id,
		      model_id,
		      last_seen,
		      capability_family,
		      supports_stream
		    FROM naap.api_observed_capability_offer
		    WHERE last_seen >= ? AND last_seen < ?
		      AND if(capability_family = 'byoc', capability_name, ifNull(canonical_pipeline, capability_name)) != ''
		    UNION ALL
		    SELECT
		      orch_address,
		      capability_name AS pipeline_id,
		      model AS model_id,
		      last_seen,
		      'byoc' AS capability_family,
		      toUInt8(0) AS supports_stream
		    FROM naap.api_observed_byoc_worker
		    WHERE last_seen >= ? AND last_seen < ?
		      AND capability_name != ''
		  ),
		  observed_orchestrators AS (
		    SELECT
		      o.orch_address,
		      ifNull(i.orchestrator_uri, '') AS orchestrator_uri,
		      max(o.last_seen) AS last_seen
		    FROM offers o
		    LEFT JOIN naap.api_orchestrator_identity i
		      ON i.orch_address = o.orch_address
		    GROUP BY o.orch_address, i.orchestrator_uri
		  ),
		  streaming_sla AS (
		    SELECT
		      orchestrator_address AS orch_address,
		      pipeline_id,
		      sum(ifNull(sla_score, 0) * requested_sessions)
		        / nullIf(sum(requested_sessions), 0) / 100.0 AS score
		    FROM naap.api_hourly_streaming_sla
		    WHERE window_start >= now() - INTERVAL 2 HOUR
		      AND orchestrator_address != ''
		    GROUP BY orchestrator_address, pipeline_id
		  ),
		  request_sla AS (
		    SELECT
		      orchestrator_uri AS orch_uri,
		      if(capability_family = 'byoc', capability_name, ifNull(canonical_pipeline, capability_name)) AS pipeline_id,
		      sum(success_count) / nullIf(toFloat64(sum(job_count)), 0.0) AS score
		    FROM naap.api_hourly_request_demand
		    WHERE window_start >= now() - INTERVAL 2 HOUR
		      AND orchestrator_uri != ''
		      AND execution_mode = 'request'
		    GROUP BY orchestrator_uri, pipeline_id
		  )
		SELECT
		  anyLast(o.orchestrator_uri) AS address,
		  round(coalesce(s.score, a.score, 0.0), 3) AS score,
		  arrayDistinct(groupArrayIf(
		      p.pipeline_id || '/' || ifNull(p.model_id, ''),
		      ifNull(p.model_id, '') != ''
		  )) AS capabilities,
		  toInt64(toUnixTimestamp64Milli(max(o.last_seen))) AS last_seen_ms,
		  max(o.last_seen) AS last_seen,
		  (s.score IS NOT NULL OR a.score IS NOT NULL) AS recent_work
		FROM observed_orchestrators o
		INNER JOIN offers p
		  ON o.orch_address = p.orch_address
		LEFT JOIN streaming_sla s
		  ON p.supports_stream = 1
		 AND s.orch_address = o.orch_address
		 AND s.pipeline_id = p.pipeline_id
		LEFT JOIN request_sla a
		  ON p.supports_stream = 0
		 AND a.orch_uri = o.orchestrator_uri
		 AND a.pipeline_id = p.pipeline_id
		WHERE p.pipeline_id != ''
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
