package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetRequestsModels serves GET /v1/requests/models.
// Non-streaming capacity and 24h stats come from the capability-aware offer
// and request-demand surfaces. The public job_type field is derived here from
// capability_family rather than being treated as a base storage dimension.
func (r *Repo) GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error) {
	rows, err := r.conn.Query(ctx, `
		WITH offers AS (
		    SELECT
		        if(capability_family = 'byoc', offered_name, ifNull(canonical_pipeline, capability_name)) AS pipeline,
		        model_id AS model,
		        capability_family,
		        toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		        toInt64(count(DISTINCT nullIf(ifNull(gpu_id, ''), ''))) AS gpu_slots
		    FROM naap.api_current_capability_offer
		    WHERE supports_request = 1
		      AND model_id IS NOT NULL AND model_id != ''
		      AND ifNull(canonical_pipeline, '') != 'live-video-to-video'
		      AND if(capability_family = 'byoc', offered_name, ifNull(canonical_pipeline, capability_name)) != ''
		      AND last_seen > now() - INTERVAL 30 MINUTE
		    GROUP BY pipeline, model, capability_family
		),
		demand AS (
		    SELECT
		        if(capability_family = 'byoc', capability_name, ifNull(canonical_pipeline, capability_name)) AS pipeline,
		        canonical_model AS model,
		        capability_family,
		        toInt64(sum(job_count)) AS job_count_24h,
		        sum(success_count) / nullIf(sum(job_count), 0) AS success_rate,
		        sum(duration_ms_sum) / nullIf(sum(job_count), 0) AS avg_duration_ms
		    FROM naap.api_hourly_request_demand
		    WHERE window_start >= now() - INTERVAL 24 HOUR
		    GROUP BY pipeline, model, capability_family
		)
		SELECT
		    o.pipeline,
		    o.model,
		    if(o.capability_family = 'byoc', 'byoc', 'ai-batch') AS job_type,
		    o.warm_orch_count,
		    o.gpu_slots,
		    ifNull(d.job_count_24h, 0) AS job_count_24h,
		    round(ifNull(d.success_rate, 0), 3) AS success_rate,
		    round(ifNull(d.avg_duration_ms, 0), 0) AS avg_duration_ms
		FROM offers o
		LEFT JOIN demand d
		  ON d.pipeline = o.pipeline
		 AND ifNull(d.model, '') = ifNull(o.model, '')
		 AND d.capability_family = o.capability_family
		ORDER BY job_count_24h DESC, warm_orch_count DESC, pipeline ASC, model ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("requests models: %w", err)
	}
	defer rows.Close()

	var result []types.RequestsModel
	for rows.Next() {
		var m types.RequestsModel
		if err := rows.Scan(&m.Pipeline, &m.Model, &m.JobType,
			&m.WarmOrchCount, &m.GPUSlots,
			&m.JobCount24h, &m.SuccessRate, &m.AvgDurationMs); err != nil {
			return nil, fmt.Errorf("requests models scan: %w", err)
		}
		result = append(result, m)
	}
	if result == nil {
		result = []types.RequestsModel{}
	}
	return result, nil
}
