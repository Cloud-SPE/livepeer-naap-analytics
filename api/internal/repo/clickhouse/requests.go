package clickhouse

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/types"
)

// GetRequestsModels serves GET /v1/requests/models.
// Non-streaming pipelines: capacity from pipeline models + 24h job stats from
// api_requests_sla.
func (r *Repo) GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT s.pipeline_id AS pipeline, s.model_id AS model,
		    ifNull(t.job_type, 'unknown') AS job_type,
		    s.warm_orch_count, s.gpu_slots,
		    ifNull(t.job_count_24h, 0) AS job_count_24h,
		    round(ifNull(t.success_rate, 0), 3) AS success_rate,
		    round(ifNull(t.avg_duration_ms, 0), 0) AS avg_duration_ms
		FROM (
		    SELECT pipeline_id, model_id,
		           toInt64(count(DISTINCT orch_address)) AS warm_orch_count,
		           toInt64(count(DISTINCT nullIf(ifNull(gpu_id, ''), ''))) AS gpu_slots
		    FROM naap.api_latest_orchestrator_pipeline_models
		    WHERE pipeline_id != 'live-video-to-video'
		      AND last_seen > now() - INTERVAL 30 MINUTE AND pipeline_id != '' AND model_id != ''
		    GROUP BY pipeline_id, model_id
		) s
		LEFT JOIN (
		    SELECT pipeline_id, model_id, job_type,
		           toInt64(sum(job_count)) AS job_count_24h,
		           sum(success_count) / nullIf(sum(job_count), 0) AS success_rate,
		           sum(avg_duration_ms * job_count) / nullIf(sum(job_count), 0) AS avg_duration_ms
		    FROM naap.api_requests_sla
		    WHERE window_start >= now() - INTERVAL 24 HOUR
		    GROUP BY pipeline_id, model_id, job_type
		) t ON s.pipeline_id = t.pipeline_id AND s.model_id = t.model_id
		ORDER BY job_count_24h DESC
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
