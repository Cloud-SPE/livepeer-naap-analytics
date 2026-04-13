package types

import "time"

// JobsParams holds validated query parameters for /v1/requests/* endpoints.
type JobsParams struct {
	// OrchestratorURI applies only to /v1/requests/sla. Non-streaming jobs are
	// keyed by service URI, not by on-chain address.
	OrchestratorURI string
	Gateway         string
	PipelineID      string
	ModelID         string
	JobType         string // optional filter: "ai-batch" or "byoc"
	Org             string
	Start           time.Time
	End             time.Time
	Limit           int
	Cursor          string
}

// JobsDemandRow is one row in the /v1/requests/demand response.
type JobsDemandRow struct {
	WindowStart   time.Time `json:"window_start"`
	Org           *string   `json:"org,omitempty"`
	Gateway       string    `json:"gateway"`
	PipelineID    string    `json:"pipeline_id"`
	ModelID       *string   `json:"model_id"`
	JobType       string    `json:"job_type"`
	JobCount      uint64    `json:"job_count"`
	SuccessCount  uint64    `json:"success_count"`
	SuccessRate   float64   `json:"success_rate"`
	AvgDurationMs float64   `json:"avg_duration_ms"`
	TotalMinutes  float64   `json:"total_minutes"`
}

// JobsSLARow is one row in the /v1/requests/sla response.
type JobsSLARow struct {
	WindowStart     time.Time `json:"window_start"`
	Org             *string   `json:"org,omitempty"`
	OrchestratorURI string    `json:"orchestrator_uri"`
	PipelineID      string    `json:"pipeline_id"`
	ModelID         *string   `json:"model_id"`
	GPUID           *string   `json:"gpu_id"`
	JobType         string    `json:"job_type"`
	JobCount        uint64    `json:"job_count"`
	SuccessCount    uint64    `json:"success_count"`
	SuccessRate     float64   `json:"success_rate"`
	AvgDurationMs   float64   `json:"avg_duration_ms"`
	SLAScore        *float64  `json:"sla_score"`
}
