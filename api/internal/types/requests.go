package types

// RequestsModel — one row from GET /v1/requests/models.
//
// Per-model capacity and load view for non-streaming pipelines (AI Batch).
type RequestsModel struct {
	Pipeline      string  `json:"pipeline"`
	Model         string  `json:"model"`
	JobType       string  `json:"job_type"`
	WarmOrchCount int64   `json:"warm_orch_count"`
	GPUSlots      int64   `json:"gpu_slots"`
	JobCount24h   int64   `json:"job_count_24h"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}
