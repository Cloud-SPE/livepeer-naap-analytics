package types

// OrchModelStats is one orch row within a model detail response.
type OrchModelStats struct {
	Address string
	Name    string
	AvgFPS  float64
	IsWarm  bool
}

// ModelPerformance is one row for active requirement PERF-001 served by
// GET /v1/perf/by-model.
type ModelPerformance struct {
	ModelID       string  `json:"ModelID"`
	Pipeline      string  `json:"Pipeline"`
	AvgFPS        float64 `json:"AvgFPS"`
	P50FPS        float64 `json:"P50FPS"`
	P99FPS        float64 `json:"P99FPS"`
	WarmOrchCount int64   `json:"WarmOrchCount"`
	TotalStreams  int64   `json:"TotalStreams"`
}

// ModelDetail is a lower-layer companion shape for model-performance exploration.
// It is not a current routed API response.
type ModelDetail struct {
	ModelID       string
	Pipeline      string
	AvgFPS        float64
	P50FPS        float64
	P99FPS        float64
	WarmOrchCount int64
	TotalStreams  int64
	Orchs         []OrchModelStats
}

// JobModelPerformance is one row for GET /v1/jobs/by-model.
// Request/response job types only (ai-batch, byoc) — duration-based metrics.
type JobModelPerformance struct {
	ModelID       string   `json:"model_id"`
	Pipeline      string   `json:"pipeline"`
	JobType       string   `json:"job_type"`
	JobCount      int64    `json:"job_count"`
	WarmOrchCount int64    `json:"warm_orch_count"`
	AvgDurationMs *float64 `json:"avg_duration_ms,omitempty"`
	P50DurationMs *float64 `json:"p50_duration_ms,omitempty"`
	P99DurationMs *float64 `json:"p99_duration_ms,omitempty"`
}
