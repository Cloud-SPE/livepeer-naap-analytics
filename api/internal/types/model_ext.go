package types

// OrchModelStats is one orch row within a model detail response.
type OrchModelStats struct {
	Address string
	Name    string
	AvgFPS  float64
	IsWarm  bool
}

// ModelPerformance is one row for GET /v1/perf/by-model (MPERF-001). FPS performance broken down by AI model.
type ModelPerformance struct {
	ModelID       string
	Pipeline      string
	AvgFPS        float64
	P50FPS        float64
	P99FPS        float64
	WarmOrchCount int64
	TotalStreams   int64
}

// ModelDetail holds detail for one model including per-orchestrator stats (MPERF-002).
type ModelDetail struct {
	ModelID       string
	Pipeline      string
	AvgFPS        float64
	P50FPS        float64
	P99FPS        float64
	WarmOrchCount int64
	TotalStreams   int64
	Orchs         []OrchModelStats
}
