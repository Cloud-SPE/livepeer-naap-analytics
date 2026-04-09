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
	ModelID       string
	Pipeline      string
	AvgFPS        float64
	P50FPS        float64
	P99FPS        float64
	WarmOrchCount int64
	TotalStreams   int64
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
	TotalStreams   int64
	Orchs         []OrchModelStats
}
