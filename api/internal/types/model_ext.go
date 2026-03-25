package types

// OrchModelStats is one orch row within a model detail response.
type OrchModelStats struct {
	Address string
	Name    string
	AvgFPS  float64
	IsWarm  bool
}

// ModelPerformance is one row for GET /v1/perf/by-model (MPERF-001).
type ModelPerformance struct {
	ModelID       string
	Pipeline      string
	AvgFPS        float64
	P50FPS        float64
	P99FPS        float64
	WarmOrchCount int64
	TotalStreams   int64
}

// ModelDetail is the response for GET /v1/net/model (MPERF-002).
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
