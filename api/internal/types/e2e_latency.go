package types

import "time"

// E2ELatencyStats holds end-to-end latency distribution statistics.
type E2ELatencyStats struct {
	AvgMS       float64
	P50MS       float64
	P95MS       float64
	P99MS       float64
	SampleCount int64
}

// PipelineE2ELatency holds E2E latency for one pipeline (E2E-001).
type PipelineE2ELatency struct {
	Pipeline string
	E2ELatencyStats
}

// OrchE2ELatency holds E2E latency attributed to one orchestrator (E2E-001).
type OrchE2ELatency struct {
	Address  string
	Pipeline string
	E2ELatencyStats
}

// E2ELatencySummary is the response for GET /v1/perf/e2e-latency (E2E-001).
type E2ELatencySummary struct {
	StartTime      time.Time
	EndTime        time.Time
	Overall        E2ELatencyStats
	ByPipeline     []PipelineE2ELatency
	ByOrchestrator []OrchE2ELatency
}

// E2ELatencyBucket is one hourly bucket for GET /v1/perf/e2e-latency/history (E2E-002).
type E2ELatencyBucket struct {
	Timestamp   time.Time
	AvgMS       float64
	P50MS       float64
	P95MS       float64
	SampleCount int64
}
