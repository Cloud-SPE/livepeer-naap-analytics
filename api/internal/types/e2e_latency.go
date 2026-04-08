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

// E2ELatencySummary holds E2E latency p50/p95/p99 by pipeline and orchestrator (E2E-001).
type E2ELatencySummary struct {
	StartTime      time.Time
	EndTime        time.Time
	Overall        E2ELatencyStats
	ByPipeline     []PipelineE2ELatency
	ByOrchestrator []OrchE2ELatency
}

// E2ELatencyBucket is one hourly bucket in the E2E latency time-series (E2E-002).
type E2ELatencyBucket struct {
	Timestamp   time.Time
	AvgMS       float64
	P50MS       float64
	P95MS       float64
	SampleCount int64
}
