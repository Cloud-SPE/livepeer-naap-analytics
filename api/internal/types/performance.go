package types

import "time"

// FPSStats holds FPS distribution statistics.
type FPSStats struct {
	Avg float64
	P5  float64
	P50 float64
	P99 float64
}

// PipelineFPS aggregates FPS metrics for one pipeline (PERF-001).
type PipelineFPS struct {
	Pipeline     string
	InferenceFPS FPSStats
	InputFPS     FPSStats
	SampleCount  int64
}

// OrchFPS aggregates FPS metrics for one orch+pipeline pair (PERF-001).
type OrchFPS struct {
	Address      string
	Name         string
	Pipeline     string
	InferenceFPS FPSStats
	InputFPS     FPSStats
	SampleCount  int64
}

// FPSSummary is the response payload for GET /v1/performance/fps (PERF-001).
type FPSSummary struct {
	StartTime      time.Time
	EndTime        time.Time
	ByPipeline     []PipelineFPS
	ByOrchestrator []OrchFPS
}

// FPSBucket is one time-bucket for GET /v1/performance/fps/history (PERF-002).
// Degraded is true when avg inference FPS drops ≥20% from the prior bucket.
type FPSBucket struct {
	Timestamp       time.Time
	AvgInferenceFPS float64
	AvgInputFPS     float64
	SampleCount     int64
	Degraded        bool
}

// OrchLatency holds discovery latency stats for one orchestrator (PERF-003).
type OrchLatency struct {
	Address     string
	Name        string
	AvgMS       float64
	P50MS       float64
	P95MS       float64
	P99MS       float64
	SampleCount int64
}

// LatencySummary is the response payload for GET /v1/performance/latency (PERF-003).
type LatencySummary struct {
	ByOrchestrator []OrchLatency
	NetworkAvgMS   float64
}

// VideoQuality holds WebRTC video quality metrics.
type VideoQuality struct {
	AvgJitterMS    float64
	PacketLossRate float64
}

// AudioQuality holds WebRTC audio quality metrics.
type AudioQuality struct {
	AvgJitterMS    float64
	PacketLossRate float64
}

// ConnQualityDist holds the fraction of streams in each quality tier.
type ConnQualityDist struct {
	Good float64
	Fair float64
	Poor float64
}

// WebRTCQuality is the response payload for GET /v1/performance/quality (PERF-004).
type WebRTCQuality struct {
	StartTime   time.Time
	EndTime     time.Time
	Video       VideoQuality
	Audio       AudioQuality
	ConnQuality ConnQualityDist
}
