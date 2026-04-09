package types

import "time"

// FPSStats holds FPS distribution statistics.
type FPSStats struct {
	Avg float64
	P5  float64
	P50 float64
	P99 float64
}

// PipelineFPS is one pipeline aggregate in the legacy PERF-001 lower-layer FPS summary shape.
type PipelineFPS struct {
	Pipeline     string
	InferenceFPS FPSStats
	InputFPS     FPSStats
	SampleCount  int64
}

// OrchFPS is one orch-plus-pipeline aggregate in the legacy PERF-001 lower-layer FPS summary shape.
type OrchFPS struct {
	Address      string
	Name         string
	Pipeline     string
	InferenceFPS FPSStats
	InputFPS     FPSStats
	SampleCount  int64
}

// FPSSummary is the legacy PERF-001 lower-layer FPS summary shape.
// It is not a current routed API response.
type FPSSummary struct {
	StartTime      time.Time
	EndTime        time.Time
	ByPipeline     []PipelineFPS
	ByOrchestrator []OrchFPS
}

// FPSBucket is one bucket in the legacy PERF-002 lower-layer FPS history shape.
// Degraded is true when avg inference FPS drops ≥20% from the prior bucket.
type FPSBucket struct {
	Timestamp       time.Time
	AvgInferenceFPS float64
	AvgInputFPS     float64
	SampleCount     int64
	Degraded        bool
}

// OrchLatency is one orchestrator row in the legacy PERF-003 lower-layer latency shape.
type OrchLatency struct {
	Address     string
	Name        string
	AvgMS       float64
	P50MS       float64
	P95MS       float64
	P99MS       float64
	SampleCount int64
}

// LatencySummary is the legacy PERF-003 lower-layer discovery-latency summary shape.
// It is not a current routed API response.
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

// WebRTCQuality is the legacy PERF-004 lower-layer WebRTC-quality summary shape.
// It is not a current routed API response.
type WebRTCQuality struct {
	StartTime   time.Time
	EndTime     time.Time
	Video       VideoQuality
	Audio       AudioQuality
	ConnQuality ConnQualityDist
}
