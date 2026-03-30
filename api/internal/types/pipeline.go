package types

import "time"

// PipelineSummary is one row for GET /v1/pipelines (PIPE-001).
type PipelineSummary struct {
	Pipeline         string
	ActiveStreams    int64
	RequestedCount   int64
	SuccessRate      float64
	AvgInferenceFPS  float64
	TotalPaymentsWEI WEI
	WarmOrchCount    int64
	TopOrchAddress   string
}

// ModelPipelineStats holds per-model stats within a pipeline.
type ModelPipelineStats struct {
	ModelID       string
	WarmOrchCount int64
	AvgFPS        float64
}

// PipelineDetail is the response for GET /v1/pipelines/{pipeline} (PIPE-002).
type PipelineDetail struct {
	Pipeline         string
	StartTime        time.Time
	EndTime          time.Time
	ActiveStreams    int64
	RequestedCount   int64
	SuccessRate      float64
	AvgInferenceFPS  float64
	TotalPaymentsWEI WEI
	WarmOrchCount    int64
	ByModel          []ModelPipelineStats
}
