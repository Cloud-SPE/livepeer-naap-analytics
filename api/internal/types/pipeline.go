package types

import "time"

// PipelineSummary holds cross-cutting summary data per pipeline (PIPE-001).
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

// PipelineDetail holds detailed pipeline stats with model breakdown (PIPE-002).
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
