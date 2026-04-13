package types

// DashboardKPICombined wraps streaming KPI and request-job overview into one payload.
type DashboardKPICombined struct {
	Streaming *DashboardKPI          `json:"streaming"`
	Requests  *DashboardJobsOverview `json:"requests"`
}

// DashboardPipelinesCombined wraps streaming pipeline usage and request-job breakdowns.
type DashboardPipelinesCombined struct {
	Streaming []DashboardPipelineUsage            `json:"streaming"`
	Requests  *DashboardPipelinesRequestsSection  `json:"requests"`
}

// DashboardPipelinesRequestsSection holds per-pipeline (AI Batch) and per-capability (BYOC) breakdowns.
type DashboardPipelinesRequestsSection struct {
	ByPipeline   []DashboardJobsByPipelineRow   `json:"by_pipeline"`
	ByCapability []DashboardJobsByCapabilityRow `json:"by_capability"`
}
