package types

// DashboardJobsOverview is a top-level summary across AI batch and BYOC jobs.
type DashboardJobsOverview struct {
	AIBatch DashboardJobsStats `json:"ai_batch"`
	BYOC    DashboardJobsStats `json:"byoc"`
}

// DashboardJobsStats holds aggregate metrics for one job family.
type DashboardJobsStats struct {
	TotalJobs       int64   `json:"total_jobs"`
	SuccessRate     float64 `json:"success_rate"`
	AvgDurationMs   float64 `json:"avg_duration_ms"`
	P99DurationMs   float64 `json:"p99_duration_ms"`
}

// DashboardJobsByPipelineRow is one row in the AI batch pipeline breakdown.
type DashboardJobsByPipelineRow struct {
	Pipeline      string  `json:"pipeline"`
	TotalJobs     int64   `json:"total_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// DashboardJobsByCapabilityRow is one row in the BYOC capability breakdown.
type DashboardJobsByCapabilityRow struct {
	Capability    string  `json:"capability"`
	TotalJobs     int64   `json:"total_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}
