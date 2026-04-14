package types

// ---------------------------------------------------------------------------
// Dashboard KPI (endpoint 1)
// ---------------------------------------------------------------------------

type MetricDelta struct {
	Value float64 `json:"value"`
	Delta float64 `json:"delta"`
}

type MetricValue struct {
	Value float64 `json:"value"`
}

type HourlyBucket struct {
	Hour  string  `json:"hour"` // RFC 3339
	Value float64 `json:"value"`
}

type DashboardKPI struct {
	SuccessRate         MetricValue    `json:"successRate"`
	OrchestratorsOnline MetricValue    `json:"orchestratorsOnline"`
	DailyUsageMins      MetricDelta    `json:"dailyUsageMins"`
	DailySessionCount   MetricDelta    `json:"dailySessionCount"`
	DailyNetworkFeesEth MetricDelta    `json:"dailyNetworkFeesEth"`
	TimeframeHours      int            `json:"timeframeHours"`
	HourlySessions      []HourlyBucket `json:"hourlySessions,omitempty"`
	HourlyUsage         []HourlyBucket `json:"hourlyUsage,omitempty"`
}

type DashboardJobsStats struct {
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

type DashboardJobsOverview struct {
	AIBatch DashboardJobsStats `json:"ai_batch"`
	BYOC    DashboardJobsStats `json:"byoc"`
}

type DashboardKPICombined struct {
	Streaming *DashboardKPI          `json:"streaming"`
	Requests  *DashboardJobsOverview `json:"requests"`
}

// ---------------------------------------------------------------------------
// Dashboard Pipelines (endpoint 2)
// ---------------------------------------------------------------------------

type DashboardPipelineModelMins struct {
	Model    string  `json:"model"`
	Mins     float64 `json:"mins"`
	Sessions int64   `json:"sessions"`
	AvgFps   float64 `json:"avgFps"`
}

type DashboardPipelineUsage struct {
	Name      string                       `json:"name"`
	Mins      float64                      `json:"mins"`
	Sessions  int64                        `json:"sessions"`
	AvgFps    float64                      `json:"avgFps"`
	ModelMins []DashboardPipelineModelMins `json:"modelMins,omitempty"`
}

type DashboardJobsByPipelineRow struct {
	Pipeline      string  `json:"pipeline"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

type DashboardJobsByCapabilityRow struct {
	Capability    string  `json:"capability"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

type DashboardPipelinesRequestsSection struct {
	ByPipeline   []DashboardJobsByPipelineRow   `json:"by_pipeline"`
	ByCapability []DashboardJobsByCapabilityRow `json:"by_capability"`
}

type DashboardPipelinesCombined struct {
	Streaming []DashboardPipelineUsage           `json:"streaming"`
	Requests  *DashboardPipelinesRequestsSection `json:"requests"`
}

// ---------------------------------------------------------------------------
// Dashboard Orchestrators (endpoint 3)
// ---------------------------------------------------------------------------

type DashboardPipelineModelOffer struct {
	PipelineID string   `json:"pipelineId"`
	ModelIDs   []string `json:"modelIds"`
}

type DashboardOrchestrator struct {
	Address              string                        `json:"address"`
	ServiceURI           string                        `json:"serviceUri,omitempty"`
	KnownSessions        int64                         `json:"knownSessions"`
	SuccessSessions      int64                         `json:"successSessions"`
	SuccessRatio         float64                       `json:"successRatio"`
	EffectiveSuccessRate *float64                      `json:"effectiveSuccessRate"`
	NoSwapRatio          *float64                      `json:"noSwapRatio"`
	SLAScore             *float64                      `json:"slaScore"`
	SLAWindowStart       *string                       `json:"slaWindowStart,omitempty"`
	Pipelines            []string                      `json:"pipelines"`
	PipelineModels       []DashboardPipelineModelOffer `json:"pipelineModels"`
	GPUCount             int64                         `json:"gpuCount"`
}

// ---------------------------------------------------------------------------
// Dashboard GPU Capacity (endpoint 4)
// ---------------------------------------------------------------------------

type DashboardGPUModelCapacity struct {
	Model string `json:"model"`
	Count int64  `json:"count"`
}

type DashboardGPUCapacityPipelineModel struct {
	Model string `json:"model"`
	GPUs  int64  `json:"gpus"`
}

type DashboardGPUCapacityPipeline struct {
	Name   string                              `json:"name"`
	GPUs   int64                               `json:"gpus"`
	Models []DashboardGPUCapacityPipelineModel `json:"models,omitempty"`
}

type DashboardGPUCapacity struct {
	TotalGPUs    int64                          `json:"totalGPUs"`
	Models       []DashboardGPUModelCapacity    `json:"models"`
	PipelineGPUs []DashboardGPUCapacityPipeline `json:"pipelineGPUs"`
}

// ---------------------------------------------------------------------------
// Dashboard Pipeline Catalog (endpoint 5)
// ---------------------------------------------------------------------------

type DashboardPipelineCatalogEntry struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Models  []string `json:"models"`
	Regions []string `json:"regions"`
}

// ---------------------------------------------------------------------------
// Dashboard Pricing (endpoint 6)
// ---------------------------------------------------------------------------

type DashboardPipelinePricing struct {
	OrchAddress     string `json:"orchAddress"`
	OrchName        string `json:"orchName,omitempty"`
	Pipeline        string `json:"pipeline"`
	Model           string `json:"model,omitempty"`
	PriceWeiPerUnit int64  `json:"priceWeiPerUnit"`
	PixelsPerUnit   int64  `json:"pixelsPerUnit"`
	IsWarm          bool   `json:"isWarm"`
}

// ---------------------------------------------------------------------------
// Dashboard Job Feed (endpoint 7)
// ---------------------------------------------------------------------------

type DashboardJobFeedItem struct {
	ID                  string   `json:"id"`
	Pipeline            string   `json:"pipeline"`
	Model               string   `json:"model,omitempty"`
	Gateway             string   `json:"gateway"`
	OrchestratorAddress string   `json:"orchestratorAddress"`
	OrchestratorURL     string   `json:"orchestratorUrl,omitempty"`
	State               string   `json:"state"`
	InputFPS            float64  `json:"inputFps"`
	OutputFPS           float64  `json:"outputFps"`
	FirstSeen           string   `json:"firstSeen"`
	LastSeen            string   `json:"lastSeen"`
	DurationSeconds     *float64 `json:"durationSeconds,omitempty"`
}
