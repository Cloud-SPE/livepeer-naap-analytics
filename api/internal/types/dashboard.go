// Package types — Dashboard endpoint types (R16).
// These types map directly to the @naap/plugin-sdk TypeScript interfaces so the
// naap-ui facade can use the API responses without client-side transformation.
package types

// DashboardMetricDelta is a single KPI value paired with a period-over-period delta.
type DashboardMetricDelta struct {
	Value float64 `json:"value"`
	Delta float64 `json:"delta"`
}

// DashboardHourlyBucket is one hour of a time-series KPI chart.
type DashboardHourlyBucket struct {
	Hour  string  `json:"hour"`  // RFC 3339
	Value float64 `json:"value"`
}

// DashboardKPI is the top-level KPI widget payload (matches plugin-sdk DashboardKPI).
type DashboardKPI struct {
	SuccessRate         DashboardMetricDelta    `json:"successRate"`
	OrchestratorsOnline DashboardMetricDelta    `json:"orchestratorsOnline"`
	DailyUsageMins      DashboardMetricDelta    `json:"dailyUsageMins"`      // Phase 2
	DailySessionCount   DashboardMetricDelta    `json:"dailySessionCount"`
	DailyNetworkFeesEth DashboardMetricDelta    `json:"dailyNetworkFeesEth"` // Phase 4
	TimeframeHours      int                     `json:"timeframeHours"`
	HourlySessions      []DashboardHourlyBucket `json:"hourlySessions,omitempty"`
	HourlyUsage         []DashboardHourlyBucket `json:"hourlyUsage,omitempty"`
}

// DashboardPipelineModelMins is a per-model breakdown within a pipeline usage entry.
type DashboardPipelineModelMins struct {
	Model    string  `json:"model"`
	Mins     float64 `json:"mins"`
	Sessions int64   `json:"sessions"`
	AvgFps   float64 `json:"avgFps"`
}

// DashboardPipelineUsage is one pipeline entry in the pipeline usage widget
// (matches plugin-sdk DashboardPipelineUsage).
type DashboardPipelineUsage struct {
	Name      string                       `json:"name"`
	Mins      float64                      `json:"mins"`     // Phase 2: from network demand
	Sessions  int64                        `json:"sessions"`
	AvgFps    float64                      `json:"avgFps"`   // Phase 2
	JobType   string                       `json:"job_type,omitempty"`
	ModelMins []DashboardPipelineModelMins `json:"modelMins,omitempty"`
}

// DashboardPipelineModelOffer lists the model IDs an orchestrator serves for a pipeline.
type DashboardPipelineModelOffer struct {
	PipelineID string   `json:"pipelineId"`
	ModelIDs   []string `json:"modelIds"`
}

// DashboardOrchestrator is one row in the orchestrator table widget
// (matches plugin-sdk DashboardOrchestrator).
type DashboardOrchestrator struct {
	Address              string                        `json:"address"`
	EnsName              string                        `json:"ensName,omitempty"`
	ServiceURI           string                        `json:"serviceUri,omitempty"`
	KnownSessions        int64                         `json:"knownSessions"`
	SuccessSessions      int64                         `json:"successSessions"`
	SuccessRatio         float64                       `json:"successRatio"`
	EffectiveSuccessRate *float64                      `json:"effectiveSuccessRate"`
	NoSwapRatio          *float64                      `json:"noSwapRatio"`
	SLAScore             *float64                      `json:"slaScore"`
	Pipelines            []string                      `json:"pipelines"`
	PipelineModels       []DashboardPipelineModelOffer `json:"pipelineModels"`
	GPUCount             int64                         `json:"gpuCount"`
}

// DashboardGPUModelCapacity is one GPU model row in the overall GPU capacity summary.
type DashboardGPUModelCapacity struct {
	Model string `json:"model"`
	Count int64  `json:"count"`
}

// DashboardGPUCapacityPipelineModel is one model within a pipeline GPU breakdown.
type DashboardGPUCapacityPipelineModel struct {
	Model string `json:"model"`
	GPUs  int64  `json:"gpus"`
}

// DashboardGPUCapacityPipeline is one pipeline row in the GPU capacity widget.
type DashboardGPUCapacityPipeline struct {
	Name   string                              `json:"name"`
	GPUs   int64                               `json:"gpus"`
	Models []DashboardGPUCapacityPipelineModel `json:"models,omitempty"`
}

// DashboardGPUCapacity is the GPU capacity widget payload
// (matches plugin-sdk DashboardGPUCapacity).
type DashboardGPUCapacity struct {
	TotalGPUs         int64                          `json:"totalGPUs"`
	ActiveGPUs        int64                          `json:"activeGPUs"`
	AvailableCapacity float64                        `json:"availableCapacity"`
	Models            []DashboardGPUModelCapacity    `json:"models"`
	PipelineGPUs      []DashboardGPUCapacityPipeline `json:"pipelineGPUs"`
}

// DashboardPipelineCatalogEntry is one pipeline in the catalog widget
// (matches plugin-sdk DashboardPipelineCatalogEntry).
type DashboardPipelineCatalogEntry struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Models  []string `json:"models"`
	Regions []string `json:"regions"`
}

// DashboardPipelinePricing carries raw wei pricing per pipeline+model so the UI
// can apply its own unit conversion and formatting.
type DashboardPipelinePricing struct {
	Pipeline           string  `json:"pipeline"`
	Model              string  `json:"model,omitempty"`
	OrchCount          int64   `json:"orchCount"`
	PriceMinWeiPerUnit int64   `json:"priceMinWeiPerUnit"`
	PriceMaxWeiPerUnit int64   `json:"priceMaxWeiPerUnit"`
	PriceAvgWeiPerUnit float64 `json:"priceAvgWeiPerUnit"`
	PixelsPerUnit      int64   `json:"pixelsPerUnit"`
}

// DashboardJobFeedItem is one active stream in the live job feed widget
// (matches the JobFeedItem shape in naap-ui/facade/types.ts).
type DashboardJobFeedItem struct {
	ID              string   `json:"id"`
	Pipeline        string   `json:"pipeline"`
	Model           string   `json:"model,omitempty"`
	Gateway         string   `json:"gateway"`
	OrchestratorURL string   `json:"orchestratorUrl"`
	State           string   `json:"state"`
	JobType         string   `json:"job_type,omitempty"`
	InputFPS        float64  `json:"inputFps"`
	OutputFPS       float64  `json:"outputFps"`
	FirstSeen       string   `json:"firstSeen"`
	LastSeen        string   `json:"lastSeen"`
	DurationSeconds *float64 `json:"durationSeconds,omitempty"`
}
