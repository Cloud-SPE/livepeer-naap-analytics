package types

import "time"

// ---------------------------------------------------------------------------
// Requests Models (endpoint 13)
// ---------------------------------------------------------------------------

type RequestsModel struct {
	Pipeline      string  `json:"pipeline"`
	Model         string  `json:"model"`
	JobType       string  `json:"job_type"`
	WarmOrchCount int64   `json:"warm_orch_count"`
	GPUSlots      int64   `json:"gpu_slots"`
	JobCount24h   int64   `json:"job_count_24h"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// ---------------------------------------------------------------------------
// Requests Orchestrators (endpoint 14)
// ---------------------------------------------------------------------------

type RequestsOrchestrator struct {
	Address      string   `json:"address"`
	URI          string   `json:"uri"`
	Capabilities []string `json:"capabilities"`
	GPUCount     int64    `json:"gpu_count"`
	LastSeen     string   `json:"last_seen"` // RFC 3339
}

// ---------------------------------------------------------------------------
// AI Batch Summary (endpoint 15)
// ---------------------------------------------------------------------------

type AIBatchSummaryRow struct {
	Pipeline      string  `json:"pipeline"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// ---------------------------------------------------------------------------
// AI Batch Jobs (endpoint 16)
// ---------------------------------------------------------------------------

type AIBatchJobRecord struct {
	RequestID         string    `json:"request_id"`
	Org               string    `json:"org"`
	Gateway           string    `json:"gateway"`
	Pipeline          string    `json:"pipeline"`
	ModelID           string    `json:"model_id"`
	CompletedAt       time.Time `json:"completed_at"`
	Success           bool      `json:"success"`
	Tries             int32     `json:"tries"`
	DurationMs        int64     `json:"duration_ms"`
	OrchURL           string    `json:"orch_url"`
	SelectionOutcome  string    `json:"selection_outcome"`
	GPUModelName      string    `json:"gpu_model_name"`
	AttributionStatus string    `json:"attribution_status"`
}

// ---------------------------------------------------------------------------
// AI Batch LLM Summary (endpoint 17)
// ---------------------------------------------------------------------------

type AIBatchLLMSummaryRow struct {
	Model            string  `json:"model"`
	TotalRequests    int64   `json:"total_requests"`
	SuccessRate      float64 `json:"success_rate"`
	AvgTokensPerSec  float64 `json:"avg_tokens_per_sec"`
	AvgTTFTMs        float64 `json:"avg_ttft_ms"`
	AvgTotalTokens   float64 `json:"avg_total_tokens"`
}

// ---------------------------------------------------------------------------
// BYOC Summary (endpoint 18)
// ---------------------------------------------------------------------------

type BYOCSummaryRow struct {
	Capability    string  `json:"capability"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// ---------------------------------------------------------------------------
// BYOC Jobs (endpoint 19)
// ---------------------------------------------------------------------------

type BYOCJobRecord struct {
	RequestID         string    `json:"request_id"`
	Org               string    `json:"org"`
	Capability        string    `json:"capability"`
	Model             string    `json:"model"`
	CompletedAt       time.Time `json:"completed_at"`
	Success           bool      `json:"success"`
	DurationMs        int64     `json:"duration_ms"`
	HTTPStatus        int32     `json:"http_status"`
	OrchAddress       string    `json:"orch_address"`
	OrchURL           string    `json:"orch_url"`
	WorkerURL         string    `json:"worker_url"`
	SelectionOutcome  string    `json:"selection_outcome"`
	GPUModelName      string    `json:"gpu_model_name"`
	AttributionStatus string    `json:"attribution_status"`
	Error             string    `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// BYOC Workers (endpoint 20)
// ---------------------------------------------------------------------------

type BYOCWorkerRow struct {
	Capability      string   `json:"capability"`
	WorkerCount     int64    `json:"worker_count"`
	Models          []string `json:"models"`
	AvgPricePerUnit float64  `json:"avg_price_per_unit"`
}

// ---------------------------------------------------------------------------
// BYOC Auth (endpoint 21)
// ---------------------------------------------------------------------------

type BYOCAuthRow struct {
	Capability   string  `json:"capability"`
	TotalEvents  int64   `json:"total_events"`
	SuccessRate  float64 `json:"success_rate"`
	FailureCount int64   `json:"failure_count"`
}
