package types

import "time"

// RequestsModel — one row from GET /v1/requests/models.
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

// RequestsOrchestrator — one row from GET /v1/requests/orchestrators.
type RequestsOrchestrator struct {
	Address      string    `json:"address"`
	URI          string    `json:"uri"`
	Capabilities []string  `json:"capabilities"`
	GPUCount     int64     `json:"gpu_count"`
	LastSeen     time.Time `json:"last_seen"`
}

// AIBatchSummaryRow — GET /v1/requests/ai-batch/summary row.
type AIBatchSummaryRow struct {
	Pipeline      string  `json:"pipeline"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// AIBatchJobRecord — single AI Batch job record.
type AIBatchJobRecord struct {
	RequestID         string    `json:"request_id"`
	Org               string    `json:"org"`
	Gateway           string    `json:"gateway"`
	Pipeline          string    `json:"pipeline"`
	ModelID           string    `json:"model_id"`
	CompletedAt       time.Time `json:"completed_at"`
	Success           bool      `json:"success"`
	Tries             int64     `json:"tries"`
	DurationMs        int64     `json:"duration_ms"`
	OrchURL           string    `json:"orch_url"`
	SelectionOutcome  string    `json:"selection_outcome"`
	GPUModelName      string    `json:"gpu_model_name"`
	AttributionStatus string    `json:"attribution_status"`
}

// AIBatchLLMSummaryRow — GET /v1/requests/ai-batch/llm-summary row.
type AIBatchLLMSummaryRow struct {
	Model            string  `json:"model"`
	TotalRequests    int64   `json:"total_requests"`
	SuccessRate      float64 `json:"success_rate"`
	AvgTokensPerSec  float64 `json:"avg_tokens_per_sec"`
	AvgTTFTMs        float64 `json:"avg_ttft_ms"`
	AvgTotalTokens   float64 `json:"avg_total_tokens"`
}

// BYOCSummaryRow — GET /v1/requests/byoc/summary row.
type BYOCSummaryRow struct {
	Capability    string  `json:"capability"`
	TotalJobs     int64   `json:"total_jobs"`
	SelectedJobs  int64   `json:"selected_jobs"`
	NoOrchJobs    int64   `json:"no_orch_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// BYOCJobRecord — single BYOC job record.
type BYOCJobRecord struct {
	RequestID         string    `json:"request_id"`
	Org               string    `json:"org"`
	Capability        string    `json:"capability"`
	Model             string    `json:"model"`
	CompletedAt       time.Time `json:"completed_at"`
	Success           bool      `json:"success"`
	DurationMs        int64     `json:"duration_ms"`
	HTTPStatus        int64     `json:"http_status"`
	OrchAddress       string    `json:"orch_address"`
	OrchURL           string    `json:"orch_url"`
	WorkerURL         string    `json:"worker_url"`
	SelectionOutcome  string    `json:"selection_outcome"`
	GPUModelName      string    `json:"gpu_model_name"`
	AttributionStatus string    `json:"attribution_status"`
	Error             string    `json:"error"`
}

// BYOCWorkerRow — GET /v1/requests/byoc/workers row.
type BYOCWorkerRow struct {
	Capability      string   `json:"capability"`
	WorkerCount     int64    `json:"worker_count"`
	Models          []string `json:"models"`
	AvgPricePerUnit float64  `json:"avg_price_per_unit"`
}

// BYOCAuthRow — GET /v1/requests/byoc/auth row.
type BYOCAuthRow struct {
	Capability   string  `json:"capability"`
	TotalEvents  int64   `json:"total_events"`
	SuccessRate  float64 `json:"success_rate"`
	FailureCount int64   `json:"failure_count"`
}

// Cursor envelopes.
type CursorEnvelopeAIBatchJobs struct {
	Data       []AIBatchJobRecord `json:"data"`
	Pagination CursorPageInfo     `json:"pagination"`
	Meta       map[string]any     `json:"meta"`
}

type CursorEnvelopeBYOCJobs struct {
	Data       []BYOCJobRecord `json:"data"`
	Pagination CursorPageInfo  `json:"pagination"`
	Meta       map[string]any  `json:"meta"`
}
