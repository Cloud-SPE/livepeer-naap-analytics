// Package types — AI Batch job and BYOC job domain models.
package types

import "time"

// AIBatchJobSummary is a per-pipeline aggregate for AI batch jobs (R17).
type AIBatchJobSummary struct {
	Pipeline       string  `json:"pipeline"`
	TotalJobs      int64   `json:"total_jobs"`
	SuccessRate    float64 `json:"success_rate"`
	AvgDurationMs  float64 `json:"avg_duration_ms"`
	AvgLatency     float64 `json:"avg_latency_score"`
}

// AIBatchJobRecord is one completed AI batch job (R17).
type AIBatchJobRecord struct {
	RequestID     string     `json:"request_id"`
	Org           string     `json:"org"`
	Gateway       string     `json:"gateway"`
	Pipeline      string     `json:"pipeline"`
	ModelID       string     `json:"model_id"`
	CompletedAt   time.Time  `json:"completed_at"`
	Success       *bool      `json:"success"`
	Tries         int64      `json:"tries"`
	DurationMs    int64      `json:"duration_ms"`
	OrchURL       string     `json:"orch_url"`
	LatencyScore  float64    `json:"latency_score"`
	PricePerUnit  float64    `json:"price_per_unit"`
	ErrorType     string     `json:"error_type"`
	Error         string     `json:"error"`
}

// AIBatchLLMSummary is a per-model aggregate for LLM requests within AI batch (R17).
type AIBatchLLMSummary struct {
	Model          string  `json:"model"`
	TotalRequests  int64   `json:"total_requests"`
	SuccessRate    float64 `json:"success_rate"`
	AvgTokensPerSec float64 `json:"avg_tokens_per_sec"`
	AvgTTFTMs      float64 `json:"avg_ttft_ms"`
	AvgTotalTokens float64 `json:"avg_total_tokens"`
}

// BYOCJobSummary is a per-capability aggregate for BYOC jobs (R18).
// Capability is stored verbatim (e.g. "openai-chat-completions") — never hardcoded.
type BYOCJobSummary struct {
	Capability    string  `json:"capability"`
	TotalJobs     int64   `json:"total_jobs"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// BYOCJobRecord is one completed BYOC job (R18).
type BYOCJobRecord struct {
	RequestID   string    `json:"request_id"`
	Org         string    `json:"org"`
	Capability  string    `json:"capability"`
	CompletedAt time.Time `json:"completed_at"`
	Success     *bool     `json:"success"`
	DurationMs  int64     `json:"duration_ms"`
	HTTPStatus  int64     `json:"http_status"`
	OrchAddress string    `json:"orch_address"`
	OrchURL     string    `json:"orch_url"`
	WorkerURL   string    `json:"worker_url"`
	Error       string    `json:"error"`
}

// BYOCWorkerSummary is a per-capability worker inventory snapshot (R18).
type BYOCWorkerSummary struct {
	Capability      string   `json:"capability"`
	WorkerCount     int64    `json:"worker_count"`
	Models          []string `json:"models"`
	AvgPricePerUnit float64  `json:"avg_price_per_unit"`
}

// BYOCAuthSummary is a per-capability auth event summary (R18).
type BYOCAuthSummary struct {
	Capability   string  `json:"capability"`
	TotalEvents  int64   `json:"total_events"`
	SuccessRate  float64 `json:"success_rate"`
	FailureCount int64   `json:"failure_count"`
}
