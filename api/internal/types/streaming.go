package types

import "time"

// StreamingModel — one row from GET /v1/streaming/models.
type StreamingModel struct {
	Pipeline          string  `json:"pipeline"`
	Model             string  `json:"model"`
	WarmOrchCount     int64   `json:"warm_orch_count"`
	GPUSlots          int64   `json:"gpu_slots"`
	ActiveStreams     int64   `json:"active_streams"`
	AvailableCapacity int64   `json:"available_capacity"`
	AvgFPS            float64 `json:"avg_fps"`
}

// StreamingOrchestrator — one row from GET /v1/streaming/orchestrators.
type StreamingOrchestrator struct {
	Address  string    `json:"address"`
	URI      string    `json:"uri"`
	Models   []string  `json:"models"`
	GPUCount int64     `json:"gpu_count"`
	LastSeen time.Time `json:"last_seen"`
}

// StreamingSLARow — GET /v1/streaming/sla data row.
type StreamingSLARow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    string    `json:"org"`
	OrchestratorAddress    string    `json:"orchestrator_address"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                string    `json:"model_id"`
	GPUID                  string    `json:"gpu_id"`
	RequestedSessions      int64     `json:"requested_sessions"`
	StartupSuccessSessions int64     `json:"startup_success_sessions"`
	EffectiveSuccessRate   float64   `json:"effective_success_rate"`
	NoSwapRate             float64   `json:"no_swap_rate"`
	AvgOutputFPS           float64   `json:"avg_output_fps"`
	SLAScore               float64   `json:"sla_score"`
}

// StreamingDemandRow — GET /v1/streaming/demand data row.
type StreamingDemandRow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    string    `json:"org"`
	Gateway                string    `json:"gateway"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                string    `json:"model_id"`
	RequestedSessions      int64     `json:"requested_sessions"`
	StartupSuccessSessions int64     `json:"startup_success_sessions"`
	EffectiveSuccessRate   float64   `json:"effective_success_rate"`
	TotalMinutes           float64   `json:"total_minutes"`
	NoOrchSessions         int64     `json:"no_orch_sessions"`
}

// StreamingGPUMetricRow — GET /v1/streaming/gpu-metrics data row.
type StreamingGPUMetricRow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    string    `json:"org"`
	OrchestratorAddress    string    `json:"orchestrator_address"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                string    `json:"model_id"`
	GPUID                  string    `json:"gpu_id"`
	GPUModelName           string    `json:"gpu_model_name"`
	KnownSessionsCount     int64     `json:"known_sessions_count"`
	StartupSuccessSessions int64     `json:"startup_success_sessions"`
	AvgOutputFPS           float64   `json:"avg_output_fps"`
	AvgE2ELatencyMs        float64   `json:"avg_e2e_latency_ms"`
	SwapRate               float64   `json:"swap_rate"`
}

// Cursor-paginated envelopes.
type CursorEnvelopeStreamingSLA struct {
	Data       []StreamingSLARow `json:"data"`
	Pagination CursorPageInfo    `json:"pagination"`
	Meta       map[string]any    `json:"meta"`
}

type CursorEnvelopeStreamingDemand struct {
	Data       []StreamingDemandRow `json:"data"`
	Pagination CursorPageInfo       `json:"pagination"`
	Meta       map[string]any       `json:"meta"`
}

type CursorEnvelopeStreamingGPUMetrics struct {
	Data       []StreamingGPUMetricRow `json:"data"`
	Pagination CursorPageInfo          `json:"pagination"`
	Meta       map[string]any          `json:"meta"`
}
