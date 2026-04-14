package types

import "time"

// ---------------------------------------------------------------------------
// Streaming Models (endpoint 8)
// ---------------------------------------------------------------------------

type StreamingModel struct {
	Pipeline          string  `json:"pipeline"`
	Model             string  `json:"model"`
	WarmOrchCount     int64   `json:"warm_orch_count"`
	GPUSlots          int64   `json:"gpu_slots"`
	ActiveStreams     int64   `json:"active_streams"`
	AvailableCapacity int64   `json:"available_capacity"`
	AvgFPS            float64 `json:"avg_fps"`
}

// ---------------------------------------------------------------------------
// Streaming Orchestrators (endpoint 9)
// ---------------------------------------------------------------------------

type StreamingOrchestrator struct {
	Address  string   `json:"address"`
	URI      string   `json:"uri"`
	Models   []string `json:"models"`
	GPUCount int64    `json:"gpu_count"`
	LastSeen string   `json:"last_seen"` // RFC 3339
}

// ---------------------------------------------------------------------------
// Streaming SLA (endpoint 10)
// ---------------------------------------------------------------------------

type StreamingSLARow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    *string   `json:"org,omitempty"`
	OrchestratorAddress    string    `json:"orchestrator_address"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                string    `json:"model_id"`
	GPUID                  string    `json:"gpu_id"`
	RequestedSessions      uint64    `json:"requested_sessions"`
	StartupSuccessSessions uint64    `json:"startup_success_sessions"`
	EffectiveSuccessRate   float64   `json:"effective_success_rate"`
	NoSwapRate             float64   `json:"no_swap_rate"`
	AvgOutputFPS           float64   `json:"avg_output_fps"`
	SLAScore               float64   `json:"sla_score"`
}

// ---------------------------------------------------------------------------
// Streaming Demand (endpoint 11)
// ---------------------------------------------------------------------------

type StreamingDemandRow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    *string   `json:"org,omitempty"`
	Gateway                string    `json:"gateway"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                *string   `json:"model_id"`
	RequestedSessions      uint64    `json:"requested_sessions"`
	StartupSuccessSessions uint64    `json:"startup_success_sessions"`
	EffectiveSuccessRate   float64   `json:"effective_success_rate"`
	TotalMinutes           float64   `json:"total_minutes"`
	NoOrchSessions         uint64    `json:"no_orch_sessions"`
}

// ---------------------------------------------------------------------------
// Streaming GPU Metrics (endpoint 12)
// ---------------------------------------------------------------------------

type StreamingGPUMetricRow struct {
	WindowStart            time.Time `json:"window_start"`
	Org                    *string   `json:"org,omitempty"`
	OrchestratorAddress    string    `json:"orchestrator_address"`
	PipelineID             string    `json:"pipeline_id"`
	ModelID                string    `json:"model_id"`
	GPUID                  string    `json:"gpu_id"`
	GPUModelName           string    `json:"gpu_model_name"`
	KnownSessionsCount    uint64    `json:"known_sessions_count"`
	StartupSuccessSessions uint64    `json:"startup_success_sessions"`
	AvgOutputFPS           float64   `json:"avg_output_fps"`
	AvgE2ELatencyMs        float64   `json:"avg_e2e_latency_ms"`
	SwapRate               float64   `json:"swap_rate"`
}
