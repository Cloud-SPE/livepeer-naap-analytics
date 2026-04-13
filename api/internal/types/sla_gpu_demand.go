package types

import "time"

// ---------------------------------------------------------------------------
// SLA Compliance
// ---------------------------------------------------------------------------

// SLAComplianceParams holds validated query parameters for GET /v1/streaming/sla.
type SLAComplianceParams struct {
	OrchestratorAddress string
	Region              string
	PipelineID          string
	ModelID             string
	GPUID               string
	Org                 string
	Start               time.Time
	End                 time.Time
	Limit               int
	Cursor              string
}

// SLAComplianceRow is one row in the /v1/streaming/sla response.
// `sla_score` is the contracted quality-aware composite score. Rows come from
// precomputed final SLA serving stores, and component fields are exposed
// alongside additive support inputs so downstream consumers do not have to
// infer or re-roll unsafe aggregate-of-aggregate math.
// Nullable fields use pointer types; nil serialises as JSON null.
type SLAComplianceRow struct {
	WindowStart               time.Time `json:"window_start"`
	Org                       *string   `json:"org,omitempty"`
	OrchestratorAddress       string    `json:"orchestrator_address"`
	PipelineID                string    `json:"pipeline_id"`
	ModelID                   *string   `json:"model_id"`
	GPUID                     *string   `json:"gpu_id"`
	GPUModelName              *string   `json:"gpu_model_name"`
	Region                    *string   `json:"region"`
	KnownSessionsCount        uint64    `json:"known_sessions_count"`
	RequestedSessions         uint64    `json:"requested_sessions"`
	StartupSuccessSessions    uint64    `json:"startup_success_sessions"`
	NoOrchSessions            uint64    `json:"no_orch_sessions"`
	StartupExcusedSessions    uint64    `json:"startup_excused_sessions"`
	StartupFailedSessions     uint64    `json:"startup_failed_sessions"`
	LoadingOnlySessions       uint64    `json:"loading_only_sessions"`
	ZeroOutputFPSSessions     uint64    `json:"zero_output_fps_sessions"`
	OutputFailedSessions      uint64    `json:"output_failed_sessions"`
	EffectiveFailedSessions   uint64    `json:"effective_failed_sessions"`
	ConfirmedSwappedSessions  uint64    `json:"confirmed_swapped_sessions"`
	InferredSwapSessions      uint64    `json:"inferred_swap_sessions"`
	TotalSwappedSessions      uint64    `json:"total_swapped_sessions"`
	SessionsEndingInError     uint64    `json:"sessions_ending_in_error"`
	ErrorStatusSamples        uint64    `json:"error_status_samples"`
	HealthSignalCount         uint64    `json:"health_signal_count"`
	HealthExpectedSignalCount uint64    `json:"health_expected_signal_count"`
	HealthSignalCoverageRatio float64   `json:"health_signal_coverage_ratio"`
	StartupSuccessRate        *float64  `json:"startup_success_rate"`
	ExcusedFailureRate        *float64  `json:"excused_failure_rate"`
	EffectiveSuccessRate      *float64  `json:"effective_success_rate"`
	NoSwapRate                *float64  `json:"no_swap_rate"`
	OutputViabilityRate       *float64  `json:"output_viability_rate"`
	OutputFPSSum              float64   `json:"output_fps_sum"`
	StatusSamples             uint64    `json:"status_samples"`
	AvgOutputFPS              *float64  `json:"avg_output_fps"`
	PromptToFirstFrameSumMS   float64   `json:"prompt_to_first_frame_sum_ms"`
	PromptToFirstFrameSamples uint64    `json:"prompt_to_first_frame_sample_count"`
	AvgPromptToFirstFrameMS   *float64  `json:"avg_prompt_to_first_frame_ms"`
	E2ELatencySumMS           float64   `json:"e2e_latency_sum_ms"`
	E2ELatencySamples         uint64    `json:"e2e_latency_sample_count"`
	AvgE2ELatencyMS           *float64  `json:"avg_e2e_latency_ms"`
	ReliabilityScore          *float64  `json:"reliability_score"`
	PTFFScore                 float64   `json:"ptff_score"`
	E2EScore                  float64   `json:"e2e_score"`
	LatencyScore              float64   `json:"latency_score"`
	FPSScore                  float64   `json:"fps_score"`
	QualityScore              float64   `json:"quality_score"`
	SLASemanticsVersion       string    `json:"sla_semantics_version"`
	SLAScore                  *float64  `json:"sla_score"`
}

// ---------------------------------------------------------------------------
// Network Demand
// ---------------------------------------------------------------------------

// NetworkDemandParams holds validated query parameters for GET /v1/streaming/demand.
type NetworkDemandParams struct {
	Gateway    string
	Region     string
	PipelineID string
	ModelID    string
	Org        string
	Start      time.Time
	End        time.Time
	Limit      int
	Cursor     string
}

// NetworkDemandRow is one row in the /v1/streaming/demand response.
type NetworkDemandRow struct {
	WindowStart               time.Time `json:"window_start"`
	Org                       *string   `json:"org,omitempty"`
	Gateway                   string    `json:"gateway"`
	Region                    *string   `json:"region"`
	PipelineID                string    `json:"pipeline_id"`
	ModelID                   *string   `json:"model_id"`
	SessionsCount             uint64    `json:"sessions_count"`
	AvgOutputFPS              float64   `json:"avg_output_fps"`
	OutputFPSSum              float64   `json:"output_fps_sum"`
	StatusSamples             uint64    `json:"status_samples"`
	TotalMinutes              float64   `json:"total_minutes"`
	KnownSessionsCount        uint64    `json:"known_sessions_count"`
	RequestedSessions         uint64    `json:"requested_sessions"`
	StartupSuccessSessions    uint64    `json:"startup_success_sessions"`
	NoOrchSessions            uint64    `json:"no_orch_sessions"`
	StartupExcusedSessions    uint64    `json:"startup_excused_sessions"`
	StartupFailedSessions     uint64    `json:"startup_failed_sessions"`
	LoadingOnlySessions       uint64    `json:"loading_only_sessions"`
	ZeroOutputFPSSessions     uint64    `json:"zero_output_fps_sessions"`
	EffectiveFailedSessions   uint64    `json:"effective_failed_sessions"`
	ConfirmedSwappedSessions  uint64    `json:"confirmed_swapped_sessions"`
	InferredSwapSessions      uint64    `json:"inferred_swap_sessions"`
	TotalSwappedSessions      uint64    `json:"total_swapped_sessions"`
	SessionsEndingInError     uint64    `json:"sessions_ending_in_error"`
	ErrorStatusSamples        uint64    `json:"error_status_samples"`
	HealthSignalCount         uint64    `json:"health_signal_count"`
	HealthExpectedSignalCount uint64    `json:"health_expected_signal_count"`
	HealthSignalCoverageRatio float64   `json:"health_signal_coverage_ratio"`
	StartupSuccessRate        float64   `json:"startup_success_rate"`
	ExcusedFailureRate        float64   `json:"excused_failure_rate"`
	EffectiveSuccessRate      float64   `json:"effective_success_rate"`
	TicketFaceValueETH        float64   `json:"ticket_face_value_eth"`
}

// ---------------------------------------------------------------------------
// GPU-Sliced Network Demand
// ---------------------------------------------------------------------------

// GPUNetworkDemandParams holds validated query parameters for GET /v1/streaming/gpu-demand.
type GPUNetworkDemandParams struct {
	Gateway             string
	OrchestratorAddress string
	Region              string
	PipelineID          string
	ModelID             string
	GPUID               string
	Org                 string
	Start               time.Time
	End                 time.Time
	Limit               int
	Cursor              string
}

// GPUNetworkDemandRow is one row in the /v1/streaming/gpu-demand response.
type GPUNetworkDemandRow struct {
	WindowStart               time.Time `json:"window_start"`
	Org                       *string   `json:"org,omitempty"`
	Gateway                   string    `json:"gateway"`
	OrchestratorAddress       string    `json:"orchestrator_address"`
	Region                    *string   `json:"region"`
	PipelineID                string    `json:"pipeline_id"`
	ModelID                   *string   `json:"model_id"`
	GPUID                     *string   `json:"gpu_id"`
	GPUIdentityStatus         string    `json:"gpu_identity_status"`
	SessionsCount             uint64    `json:"sessions_count"`
	AvgOutputFPS              float64   `json:"avg_output_fps"`
	OutputFPSSum              float64   `json:"output_fps_sum"`
	StatusSamples             uint64    `json:"status_samples"`
	TotalMinutes              float64   `json:"total_minutes"`
	KnownSessionsCount        uint64    `json:"known_sessions_count"`
	RequestedSessions         uint64    `json:"requested_sessions"`
	StartupSuccessSessions    uint64    `json:"startup_success_sessions"`
	NoOrchSessions            uint64    `json:"no_orch_sessions"`
	StartupExcusedSessions    uint64    `json:"startup_excused_sessions"`
	StartupFailedSessions     uint64    `json:"startup_failed_sessions"`
	LoadingOnlySessions       uint64    `json:"loading_only_sessions"`
	ZeroOutputFPSSessions     uint64    `json:"zero_output_fps_sessions"`
	EffectiveFailedSessions   uint64    `json:"effective_failed_sessions"`
	ConfirmedSwappedSessions  uint64    `json:"confirmed_swapped_sessions"`
	InferredSwapSessions      uint64    `json:"inferred_swap_sessions"`
	TotalSwappedSessions      uint64    `json:"total_swapped_sessions"`
	SessionsEndingInError     uint64    `json:"sessions_ending_in_error"`
	ErrorStatusSamples        uint64    `json:"error_status_samples"`
	HealthSignalCount         uint64    `json:"health_signal_count"`
	HealthExpectedSignalCount uint64    `json:"health_expected_signal_count"`
	HealthSignalCoverageRatio float64   `json:"health_signal_coverage_ratio"`
	StartupSuccessRate        float64   `json:"startup_success_rate"`
	ExcusedFailureRate        float64   `json:"excused_failure_rate"`
	EffectiveSuccessRate      float64   `json:"effective_success_rate"`
	TicketFaceValueETH        float64   `json:"ticket_face_value_eth"`
}

// ---------------------------------------------------------------------------
// GPU Metrics
// ---------------------------------------------------------------------------

// GPUMetricsParams holds validated query parameters for GET /v1/streaming/gpu-metrics.
type GPUMetricsParams struct {
	OrchestratorAddress string
	GPUID               string
	Region              string
	PipelineID          string
	ModelID             string
	GPUModelName        string
	RunnerVersion       string
	CudaVersion         string
	Org                 string
	Start               time.Time
	End                 time.Time
	Limit               int
	Cursor              string
}

// GPUMetric is one row in the /v1/streaming/gpu-metrics response.
type GPUMetric struct {
	WindowStart               time.Time `json:"window_start"`
	Org                       *string   `json:"org,omitempty"`
	OrchestratorAddress       string    `json:"orchestrator_address"`
	PipelineID                string    `json:"pipeline_id"`
	ModelID                   *string   `json:"model_id"`
	GPUID                     *string   `json:"gpu_id"`
	Region                    *string   `json:"region"`
	AvgOutputFPS              float64   `json:"avg_output_fps"`
	OutputFPSSum              float64   `json:"output_fps_sum"`
	P95OutputFPS              float64   `json:"p95_output_fps"`
	FPSJitterCoefficient      *float64  `json:"fps_jitter_coefficient"`
	StatusSamples             uint64    `json:"status_samples"`
	ErrorStatusSamples        uint64    `json:"error_status_samples"`
	HealthSignalCount         uint64    `json:"health_signal_count"`
	HealthExpectedSignalCount uint64    `json:"health_expected_signal_count"`
	HealthSignalCoverageRatio float64   `json:"health_signal_coverage_ratio"`

	// Hardware dimensions (from naap.agg_gpu_inventory join)
	GPUModelName        *string `json:"gpu_model_name"`
	GPUMemoryBytesTotal *uint64 `json:"gpu_memory_bytes_total"`
	RunnerVersion       *string `json:"runner_version"`
	CudaVersion         *string `json:"cuda_version"`

	// Latency metrics
	AvgPromptToFirstFrameMS        *float64 `json:"avg_prompt_to_first_frame_ms"`
	PromptToFirstFrameSumMS        float64  `json:"prompt_to_first_frame_sum_ms"`
	AvgStartupLatencyMS            *float64 `json:"avg_startup_latency_ms"`
	StartupLatencySumMS            float64  `json:"startup_latency_sum_ms"`
	AvgE2ELatencyMS                *float64 `json:"avg_e2e_latency_ms"`
	E2ELatencySumMS                float64  `json:"e2e_latency_sum_ms"`
	P95PromptToFirstFrameLatencyMS *float64 `json:"p95_prompt_to_first_frame_latency_ms"`
	P95StartupLatencyMS            *float64 `json:"p95_startup_latency_ms"`
	P95E2ELatencyMS                *float64 `json:"p95_e2e_latency_ms"`

	// Sample counts
	PromptToFirstFrameSampleCount uint64 `json:"prompt_to_first_frame_sample_count"`
	StartupLatencySampleCount     uint64 `json:"startup_latency_sample_count"`
	E2ELatencySampleCount         uint64 `json:"e2e_latency_sample_count"`

	// Session breakdowns
	KnownSessionsCount       uint64 `json:"known_sessions_count"`
	StartupSuccessSessions   uint64 `json:"startup_success_sessions"`
	NoOrchSessions           uint64 `json:"no_orch_sessions"`
	StartupExcusedSessions   uint64 `json:"startup_excused_sessions"`
	StartupFailedSessions    uint64 `json:"startup_failed_sessions"`
	ConfirmedSwappedSessions uint64 `json:"confirmed_swapped_sessions"`
	InferredSwapSessions     uint64 `json:"inferred_swap_sessions"`
	TotalSwappedSessions     uint64 `json:"total_swapped_sessions"`
	SessionsEndingInError    uint64 `json:"sessions_ending_in_error"`

	// Rates
	StartupFailedRate float64 `json:"startup_failed_rate"`
	SwapRate          float64 `json:"swap_rate"`
}
