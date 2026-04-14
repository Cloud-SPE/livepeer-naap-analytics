package types

// StreamingModel — one row from GET /v1/streaming/models.
//
// Per-model capacity and load view for the live-video-to-video streaming page.
type StreamingModel struct {
	Pipeline          string  `json:"pipeline"`
	Model             string  `json:"model"`
	WarmOrchCount     int64   `json:"warm_orch_count"`
	GPUSlots          int64   `json:"gpu_slots"`
	ActiveStreams     int64   `json:"active_streams"`
	AvailableCapacity int64   `json:"available_capacity"`
	AvgFPS            float64 `json:"avg_fps"`
}
