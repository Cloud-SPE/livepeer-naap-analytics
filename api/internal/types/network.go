package types

import "time"

// NetworkSummary is the response payload for GET /v1/network/summary (NET-001).
type NetworkSummary struct {
	Org                    string
	SnapshotTime           time.Time
	TotalRegistered        int64
	TotalActive            int64
	ActiveThresholdMinutes int
}

// Orchestrator is a single orch entry for GET /v1/network/orchestrators (NET-002).
// RawCapabilities is the full JSON blob from the capabilities snapshot — the service
// layer parses GPU, model, and pricing detail from it for the HTTP response.
type Orchestrator struct {
	Address         string
	Org             string
	Name            string // hostname of orch_uri; falls back to address
	URI             string // orch_uri from capabilities event
	Version         string
	LastSeen        time.Time
	IsActive        bool
	RawCapabilities string // raw JSON; parsed by service layer for GPU/model/pricing
}

// GPUModel is aggregated GPU stats for one hardware model (NET-003).
type GPUModel struct {
	Model       string
	Count       int64
	TotalVRAMGB float64
	VRAMPerGPUGB float64
}

// GPUSummary is the response payload for GET /v1/network/gpus (NET-003).
type GPUSummary struct {
	TotalGPUs   int64
	TotalVRAMGB float64
	ByModel     []GPUModel
}

// ModelAvailability is a single model entry for GET /v1/network/models (NET-004).
type ModelAvailability struct {
	Pipeline            string
	Model               string
	WarmOrchCount       int64
	TotalCapacity       int64
	PriceMinWeiPerPixel int64
	PriceMaxWeiPerPixel int64
	PriceAvgWeiPerPixel float64
}
