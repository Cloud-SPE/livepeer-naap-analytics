package types

import "time"

// NetworkSummary is a legacy network-snapshot shape from earlier R1 draft work.
// It is retained for internal and lower-layer use and is not a current routed API response.
type NetworkSummary struct {
	Org                    string
	SnapshotTime           time.Time
	TotalRegistered        int64
	TotalActive            int64
	ActiveThresholdMinutes int
}

// Orchestrator is one row for active requirement NET-001 served by
// GET /v1/net/orchestrators.
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

// GPUModel is one aggregated hardware row used by legacy GPU inventory summary shapes.
type GPUModel struct {
	Model       string
	Count       int64
	TotalVRAMGB float64
	VRAMPerGPUGB float64
}

// GPUSummary is a legacy GPU-inventory summary shape from earlier R1 draft work.
// It is retained for internal and lower-layer use and is not a current routed API response.
type GPUSummary struct {
	TotalGPUs   int64
	TotalVRAMGB float64
	ByModel     []GPUModel
}

// ModelAvailability is one row for active requirement NET-002 served by
// GET /v1/net/models.
type ModelAvailability struct {
	Pipeline            string
	Model               string
	WarmOrchCount       int64
	TotalCapacity       int64
	PriceMinWeiPerPixel int64
	PriceMaxWeiPerPixel int64
	PriceAvgWeiPerPixel float64
}
