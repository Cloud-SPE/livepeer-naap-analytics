package types

import "time"

// CapacityEntry is one (pipeline, model) row for GET /v1/net/capacity (CAP-001).
type CapacityEntry struct {
	Pipeline       string
	ModelID        string
	WarmOrchCount  int64
	ActiveStreams   int64
	UtilizationPct float64  // active / warm orchs, 0-1
	TotalVRAMBytes uint64
}

// CapacitySummary is the response for GET /v1/net/capacity (CAP-001).
type CapacitySummary struct {
	SnapshotTime time.Time
	Entries      []CapacityEntry
}
