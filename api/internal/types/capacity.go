package types

import "time"

// CapacityEntry is one row for active requirement NET-003 served by
// GET /v1/net/capacity.
type CapacityEntry struct {
	Pipeline       string
	ModelID        string
	WarmOrchCount  int64
	ActiveStreams   int64
	UtilizationPct float64  // active / warm orchs, 0-1
	TotalVRAMBytes uint64
}

// CapacitySummary is the response shape for active requirement NET-003 served by
// GET /v1/net/capacity.
type CapacitySummary struct {
	SnapshotTime time.Time
	Entries      []CapacityEntry
}
