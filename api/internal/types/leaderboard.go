package types

import "time"

// LeaderboardEntry holds competitive ranking data for one orchestrator (LDR-001).
type LeaderboardEntry struct {
	Address         string
	Name            string
	Score           float64
	StreamsHandled  int64
	AvgInferenceFPS float64
	DegradedRate    float64
	AvgLatencyMS    float64
	LastSeen        time.Time
	IsActive        bool
}

// OrchProfile holds the detailed profile for one orchestrator (LDR-002).
type OrchProfile struct {
	Address         string
	Name            string
	URI             string
	Version         string
	Org             string
	LastSeen        time.Time
	IsActive        bool
	StreamsHandled  int64
	AvgInferenceFPS float64
	DegradedRate    float64
	AvgLatencyMS    float64
	RawCapabilities string // raw JSON for GPU/model/pricing detail
}
