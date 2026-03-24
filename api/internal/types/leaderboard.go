package types

import "time"

// LeaderboardEntry is one row for GET /v1/leaderboard/orchestrators (LDR-001).
// Score is 0.0 in Phase 3; computed in Phase 6.
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

// OrchProfile is the response payload for
// GET /v1/leaderboard/orchestrators/{address} (LDR-002).
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
