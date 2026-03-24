package types

import "time"

// ActiveStreamsSummary is the response payload for GET /v1/streams/active (STR-001).
type ActiveStreamsSummary struct {
	TotalActive            int64
	ActiveThresholdSeconds int
	ByOrg                  map[string]int64
	ByPipeline             map[string]int64
	ByState                map[string]int64
}

// StreamSummary is the response payload for GET /v1/streams/summary (STR-002).
type StreamSummary struct {
	StartTime            time.Time
	EndTime              time.Time
	TotalStarted         int64
	TotalCompleted       int64
	NoOrchAvailableCount int64
	SuccessRate          float64
	NoOrchAvailableRate  float64
	OrchSwapCount        int64
}

// StreamBucket is one time-bucket entry for GET /v1/streams/history (STR-003).
type StreamBucket struct {
	Timestamp       time.Time
	Started         int64
	Completed       int64
	NoOrchAvailable int64
	OrchSwap        int64
}
