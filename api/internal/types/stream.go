package types

import "time"

// ActiveStreamsSummary holds a summary of currently active streams (STR-001).
type ActiveStreamsSummary struct {
	TotalActive            int64
	ActiveThresholdSeconds int
	ByOrg                  map[string]int64
	ByPipeline             map[string]int64
	ByState                map[string]int64
}

// StreamSummary holds aggregate stream session statistics for a time window (STR-002).
type StreamSummary struct {
	StartTime          time.Time
	EndTime            time.Time
	TotalRequested     int64
	StartupSuccesses   int64
	NoOrchSessionCount int64
	StartupSuccessRate float64
	NoOrchSessionRate  float64
	OrchSwapCount      int64
}

// StreamBucket is one time-bucket entry in the stream history time-series (STR-003).
type StreamBucket struct {
	Timestamp              time.Time
	RequestedSessions      int64
	StartupSuccessSessions int64
	NoOrchSessions         int64
	OrchSwapSessions       int64
}
