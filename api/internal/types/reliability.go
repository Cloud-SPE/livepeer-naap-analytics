package types

import "time"

// FailureBreakdown holds individual failure type counts (REL-001).
type FailureBreakdown struct {
	NoOrchSessions    int64
	OrchSwap          int64
	InferenceRestart  int64
	InferenceError    int64
	DegradedInference int64
	DegradedInput     int64
}

// ReliabilitySummary holds aggregate reliability metrics for a time window (REL-001).
type ReliabilitySummary struct {
	StartTime            time.Time
	EndTime              time.Time
	StartupSuccessRate   float64
	NoOrchSessionRate    float64
	OrchSwapCount        int64
	OrchSwapRate         float64
	InferenceRestartRate float64
	DegradedStateRate    float64
	FailureBreakdown     FailureBreakdown
}

// ReliabilityBucket is one time-bucket in the reliability history time-series (REL-002).
// Rates are nil when the sample count is below the minimum threshold (5 streams).
type ReliabilityBucket struct {
	Timestamp          time.Time
	RequestedSessions  int64
	StartupSuccessRate *float64
	NoOrchSessionRate  *float64
	DegradedRate       *float64
}

// OrchReliability holds reliability metrics for one orchestrator (REL-003).
type OrchReliability struct {
	Address        string
	StreamsHandled int64
	DegradedRate   float64
	RestartRate    float64
	ErrorRate      float64
}

// FailureEvent is one failure event record (REL-004).
type FailureEvent struct {
	Timestamp   time.Time
	FailureType string
	StreamID    string
	RequestID   string
	Org         string
	Gateway     string
	Detail      string // raw JSON data payload from source event
}
