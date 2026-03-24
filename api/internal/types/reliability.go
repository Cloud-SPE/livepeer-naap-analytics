package types

import "time"

// FailureBreakdown holds individual failure type counts (REL-001).
type FailureBreakdown struct {
	NoOrchAvailable   int64
	OrchSwap          int64
	InferenceRestart  int64
	InferenceError    int64
	DegradedInference int64
	DegradedInput     int64
}

// ReliabilitySummary is the response payload for GET /v1/reliability/summary (REL-001).
type ReliabilitySummary struct {
	StartTime            time.Time
	EndTime              time.Time
	StreamSuccessRate    float64
	NoOrchAvailableRate  float64
	OrchSwapCount        int64
	OrchSwapRate         float64
	InferenceRestartRate float64
	DegradedStateRate    float64
	FailureBreakdown     FailureBreakdown
}

// ReliabilityBucket is one time-bucket for GET /v1/reliability/history (REL-002).
// Rates are nil when the sample count is below the minimum threshold (5 streams).
type ReliabilityBucket struct {
	Timestamp           time.Time
	Started             int64
	SuccessRate         *float64
	NoOrchAvailableRate *float64
	DegradedRate        *float64
}

// OrchReliability is one orch row for GET /v1/reliability/orchestrators (REL-003).
type OrchReliability struct {
	Address        string
	StreamsHandled int64
	DegradedRate   float64
	RestartRate    float64
	ErrorRate      float64
}

// FailureEvent is one row for GET /v1/reliability/failures (REL-004).
type FailureEvent struct {
	Timestamp   time.Time
	FailureType string
	StreamID    string
	RequestID   string
	Org         string
	Gateway     string
	Detail      string // raw JSON data payload from source event
}
