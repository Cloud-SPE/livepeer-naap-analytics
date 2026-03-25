package types

import "time"

// StreamStatusSample is one telemetry sample from agg_stream_status_samples.
// Used in GET /v1/streams/samples (STR-EXT-001) and GET /v1/streams/{stream_id} (STR-EXT-002).
type StreamStatusSample struct {
	SampleTS     time.Time
	Org          string
	StreamID     string
	Gateway      string
	OrchAddress  string
	Pipeline     string
	State        string
	OutputFPS    float64
	InputFPS     float64
	E2ELatencyMS float64
	IsAttributed bool
}

// StreamDetail is the response for GET /v1/streams/{stream_id} (STR-EXT-002).
type StreamDetail struct {
	StreamID    string
	Org         string
	Pipeline    string
	Gateway     string
	OrchAddress string
	State       string
	StartedAt   time.Time
	LastSeen    time.Time
	IsClosed    bool
	HasFailure  bool
	Samples     []StreamStatusSample
}

// AttributionSummary is the response for GET /v1/streams/attribution (STR-EXT-003).
type AttributionSummary struct {
	StartTime         time.Time
	EndTime           time.Time
	TotalSamples      int64
	AttributedCount   int64
	UnattributedCount int64
	AttributionRate   float64
}
