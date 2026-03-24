// Package types defines domain types for the NAAP Analytics API.
// This is layer 1 — no imports from other internal packages.
package types

import "time"

// EventType represents the kind of network event.
type EventType string

const (
	EventTypeStreamStarted       EventType = "stream_started"
	EventTypeStreamEnded         EventType = "stream_ended"
	EventTypeTranscodeCompleted  EventType = "transcode_completed"
	EventTypeTranscodeFailed     EventType = "transcode_failed"
)

// RawEvent is a network event as received from Kafka.
type RawEvent struct {
	EventID   string            `json:"event_id"`
	EventType EventType         `json:"event_type"`
	NodeID    string            `json:"node_id"`
	StreamID  string            `json:"stream_id"`
	Timestamp time.Time         `json:"timestamp"`
	Payload   map[string]any    `json:"payload"`
}

// AggregatedWindow is a time-windowed analytics aggregate.
type AggregatedWindow struct {
	WindowStart          time.Time `json:"window_start"`
	WindowEnd            time.Time `json:"window_end"`
	NodeID               string    `json:"node_id"`
	StreamCount          int64     `json:"stream_count"`
	TranscodeSuccessRate float64   `json:"transcode_success_rate"`
	TotalDurationSeconds float64   `json:"total_duration_seconds"`
}

// Alert is a threshold breach notification.
type Alert struct {
	AlertID       string    `json:"alert_id"`
	AlertType     string    `json:"alert_type"`
	NodeID        string    `json:"node_id"`
	TriggeredAt   time.Time `json:"triggered_at"`
	Threshold     float64   `json:"threshold"`
	ObservedValue float64   `json:"observed_value"`
}

// QueryParams holds common query parameters for analytics endpoints.
type QueryParams struct {
	NodeID    string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
}
