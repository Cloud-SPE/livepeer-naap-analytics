package types

// DiscoverOrchestratorRow is one (orchestrator, pipeline) row in the discovery list.
// Returned by GET /v1/discover/orchestrators.
type DiscoverOrchestratorRow struct {
	// Address is the orchestrator service URI (e.g., "https://orch.example.com:8935").
	Address string `json:"address"`
	// Score is a fitness metric in [0, 1] derived from the last 2-hour sla_score
	// for this row's pipeline. When RecentWork is false, Score is a placeholder
	// 0.0 (no measured data); when RecentWork is true, Score is meaningful.
	Score float64 `json:"score"`
	// Capabilities are "pipeline/model" strings for this row's single pipeline.
	Capabilities []string `json:"capabilities"`
	// LastSeenMs is the Unix millisecond timestamp of the most recent heartbeat.
	LastSeenMs int64 `json:"last_seen_ms"`
	// LastSeen is the ISO 8601 representation of the most recent heartbeat.
	LastSeen string `json:"last_seen"`
	// RecentWork is true when this (orch, pipeline) had SLA samples in the last
	// 2 hours (Score is a measured value). False when there's been no recent
	// activity for this pipeline on this orchestrator (Score is 0.0 placeholder).
	RecentWork bool `json:"recent_work"`
}

// DiscoverOrchestratorsParams filters the discover/orchestrators result.
type DiscoverOrchestratorsParams struct {
	// Caps is an optional list of "pipeline/model" strings. If non-empty,
	// only orchestrators advertising ALL listed caps are returned.
	Caps []string
}
