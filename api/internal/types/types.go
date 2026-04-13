// Package types defines domain types for the NAAP Analytics API.
// This is layer 1 — no imports from other internal packages.
package types

import (
	"errors"
	"strconv"
	"time"
)

// QueryParams holds common query parameters passed from the runtime to the repo.
// Zero values signal "use default": empty Org means all orgs, zero Start/End means
// the service layer applies a default window (typically last 24 hours).
type QueryParams struct {
	Org         string    // "daydream" | "cloudspe" | "" for all
	Pipeline    string    // filter by pipeline name
	ModelID     string    // filter by model ID (e.g. "streamdiffusion-sdxl")
	OrchAddress string    // filter by orch ETH address (normalised lowercase)
	StreamID    string    // filter by stream ID
	FailureType string    // "no_orch_available" | "orch_swap" | "inference_restart" | "inference_error"
	JobType     string    // "stream" | "ai-batch" | "byoc" | "" for all
	StartTime   time.Time // time window start (zero = service-layer default)
	EndTime     time.Time // time window end (zero = now)
	Granularity string    // "1m" | "5m" | "1h" | "1d"
	ActiveOnly  bool      // for orch list: return only orchs active within threshold
	Limit       int       // max results (0 = default 50)
	Offset      int       // pagination offset (legacy — prefer Cursor for public list endpoints)
	Cursor      string    // stable base64 cursor for keyset pagination (optional)
}

// ErrInvalidCursor is returned when a cursor token is malformed or incompatible
// with the endpoint-specific keyset pagination contract.
var ErrInvalidCursor = errors.New("invalid cursor")

// CursorPageInfo is the pagination metadata returned by cursor-paginated list endpoints.
type CursorPageInfo struct {
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
	PageSize   int    `json:"page_size"`
}

// WEI is a value in wei (1e-18 ETH).
// Marshalled as a decimal string in JSON to prevent float64 precision loss.
// See PAY-001-a for the requirement.
type WEI uint64

func (w WEI) MarshalJSON() ([]byte, error) {
	s := `"` + strconv.FormatUint(uint64(w), 10) + `"`
	return []byte(s), nil
}

// ToETH converts WEI to ETH as a float64 (for display only, not storage).
func (w WEI) ToETH() float64 {
	return float64(w) / 1e18
}
