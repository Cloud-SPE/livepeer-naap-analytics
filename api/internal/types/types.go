// Package types defines domain types for the NAAP Analytics API v2.
// This is layer 1 — no imports from other internal packages.
package types

import (
	"errors"
	"time"
)

// ErrInvalidCursor is returned when a cursor token is malformed or incompatible
// with the endpoint-specific keyset pagination contract.
var ErrInvalidCursor = errors.New("invalid cursor")

// CursorPageInfo is the pagination metadata returned by cursor-paginated list endpoints.
type CursorPageInfo struct {
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
	PageSize   int    `json:"page_size"`
}

// TimeWindowParams holds start/end time window parameters used by paginated endpoints.
type TimeWindowParams struct {
	Start  time.Time
	End    time.Time
	Limit  int
	Cursor string
}
