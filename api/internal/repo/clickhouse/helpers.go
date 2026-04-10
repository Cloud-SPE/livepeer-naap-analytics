package clickhouse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

const (
	activeOrchMinutes     = 10
	activeStreamSecs      = 120
	minReliabilitySamples = 5
	defaultLimit          = 50
	maxFailureLimit       = 500
)

// effectiveWindow returns the query time window, applying defaults when zero.
// Default window is the last 24 hours.
func effectiveWindow(p types.QueryParams) (start, end time.Time) {
	end = p.EndTime
	if end.IsZero() {
		end = time.Now().UTC()
	}
	start = p.StartTime
	if start.IsZero() {
		start = end.Add(-24 * time.Hour)
	}
	return start.UTC(), end.UTC()
}

// effectiveLimit returns the query limit, applying the default when zero.
func effectiveLimit(p types.QueryParams) int {
	return normalizeLimit(p.Limit)
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

// divSafe divides a by b, returning 0 if b is zero.
func divSafe(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// rateOrNil returns nil when the sample count is below the minimum threshold,
// preventing misleading percentages from tiny samples (REL-002-a).
func rateOrNil(num, denom int64) *float64 {
	if denom < minReliabilitySamples {
		return nil
	}
	v := float64(num) / float64(denom)
	return &v
}

func activeStreamPredicate(column string) string {
	return fmt.Sprintf("%s > now() - INTERVAL %d SECOND", column, activeStreamSecs)
}

// encodeCursorValues encodes an ordered list of string values as an opaque cursor.
func encodeCursorValues(values ...string) string {
	raw, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursorValues decodes a cursor produced by encodeCursorValues.
func decodeCursorValues(cursor string, expected int) ([]string, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: decode token", types.ErrInvalidCursor)
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("%w: decode payload", types.ErrInvalidCursor)
	}
	if len(values) != expected {
		return nil, fmt.Errorf("%w: expected %d values, got %d", types.ErrInvalidCursor, expected, len(values))
	}
	return values, nil
}
