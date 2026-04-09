package clickhouse

import (
	"encoding/base64"
	"fmt"
	"strings"
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
	if p.Limit <= 0 {
		return defaultLimit
	}
	return p.Limit
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

// encodeCursor encodes a (timestamp, id) pair as a base64 stable cursor.
// Format: "<unix_nano>|<id>".
func encodeCursor(ts time.Time, id string) string {
	raw := fmt.Sprintf("%d|%s", ts.UnixNano(), id)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// decodeCursor decodes a cursor produced by encodeCursor.
// Returns zero time and empty string on any parse failure (caller treats as no cursor).
func decodeCursor(cursor string) (time.Time, string) {
	if cursor == "" {
		return time.Time{}, ""
	}
	b, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, ""
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, ""
	}
	var ns int64
	if _, err := fmt.Sscanf(parts[0], "%d", &ns); err != nil {
		return time.Time{}, ""
	}
	return time.Unix(0, ns).UTC(), parts[1]
}
