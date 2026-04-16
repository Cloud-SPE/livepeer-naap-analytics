package clickhouse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

const (
	defaultLimit           = 50
	observedInventoryHours = 24
)

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

func defaultWindow(p types.TimeWindowParams) (start, end time.Time) {
	end = p.End
	if end.IsZero() {
		end = time.Now().UTC()
	}
	start = p.Start
	if start.IsZero() {
		start = end.Add(-24 * time.Hour)
	}
	return start.UTC(), end.UTC()
}

func divSafe(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func encodeCursorValues(values ...string) string {
	raw, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(raw)
}

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

func encodeTimeCursor(t time.Time, keys ...string) string {
	values := []string{strconv.FormatInt(t.UnixMilli(), 10)}
	values = append(values, keys...)
	return encodeCursorValues(values...)
}

func decodeTimeCursor(cursor string, keyCount int) (time.Time, []string, error) {
	vals, err := decodeCursorValues(cursor, keyCount+1)
	if err != nil {
		return time.Time{}, nil, err
	}
	if vals == nil {
		return time.Time{}, nil, nil
	}
	ms, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%w: bad timestamp", types.ErrInvalidCursor)
	}
	return time.UnixMilli(ms).UTC(), vals[1:], nil
}

func nullableString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// hostnameFromURI extracts the hostname from a URI string.
func hostnameFromURI(uri string) string {
	// Simple extraction — strip scheme and path
	s := uri
	for _, prefix := range []string{"https://", "http://"} {
		if len(s) > len(prefix) && s[:len(prefix)] == prefix {
			s = s[len(prefix):]
			break
		}
	}
	// Strip port and path
	for i := 0; i < len(s); i++ {
		if s[i] == ':' || s[i] == '/' {
			return s[:i]
		}
	}
	return s
}
