// Package cursor owns the keyset pagination format shared by every
// list/paged endpoint. Phase 6.6 of serving-layer-v2 extracted these
// helpers out of repo/clickhouse/helpers.go into a standalone package so
// the encoding is one implementation with one set of tests, callable from
// either the repo layer (building the WHERE/ORDER keyset) or the runtime
// layer (validating params before dispatch).
//
// The cursor is a base64-url-encoded JSON array of strings. Time-scoped
// cursors encode the leading timestamp as Unix millis and follow it with
// one or more stable-key columns (e.g. orchestrator_address, pipeline_id,
// model_id) matching the target table's ORDER BY. A round-trip is:
//
//	cursor := cursor.EncodeTime(last.WindowStart, last.Orchestrator, last.Pipeline)
//	ts, keys, err := cursor.DecodeTime(cursor, 2) // 2 stable keys
package cursor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// EncodeValues encodes an arbitrary list of stable-key strings as a cursor.
// Callers that do not need a leading timestamp use this directly.
func EncodeValues(values ...string) string {
	raw, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeValues is the symmetric reader for EncodeValues. Returns
// (nil, nil) for an empty cursor string. Fails with types.ErrInvalidCursor
// on malformed input or an unexpected length.
func DecodeValues(cursor string, expected int) ([]string, error) {
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

// EncodeTime stamps a leading timestamp (Unix millis, UTC) followed by
// any number of stable-key strings. Every time-scoped paged endpoint
// encodes its cursor this way.
func EncodeTime(t time.Time, keys ...string) string {
	values := []string{strconv.FormatInt(t.UnixMilli(), 10)}
	values = append(values, keys...)
	return EncodeValues(values...)
}

// DecodeTime parses a cursor produced by EncodeTime. keyCount is the
// number of stable keys after the leading timestamp. Returns zero-value
// time.Time and nil keys for an empty cursor (first page).
func DecodeTime(cursor string, keyCount int) (time.Time, []string, error) {
	vals, err := DecodeValues(cursor, keyCount+1)
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
