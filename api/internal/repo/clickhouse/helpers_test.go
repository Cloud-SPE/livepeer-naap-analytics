package clickhouse

import (
	"errors"
	"testing"

	"github.com/livepeer/naap-analytics/internal/types"
)

func TestEncodeDecodeCursorValues_RoundTrip(t *testing.T) {
	cursor := encodeCursorValues("2026-04-09T12:00:00Z", "0xabc123", "pipeline-a")
	values, err := decodeCursorValues(cursor, 3)
	if err != nil {
		t.Fatalf("decodeCursorValues: %v", err)
	}

	want := []string{"2026-04-09T12:00:00Z", "0xabc123", "pipeline-a"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("value %d: got %q want %q", i, values[i], want[i])
		}
	}
}

func TestDecodeCursorValues_InvalidCursor(t *testing.T) {
	_, err := decodeCursorValues("%%%not-base64%%%", 2)
	if !errors.Is(err, types.ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestDecodeCursorValues_UnexpectedTupleLength(t *testing.T) {
	cursor := encodeCursorValues("only-one")
	_, err := decodeCursorValues(cursor, 2)
	if !errors.Is(err, types.ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}
