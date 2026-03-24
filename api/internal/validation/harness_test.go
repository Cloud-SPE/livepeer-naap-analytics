//go:build validation

// Package validation is a scenario-based data-quality test harness.
//
// Each test inserts synthetic raw events directly into naap.events, then
// asserts on the aggregate tables that ClickHouse materialized views
// populate automatically. Tests are fully isolated via a unique per-run org
// tag, so they can run against a live cluster without polluting real data.
//
// Usage:
//
//	CLICKHOUSE_ADDR=localhost:9000 \
//	CLICKHOUSE_WRITER_PASSWORD=naap_writer_changeme \
//	go test -tags=validation ./internal/validation/... -v -timeout=60s
//
// The test is skipped automatically when CLICKHOUSE_ADDR is not set.
package validation

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// rawEvent mirrors a row in naap.events.
// Tests insert directly into this table; ClickHouse MVs fire synchronously
// and populate all aggregate tables before Insert returns.
type rawEvent struct {
	EventID    string
	EventType  string
	EventTs    time.Time
	Org        string
	Gateway    string
	Data       string // raw JSON string
	IngestedAt time.Time
}

// harness holds a ClickHouse connection and the unique org tag for one test run.
type harness struct {
	conn driver.Conn
	org  string // e.g. "vtest_3f2a1b8c" — namespaces all test rows
}

// newHarness opens a ClickHouse writer connection and returns a harness.
// Tests are skipped (not failed) when CLICKHOUSE_ADDR is unset or unreachable.
func newHarness(t *testing.T) *harness {
	t.Helper()

	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set — skipping validation test")
	}

	conn, err := ch.Open(&ch.Options{
		Addr: []string{addr},
		Auth: ch.Auth{
			Database: envOrDefault("CLICKHOUSE_DB", "naap"),
			Username: envOrDefault("CLICKHOUSE_WRITER_USER", "naap_writer"),
			Password: envOrDefault("CLICKHOUSE_WRITER_PASSWORD", "naap_writer_changeme"),
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("ClickHouse open failed: %v", err)
	}
	if pingErr := conn.Ping(context.Background()); pingErr != nil {
		_ = conn.Close()
		t.Skipf("ClickHouse ping failed: %v", pingErr)
	}

	h := &harness{
		conn: conn,
		org:  fmt.Sprintf("vtest_%08x", rand.Uint32()),
	}
	t.Cleanup(func() { _ = conn.Close() })
	return h
}

// insert writes a batch of raw events into naap.events.
// ClickHouse materialized views fire synchronously, so aggregate tables
// are populated by the time this function returns.
func (h *harness) insert(t *testing.T, events []rawEvent) {
	t.Helper()
	batch, err := h.conn.PrepareBatch(
		context.Background(),
		"INSERT INTO naap.events (event_id, event_type, event_ts, org, gateway, data, ingested_at)",
	)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	for _, e := range events {
		if appendErr := batch.Append(
			e.EventID, e.EventType, e.EventTs, e.Org, e.Gateway, e.Data, e.IngestedAt,
		); appendErr != nil {
			t.Fatalf("batch.Append: %v", appendErr)
		}
	}
	if sendErr := batch.Send(); sendErr != nil {
		t.Fatalf("batch.Send: %v", sendErr)
	}
}

// queryInt runs a single-column UInt64 query and returns the value.
func (h *harness) queryInt(t *testing.T, sql string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := h.conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("queryInt %q: %v", sql, err)
	}
	return n
}

// uid generates a short unique string suitable for IDs and addresses.
func uid(prefix string) string {
	return fmt.Sprintf("%s_%08x", prefix, rand.Uint32())
}

// anchor returns a fixed time one hour in the past (so it falls into a
// completed hourly bucket and is not filtered by "recent only" guards).
func anchor() time.Time {
	return time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
