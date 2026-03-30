//go:build validation

// Package validation is a scenario-based data-quality test harness.
//
// Each test inserts synthetic inbound raw events through a local copy of the
// ingest-router contract. Accepted rows land in naap.accepted_raw_events and
// ignored rows land in naap.ignored_raw_events, then the test asserts on the
// canonical typed, fact, and serving tables/views rebuilt from those events.
// Tests are fully isolated via a unique per-run org tag, so they can run
// against a live cluster without polluting real data.
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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/resolver"
)

// rawEvent mirrors an inbound raw Kafka record after envelope extraction.
// The harness applies the same allowlist logic as the ingest boundary and
// routes the row into accepted or ignored raw storage.
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
	conn            driver.Conn
	resolverEngine  *resolver.Engine
	resolverInitErr error
	org             string // e.g. "vtest_3f2a1b8c" — namespaces all test rows
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
	cfg := &config.Config{
		ClickHouseAddr:           addr,
		ClickHouseDB:             envOrDefault("CLICKHOUSE_DB", "naap"),
		ClickHouseWriterUser:     envOrDefault("CLICKHOUSE_WRITER_USER", "naap_writer"),
		ClickHouseWriterPassword: envOrDefault("CLICKHOUSE_WRITER_PASSWORD", "naap_writer_changeme"),
		ResolverEnabled:          true,
		ResolverMode:             string(resolver.ModeRepairWindow),
		ResolverInterval:         time.Minute,
		ResolverLatenessWindow:   10 * time.Minute,
		ResolverDirtyQuietPeriod: 0,
		ResolverClaimTTL:         2 * time.Minute,
		ResolverPort:             "0",
		ResolverVersion:          "validation-selection-centered-v1",
		ResolverBatchSize:        10000,
	}
	resolverEngine, resolverErr := resolver.New(cfg, zap.NewNop())
	h.resolverEngine = resolverEngine
	h.resolverInitErr = resolverErr
	t.Cleanup(func() {
		if resolverEngine != nil {
			_ = resolverEngine.Close(context.Background())
		}
		_ = conn.Close()
	})
	return h
}

// insert routes a batch of inbound raw events into accepted or ignored raw
// storage. ClickHouse materialized views fire synchronously for accepted rows,
// so typed tables are populated by the time this function returns.
func (h *harness) insert(t *testing.T, events []rawEvent) {
	t.Helper()
	h.insertRaw(t, events)
	h.resolveInsertedBatch(t, events)
}

func (h *harness) insertRaw(t *testing.T, events []rawEvent) {
	t.Helper()
	if len(events) == 0 {
		return
	}
	minIngestedAt := events[0].IngestedAt
	maxIngestedAt := events[0].IngestedAt
	for _, e := range events[1:] {
		if e.IngestedAt.Before(minIngestedAt) {
			minIngestedAt = e.IngestedAt
		}
		if e.IngestedAt.After(maxIngestedAt) {
			maxIngestedAt = e.IngestedAt
		}
	}
	batchAnchor := time.Now().UTC().Add(-maxIngestedAt.Sub(minIngestedAt))
	type routedEvent struct {
		rawEvent
		eventSubtype string
		ignoreReason string
	}
	var accepted []routedEvent
	var ignored []routedEvent
	for _, e := range events {
		subtype := extractEventSubtype(e.Data)
		ignoreReason, accept := routeInboundEvent(e.EventType, subtype)
		routed := routedEvent{
			rawEvent:     e,
			eventSubtype: subtype,
			ignoreReason: ignoreReason,
		}
		if accept {
			accepted = append(accepted, routed)
		} else {
			ignored = append(ignored, routed)
		}
	}
	if len(accepted) > 0 {
		batch, err := h.conn.PrepareBatch(
			context.Background(),
			"INSERT INTO naap.accepted_raw_events (event_id, event_type, event_subtype, event_ts, org, gateway, data, source_topic, source_partition, source_offset, payload_hash, schema_version, ingested_at)",
		)
		if err != nil {
			t.Fatalf("PrepareBatch accepted_raw_events: %v", err)
		}
		for _, e := range accepted {
			shiftedIngestedAt := batchAnchor.Add(e.IngestedAt.Sub(minIngestedAt))
			if appendErr := batch.Append(
				e.EventID,
				e.EventType,
				e.eventSubtype,
				e.EventTs,
				e.Org,
				e.Gateway,
				e.Data,
				"validation",
				int32(0),
				int64(0),
				"",
				"",
				shiftedIngestedAt,
			); appendErr != nil {
				t.Fatalf("accepted batch.Append: %v", appendErr)
			}
		}
		if sendErr := batch.Send(); sendErr != nil {
			t.Fatalf("accepted batch.Send: %v", sendErr)
		}
	}
	if len(ignored) > 0 {
		batch, err := h.conn.PrepareBatch(
			context.Background(),
			"INSERT INTO naap.ignored_raw_events (event_id, event_type, event_subtype, event_ts, org, gateway, data, ignore_reason, source_topic, source_partition, source_offset, payload_hash, schema_version, ingested_at)",
		)
		if err != nil {
			t.Fatalf("PrepareBatch ignored_raw_events: %v", err)
		}
		for _, e := range ignored {
			shiftedIngestedAt := batchAnchor.Add(e.IngestedAt.Sub(minIngestedAt))
			if appendErr := batch.Append(
				e.EventID,
				e.EventType,
				e.eventSubtype,
				e.EventTs,
				e.Org,
				e.Gateway,
				e.Data,
				e.ignoreReason,
				"validation",
				int32(0),
				int64(0),
				"",
				"",
				shiftedIngestedAt,
			); appendErr != nil {
				t.Fatalf("ignored batch.Append: %v", appendErr)
			}
		}
		if sendErr := batch.Send(); sendErr != nil {
			t.Fatalf("ignored batch.Send: %v", sendErr)
		}
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

// queryString runs a single-column String query and returns the value.
func (h *harness) queryString(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var s string
	if err := h.conn.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("queryString %q: %v", sql, err)
	}
	return s
}

// queryFloat runs a single-column Float64 query and returns the value.
func (h *harness) queryFloat(t *testing.T, sql string, args ...any) float64 {
	t.Helper()
	var f float64
	if err := h.conn.QueryRow(context.Background(), sql, args...).Scan(&f); err != nil {
		t.Fatalf("queryFloat %q: %v", sql, err)
	}
	return f
}

func canonicalSessionKey(org, streamID, requestID string) string {
	switch {
	case org == "":
		return ""
	case streamID != "" && requestID != "":
		return fmt.Sprintf("%s|%s|%s", org, streamID, requestID)
	case streamID != "":
		return fmt.Sprintf("%s|%s|_missing_request", org, streamID)
	case requestID != "":
		return fmt.Sprintf("%s|_missing_stream|%s", org, requestID)
	default:
		return ""
	}
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

var acceptedFamilies = map[string]struct{}{
	"stream_trace":          {},
	"ai_stream_status":      {},
	"ai_stream_events":      {},
	"stream_ingest_metrics": {},
	"network_capabilities":  {},
	"discovery_results":     {},
	"create_new_payment":    {},
}

var acceptedStreamTraceSubtypes = map[string]struct{}{
	"gateway_receive_stream_request":          {},
	"gateway_ingest_stream_closed":            {},
	"gateway_send_first_ingest_segment":       {},
	"gateway_receive_first_processed_segment": {},
	"gateway_receive_few_processed_segments":  {},
	"gateway_receive_first_data_segment":      {},
	"gateway_no_orchestrators_available":      {},
	"orchestrator_swap":                       {},
	"runner_receive_first_ingest_segment":     {},
	"runner_send_first_processed_segment":     {},
}

var ignoredNonCoreStreamTraceSubtypes = map[string]struct{}{
	"app_capacity_query_response": {},
	"app_user_page_unload":        {},
	"app_start_broadcast_stream":  {},
	"app_send_stream_request":     {},
	"app_param_update":            {},
	"app_receive_first_segment":   {},
}

var ignoredScopeClientStreamTraceSubtypes = map[string]struct{}{
	"stream_heartbeat":       {},
	"pipeline_load_start":    {},
	"pipeline_loaded":        {},
	"pipeline_unloaded":      {},
	"session_created":        {},
	"session_closed":         {},
	"stream_started":         {},
	"stream_stopped":         {},
	"playback_ready":         {},
	"websocket_connected":    {},
	"websocket_disconnected": {},
}

func extractEventSubtype(data string) string {
	if data == "" {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return ""
	}
	subtype, _ := payload["type"].(string)
	return subtype
}

func routeInboundEvent(eventType, eventSubtype string) (ignoreReason string, accepted bool) {
	if _, ok := acceptedFamilies[eventType]; !ok {
		return "unsupported_event_family", false
	}
	if eventType != "stream_trace" {
		return "", true
	}
	if _, ok := acceptedStreamTraceSubtypes[eventSubtype]; ok {
		return "", true
	}
	if _, ok := ignoredNonCoreStreamTraceSubtypes[eventSubtype]; ok {
		return "ignored_stream_trace_non_core_app", false
	}
	if _, ok := ignoredScopeClientStreamTraceSubtypes[eventSubtype]; ok {
		return "ignored_stream_trace_scope_client_noise", false
	}
	if eventSubtype == "" {
		return "malformed_or_invalid_payload", false
	}
	return "unsupported_stream_trace_type", false
}

func (h *harness) resolveSelectionCentered(t *testing.T, start, end time.Time) resolver.RunStats {
	t.Helper()
	if h.resolverInitErr != nil {
		t.Fatalf("resolver init failed: %v", h.resolverInitErr)
	}
	if h.resolverEngine == nil {
		t.Fatalf("resolver engine not initialized")
	}
	h.waitForResolverWarehouseReady(t)
	stats, err := h.resolverEngine.Execute(context.Background(), resolver.RunRequest{
		Mode:  resolver.ModeRepairWindow,
		Org:   h.org,
		Start: timePointer(start.UTC()),
		End:   timePointer(end.UTC()),
	})
	if err != nil {
		t.Fatalf("selection-centered resolver: %v", err)
	}
	return stats
}

func (h *harness) resolveAuto(t *testing.T) resolver.RunStats {
	t.Helper()
	if h.resolverInitErr != nil {
		t.Fatalf("resolver init failed: %v", h.resolverInitErr)
	}
	if h.resolverEngine == nil {
		t.Fatalf("resolver engine not initialized")
	}
	h.waitForResolverWarehouseReady(t)
	stats, err := h.resolverEngine.Execute(context.Background(), resolver.RunRequest{
		Mode: resolver.ModeAuto,
		Org:  h.org,
	})
	if err != nil {
		t.Fatalf("resolver auto: %v", err)
	}
	return stats
}

func (h *harness) waitForWarehouseReady(t *testing.T) {
	t.Helper()
	required := []string{
		"stg_stream_trace",
		"stg_ai_stream_status",
		"stg_ai_stream_events",
		"canonical_capability_snapshots",
		"canonical_capability_hardware_inventory",
		"canonical_latest_orchestrator_pipeline_inventory_agg",
		"canonical_session_attribution_latest",
		"canonical_session_latest",
		"canonical_status_hours",
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var available uint64
		err := h.conn.QueryRow(
			context.Background(),
			`SELECT count()
			 FROM system.tables
			 WHERE database = ?
			   AND table IN ?`,
			envOrDefault("CLICKHOUSE_DB", "naap"),
			required,
		).Scan(&available)
		if err == nil && available == uint64(len(required)) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	var present []string
	rows, err := h.conn.Query(
		context.Background(),
		`SELECT table
		 FROM system.tables
		 WHERE database = ?
		   AND table IN ?
		 ORDER BY table`,
		envOrDefault("CLICKHOUSE_DB", "naap"),
		required,
	)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var table string
			if scanErr := rows.Scan(&table); scanErr == nil {
				present = append(present, table)
			}
		}
	}
	t.Fatalf("validation warehouse not ready; present tables: %s", strings.Join(present, ", "))
}

func (h *harness) waitForResolverWarehouseReady(t *testing.T) {
	t.Helper()
	required := []string{
		"accepted_raw_events",
		"ignored_raw_events",
		"normalized_stream_trace",
		"normalized_ai_stream_status",
		"normalized_ai_stream_events",
		"normalized_network_capabilities",
		"canonical_selection_events",
		"canonical_selection_attribution_current",
		"canonical_session_current_store",
		"canonical_status_hours_store",
		"canonical_session_demand_input_current",
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var available uint64
		err := h.conn.QueryRow(
			context.Background(),
			`SELECT count()
			 FROM system.tables
			 WHERE database = ?
			   AND table IN ?`,
			envOrDefault("CLICKHOUSE_DB", "naap"),
			required,
		).Scan(&available)
		if err == nil && available == uint64(len(required)) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	var present []string
	rows, err := h.conn.Query(
		context.Background(),
		`SELECT table
		 FROM system.tables
		 WHERE database = ?
		   AND table IN ?
		 ORDER BY table`,
		envOrDefault("CLICKHOUSE_DB", "naap"),
		required,
	)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var table string
			if scanErr := rows.Scan(&table); scanErr == nil {
				present = append(present, table)
			}
		}
	}
	t.Fatalf("selection-centered warehouse not ready; present tables: %s", strings.Join(present, ", "))
}

func timePointer(ts time.Time) *time.Time {
	return &ts
}

func (h *harness) resolveInsertedBatch(t *testing.T, events []rawEvent) {
	t.Helper()
	if len(events) == 0 {
		return
	}
	minTS := events[0].EventTs.UTC()
	maxTS := events[0].EventTs.UTC()
	for _, event := range events[1:] {
		if event.EventTs.Before(minTS) {
			minTS = event.EventTs.UTC()
		}
		if event.EventTs.After(maxTS) {
			maxTS = event.EventTs.UTC()
		}
	}
	// Mirror resolver attribution windows so validation inserts exercise the
	// same repair semantics as bounded repair-window runs.
	start := minTS.Add(-10 * time.Minute)
	end := maxTS.Add(time.Minute)
	h.resolveSelectionCentered(t, start, end)
}
