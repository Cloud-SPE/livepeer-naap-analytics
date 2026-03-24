//go:build integration

// Package clickhouse integration tests run against a live ClickHouse instance.
// They verify that every repo method executes valid SQL and returns without error.
// No assertions are made on row counts — the test DB may be empty.
//
// Usage:
//
//	CLICKHOUSE_ADDR=localhost:9000 go test -tags=integration ./internal/repo/clickhouse/... -v
//
// The test is skipped automatically when CLICKHOUSE_ADDR is not set.
package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/types"
)

func newIntegrationRepo(t *testing.T) *Repo {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set — skipping integration test")
	}
	cfg := &config.Config{
		ClickHouseAddr:     addr,
		ClickHouseDB:       envOr("CLICKHOUSE_DB", "naap"),
		ClickHouseUser:     envOr("CLICKHOUSE_USER", "naap_reader"),
		ClickHousePassword: envOr("CLICKHOUSE_PASSWORD", "naap_reader_changeme"),
		ClickHouseTimeout:  30 * time.Second,
	}
	r, err := New(cfg)
	if err != nil {
		t.Skipf("ClickHouse not reachable at %s: %v — skipping integration test", addr, err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func defaultParams() types.QueryParams {
	return types.QueryParams{Limit: 10}
}

// ── Health ────────────────────────────────────────────────────────────────────

func TestIntegration_Ping(t *testing.T) {
	r := newIntegrationRepo(t)
	if err := r.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// ── Network ───────────────────────────────────────────────────────────────────

func TestIntegration_GetNetworkSummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetNetworkSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetNetworkSummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListOrchestrators(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListOrchestrators(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListOrchestrators: %v", err)
	}
}

func TestIntegration_GetGPUSummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetGPUSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetGPUSummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListModels(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListModels(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListModels: %v", err)
	}
}

// ── Streams ───────────────────────────────────────────────────────────────────

func TestIntegration_GetActiveStreams(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetActiveStreams(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetActiveStreams: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_GetStreamSummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetStreamSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetStreamSummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListStreamHistory(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListStreamHistory(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListStreamHistory: %v", err)
	}
}

// ── Performance ───────────────────────────────────────────────────────────────

func TestIntegration_GetFPSSummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetFPSSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetFPSSummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListFPSHistory(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListFPSHistory(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListFPSHistory: %v", err)
	}
}

func TestIntegration_GetLatencySummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetLatencySummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetLatencySummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_GetWebRTCQuality(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetWebRTCQuality(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetWebRTCQuality: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

// ── Payments ──────────────────────────────────────────────────────────────────

func TestIntegration_GetPaymentSummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetPaymentSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetPaymentSummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListPaymentHistory(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListPaymentHistory(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListPaymentHistory: %v", err)
	}
}

func TestIntegration_ListPaymentsByPipeline(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListPaymentsByPipeline(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListPaymentsByPipeline: %v", err)
	}
}

func TestIntegration_ListPaymentsByOrch(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListPaymentsByOrch(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListPaymentsByOrch: %v", err)
	}
}

// ── Reliability ───────────────────────────────────────────────────────────────

func TestIntegration_GetReliabilitySummary(t *testing.T) {
	r := newIntegrationRepo(t)
	got, err := r.GetReliabilitySummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("GetReliabilitySummary: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegration_ListReliabilityHistory(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListReliabilityHistory(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListReliabilityHistory: %v", err)
	}
}

func TestIntegration_ListOrchReliability(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListOrchReliability(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListOrchReliability: %v", err)
	}
}

func TestIntegration_ListFailures(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.ListFailures(context.Background(), defaultParams()); err != nil {
		t.Fatalf("ListFailures: %v", err)
	}
}

// ── Leaderboard ───────────────────────────────────────────────────────────────

func TestIntegration_GetLeaderboard(t *testing.T) {
	r := newIntegrationRepo(t)
	if _, err := r.GetLeaderboard(context.Background(), defaultParams()); err != nil {
		t.Fatalf("GetLeaderboard: %v", err)
	}
}
