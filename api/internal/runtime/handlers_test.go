package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/providers"
	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/runtime"
	"github.com/livepeer/naap-analytics/internal/service"
	"github.com/livepeer/naap-analytics/internal/types"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newTestServer(tb testing.TB) *runtime.Server {
	tb.Helper()
	return newTestServerWithRepo(tb, &repo.NoopAnalyticsRepo{})
}

func newTestServerWithRepo(tb testing.TB, analyticsRepo repo.AnalyticsRepo) *runtime.Server {
	tb.Helper()
	cfg := &config.Config{Port: "8000", Env: "development", LogLevel: "debug"}
	p, err := providers.New(cfg)
	if err != nil {
		tb.Fatalf("init providers: %v", err)
	}
	tb.Cleanup(func() { p.Close(context.Background()) })
	return runtime.New(cfg, p, service.New(analyticsRepo))
}

func assertJSON(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON object: %v", err)
	}
	return body
}

func assertJSONArray(t *testing.T, rr *httptest.ResponseRecorder) []any {
	t.Helper()
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
	var body []any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON array: %v", err)
	}
	return body
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func TestHealthz_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body["status"])
	}
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

func TestDashboardKPI_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/kpi", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	if _, ok := body["streaming"]; !ok {
		t.Fatal("missing streaming field")
	}
	if _, ok := body["requests"]; !ok {
		t.Fatal("missing requests field")
	}
}

func TestDashboardPipelines_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/pipelines?limit=3", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	if _, ok := body["streaming"]; !ok {
		t.Fatal("missing streaming field")
	}
	if _, ok := body["requests"]; !ok {
		t.Fatal("missing requests field")
	}
}

func TestDashboardOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestDashboardGPUCapacity_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/gpu-capacity", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	if _, ok := body["totalGPUs"]; !ok {
		t.Fatal("missing totalGPUs field")
	}
}

func TestDashboardPipelineCatalog_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/pipeline-catalog", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestDashboardPricing_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/pricing", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestDashboardJobFeed_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/job-feed", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

// ---------------------------------------------------------------------------
// Streaming + Requests model inventories
// ---------------------------------------------------------------------------

func TestStreamingModels_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/models", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestRequestsModels_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/models", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestStreamingOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestStreamingSLA_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/sla", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertCursorEnvelope(t, rr)
}

func TestStreamingDemand_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/demand", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertCursorEnvelope(t, rr)
}

func TestStreamingGPUMetrics_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/gpu-metrics", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertCursorEnvelope(t, rr)
}

func TestRequestsOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestAIBatchSummary_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/summary", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestAIBatchJobs_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertCursorEnvelope(t, rr)
}

func TestAIBatchJobs_RejectsLegacyPagination(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs?offset=10", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestAIBatchLLMSummary_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/llm-summary", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestBYOCSummary_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/byoc/summary", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestBYOCJobs_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/byoc/jobs", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertCursorEnvelope(t, rr)
}

func TestBYOCWorkers_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/byoc/workers", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestBYOCAuth_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/byoc/auth", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func assertCursorEnvelope(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	body := assertJSON(t, rr)
	if _, ok := body["data"]; !ok {
		t.Fatalf("cursor envelope missing 'data': %v", body)
	}
	if _, ok := body["pagination"]; !ok {
		t.Fatalf("cursor envelope missing 'pagination': %v", body)
	}
	if _, ok := body["meta"]; !ok {
		t.Fatalf("cursor envelope missing 'meta': %v", body)
	}
}

// ---------------------------------------------------------------------------
// Discover
// ---------------------------------------------------------------------------

func TestDiscoverOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/discover/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

func TestDiscoverOrchestrators_WithCapsFilter(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet,
		"/v1/discover/orchestrators?caps=live-video-to-video/streamdiffusion-sdxl&caps=llm/glm-4.7-flash",
		nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	assertJSONArray(t, rr)
}

// TestDiscoverOrchestrators_MultipleRowsPerAddress verifies that the response
// can carry multiple rows sharing the same orchestrator address (one per
// pipeline). Uses a custom repo that returns two rows for the same address
// but different pipelines.
func TestDiscoverOrchestrators_MultipleRowsPerAddress(t *testing.T) {
	r := &multiRowDiscoverRepo{}
	srv := newTestServerWithRepo(t, r)
	req := httptest.NewRequest(http.MethodGet, "/v1/discover/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSONArray(t, rr)
	if len(body) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(body))
	}
	row0 := body[0].(map[string]any)
	row1 := body[1].(map[string]any)
	if row0["address"] != row1["address"] {
		t.Fatalf("expected both rows to share the same address, got %v vs %v",
			row0["address"], row1["address"])
	}
}

type multiRowDiscoverRepo struct {
	repo.NoopAnalyticsRepo
}

func (r *multiRowDiscoverRepo) DiscoverOrchestrators(_ context.Context, _ types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	return []types.DiscoverOrchestratorRow{
		{
			Address:      "https://orch.example.com:8935",
			Score:        0.95,
			Capabilities: []string{"live-video-to-video/streamdiffusion-sdxl"},
			LastSeenMs:   1776164007740,
			LastSeen:     "2026-04-14T10:53:27.740Z",
			RecentWork:   true,
		},
		{
			Address:      "https://orch.example.com:8935",
			Score:        0.88,
			Capabilities: []string{"llm/meta-llama/Meta-Llama-3.1-8B-Instruct"},
			LastSeenMs:   1776164007740,
			LastSeen:     "2026-04-14T10:53:27.740Z",
			RecentWork:   true,
		},
	}, nil
}

// ---------------------------------------------------------------------------
// 404
// ---------------------------------------------------------------------------

func TestUnknownRoute_Returns404(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/nonexistent", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}
