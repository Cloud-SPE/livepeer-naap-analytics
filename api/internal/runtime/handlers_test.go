package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func assertCursorEnvelope(t *testing.T, body map[string]any) {
	t.Helper()
	for _, field := range []string{"data", "pagination", "meta"} {
		if _, ok := body[field]; !ok {
			t.Fatalf("missing field %s in cursor envelope", field)
		}
	}
	pag, ok := body["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("pagination is not an object")
	}
	if _, ok := pag["has_more"]; !ok {
		t.Fatal("pagination missing has_more")
	}
	if _, ok := pag["page_size"]; !ok {
		t.Fatal("pagination missing page_size")
	}
}

func assertProblemDetail(t *testing.T, rr *httptest.ResponseRecorder, expectedDetail string) {
	t.Helper()
	if got := rr.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("expected problem content type, got %q", got)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode problem body: %v", err)
	}
	detail, _ := body["detail"].(string)
	if !strings.Contains(detail, expectedDetail) {
		t.Fatalf("expected detail containing %q, got %q", expectedDetail, detail)
	}
}

// ---------------------------------------------------------------------------
// Dashboard tests
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
// Streaming tests
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
	body := assertJSON(t, rr)
	assertCursorEnvelope(t, body)
}

func TestStreamingSLA_RejectsLegacyPagination(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/sla?limit=10&offset=0", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `"offset" is no longer supported`)
}

func TestStreamingDemand_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/demand", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	assertCursorEnvelope(t, body)
}

func TestStreamingGPUMetrics_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streaming/gpu-metrics", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSON(t, rr)
	assertCursorEnvelope(t, body)
}

// ---------------------------------------------------------------------------
// Requests tests
// ---------------------------------------------------------------------------

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
	body := assertJSON(t, rr)
	assertCursorEnvelope(t, body)
}

func TestAIBatchJobs_RejectsLegacyPagination(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs?page=1", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `"page" is no longer supported`)
}

func TestAIBatchJobs_InvalidCursor(t *testing.T) {
	r := &invalidCursorAIBatchRepo{}
	srv := newTestServerWithRepo(t, r)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs?cursor=bad", nil)
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
	body := assertJSON(t, rr)
	assertCursorEnvelope(t, body)
}

func TestBYOCJobs_RejectsLegacyPagination(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/byoc/jobs?page_size=50", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `"page_size" is no longer supported`)
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

// ---------------------------------------------------------------------------
// 404 test
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

// ---------------------------------------------------------------------------
// Custom mock repos for error testing
// ---------------------------------------------------------------------------

type invalidCursorAIBatchRepo struct {
	repo.NoopAnalyticsRepo
}

func (r *invalidCursorAIBatchRepo) ListAIBatchJobs(_ context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.AIBatchJobRecord{}, types.CursorPageInfo{}, nil
}
