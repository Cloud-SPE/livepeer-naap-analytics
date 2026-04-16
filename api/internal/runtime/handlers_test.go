package runtime_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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

func TestRequestsOrchestrators_UsesPipelineColonModelCapabilityStrings(t *testing.T) {
	srv := newTestServerWithRepo(t, &requestsOrchestratorsRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/requests/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := assertJSONArray(t, rr)
	if len(body) != 1 {
		t.Fatalf("expected 1 row, got %d", len(body))
	}
	row := body[0].(map[string]any)
	caps, ok := row["capabilities"].([]any)
	if !ok || len(caps) != 2 {
		t.Fatalf("expected 2 capabilities, got %v", row["capabilities"])
	}
	if caps[0] != "llm:meta-llama/Meta-Llama-3.1-8B-Instruct" {
		t.Fatalf("unexpected first capability: %v", caps[0])
	}
	if caps[1] != "openai-chat-completions:gpt-4.1" {
		t.Fatalf("unexpected second capability: %v", caps[1])
	}
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

func TestAIBatchJobs_PaginatesAcrossSameTimestampWithNextCursor(t *testing.T) {
	srv := newTestServerWithRepo(t, &sameTimestampAIBatchJobsRepo{})

	req1 := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs", nil)
	rr1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected page 1 status 200, got %d", rr1.Code)
	}
	body1 := assertJSON(t, rr1)
	data1, ok := body1["data"].([]any)
	if !ok || len(data1) != 1 {
		t.Fatalf("expected 1 row on page 1, got %v", body1["data"])
	}
	row1 := data1[0].(map[string]any)
	if row1["request_id"] != "req-b" {
		t.Fatalf("expected first page to return req-b, got %v", row1["request_id"])
	}
	pagination1, ok := body1["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("expected pagination object, got %v", body1["pagination"])
	}
	nextCursor, _ := pagination1["next_cursor"].(string)
	if nextCursor == "" {
		t.Fatal("expected non-empty next_cursor on page 1")
	}
	cursorTime, cursorKeys := decodeCursor(t, nextCursor)
	if !cursorTime.Equal(time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected cursor timestamp: %v", cursorTime)
	}
	if len(cursorKeys) != 1 || cursorKeys[0] != "req-b" {
		t.Fatalf("unexpected cursor keys: %v", cursorKeys)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/requests/ai-batch/jobs?cursor="+nextCursor, nil)
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected page 2 status 200, got %d", rr2.Code)
	}
	body2 := assertJSON(t, rr2)
	data2, ok := body2["data"].([]any)
	if !ok || len(data2) != 1 {
		t.Fatalf("expected 1 row on page 2, got %v", body2["data"])
	}
	row2 := data2[0].(map[string]any)
	if row2["request_id"] != "req-a" {
		t.Fatalf("expected second page to return req-a, got %v", row2["request_id"])
	}
	pagination2, ok := body2["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("expected pagination object on page 2, got %v", body2["pagination"])
	}
	if next, _ := pagination2["next_cursor"].(string); next != "" {
		t.Fatalf("expected final page next_cursor to be empty, got %q", next)
	}
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

func TestPaginatedRoutes_RejectMalformedCursor(t *testing.T) {
	srv := newTestServerWithRepo(t, &invalidCursorRepo{})
	paths := []string{
		"/v1/streaming/sla?cursor=not-base64",
		"/v1/streaming/demand?cursor=not-base64",
		"/v1/streaming/gpu-metrics?cursor=not-base64",
		"/v1/requests/ai-batch/jobs?cursor=not-base64",
		"/v1/requests/byoc/jobs?cursor=not-base64",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}
		})
	}
}

func TestPaginatedRoutes_RejectLegacyPagination(t *testing.T) {
	srv := newTestServer(t)
	paths := []string{
		"/v1/streaming/sla?page=2",
		"/v1/streaming/demand?offset=10",
		"/v1/streaming/gpu-metrics?page_size=5",
		"/v1/requests/ai-batch/jobs?offset=10",
		"/v1/requests/byoc/jobs?page=2",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}
		})
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

type invalidCursorRepo struct {
	repo.NoopAnalyticsRepo
}

type requestsOrchestratorsRepo struct {
	repo.NoopAnalyticsRepo
}

type sameTimestampAIBatchJobsRepo struct {
	repo.NoopAnalyticsRepo
}

func (r *invalidCursorRepo) ListStreamingSLA(_ context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, string, error) {
	if p.Cursor != "" {
		return nil, "", types.ErrInvalidCursor
	}
	return []types.StreamingSLARow{}, "", nil
}

func (r *invalidCursorRepo) ListStreamingDemand(_ context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, string, error) {
	if p.Cursor != "" {
		return nil, "", types.ErrInvalidCursor
	}
	return []types.StreamingDemandRow{}, "", nil
}

func (r *invalidCursorRepo) ListStreamingGPUMetrics(_ context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, string, error) {
	if p.Cursor != "" {
		return nil, "", types.ErrInvalidCursor
	}
	return []types.StreamingGPUMetricRow{}, "", nil
}

func (r *invalidCursorRepo) ListAIBatchJobs(_ context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, string, error) {
	if p.Cursor != "" {
		return nil, "", types.ErrInvalidCursor
	}
	return []types.AIBatchJobRecord{}, "", nil
}

func (r *invalidCursorRepo) ListBYOCJobs(_ context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, string, error) {
	if p.Cursor != "" {
		return nil, "", types.ErrInvalidCursor
	}
	return []types.BYOCJobRecord{}, "", nil
}

func (r *requestsOrchestratorsRepo) GetRequestsOrchestrators(_ context.Context) ([]types.RequestsOrchestrator, error) {
	return []types.RequestsOrchestrator{
		{
			Address: "0xorch",
			URI:     "https://orch.example.com:8935",
			Capabilities: []string{
				"llm:meta-llama/Meta-Llama-3.1-8B-Instruct",
				"openai-chat-completions:gpt-4.1",
			},
			GPUCount: 2,
			LastSeen: time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC),
		},
	}, nil
}

func (r *sameTimestampAIBatchJobsRepo) ListAIBatchJobs(_ context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, string, error) {
	ts := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	if p.Cursor == "" {
		return []types.AIBatchJobRecord{
			{RequestID: "req-b", CompletedAt: ts},
		}, encodeCursor(ts, "req-b"), nil
	}
	cursorTime, cursorKeys, err := decodeCursorValue(p.Cursor)
	if err != nil {
		return nil, "", types.ErrInvalidCursor
	}
	if cursorTime.Equal(ts) && len(cursorKeys) == 1 && cursorKeys[0] == "req-b" {
		return []types.AIBatchJobRecord{
			{RequestID: "req-a", CompletedAt: ts},
		}, "", nil
	}
	return nil, "", types.ErrInvalidCursor
}

func encodeCursor(ts time.Time, keys ...string) string {
	values := []string{strconv.FormatInt(ts.UTC().UnixMilli(), 10)}
	values = append(values, keys...)
	raw, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(t *testing.T, cursor string) (time.Time, []string) {
	t.Helper()
	ts, keys, err := decodeCursorValue(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	return ts, keys
}

func decodeCursorValue(cursor string) (time.Time, []string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, nil, err
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return time.Time{}, nil, err
	}
	if len(values) == 0 {
		return time.Time{}, nil, nil
	}
	ms, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return time.Time{}, nil, err
	}
	return time.UnixMilli(ms).UTC(), values[1:], nil
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
