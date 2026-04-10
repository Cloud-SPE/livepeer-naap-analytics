package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/types"
)

func TestListOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON object: %v", err)
	}
	assertCursorEnvelope(t, body)
	data, ok := body["data"].([]any)
	if !ok {
		t.Fatalf("expected data array, got %T", body["data"])
	}
	if data == nil {
		t.Error("expected non-null data array, got null")
	}
}

func TestListOrchestrators_WithCursorParams(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators?org=daydream&active_only=true&limit=10&cursor=opaque-token", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestListOrchestrators_RejectsLegacyPaginationParams(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators?limit=10&offset=0", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `query parameter "offset" is no longer supported; use limit and cursor`)
}

func TestListOrchestrators_InvalidCursor(t *testing.T) {
	srv := newTestServerWithRepo(t, &invalidCursorRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators?cursor=not-a-real-token", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, types.ErrInvalidCursor.Error())
}

func TestListOrchestrators_BadLimit_SilentFallback(t *testing.T) {
	// parseQueryParams silently falls back to default limit for non-numeric values.
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators?limit=notanumber", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 (silent fallback), got %d", rr.Code)
	}
}

func TestListModels_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/models", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body []any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON array: %v", err)
	}
	if body == nil {
		t.Error("expected non-null array, got null")
	}
}

type invalidCursorRepo struct {
	repo.NoopAnalyticsRepo
}

func (invalidCursorRepo) ListOrchestrators(_ context.Context, p types.QueryParams) ([]types.Orchestrator, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.Orchestrator{}, types.CursorPageInfo{}, nil
}

func assertCursorEnvelope(t *testing.T, body map[string]any) {
	t.Helper()
	for _, field := range []string{"data", "pagination", "meta"} {
		if _, ok := body[field]; !ok {
			t.Fatalf("missing field %s", field)
		}
	}
	pagination, ok := body["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("expected pagination object, got %T", body["pagination"])
	}
	for _, field := range []string{"has_more", "page_size"} {
		if _, ok := pagination[field]; !ok {
			t.Fatalf("missing pagination field %s", field)
		}
	}
	if _, ok := pagination["next_cursor"]; !ok {
		t.Fatal("missing pagination field next_cursor")
	}
	meta, ok := body["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected meta object, got %T", body["meta"])
	}
	for _, field := range []string{"generated_at", "request_id"} {
		if _, ok := meta[field]; !ok {
			t.Fatalf("missing meta field %s", field)
		}
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
	detail, ok := body["detail"].(string)
	if !ok {
		t.Fatalf("expected string detail, got %T", body["detail"])
	}
	if !strings.Contains(detail, expectedDetail) {
		t.Fatalf("expected detail %q, got %v", expectedDetail, body["detail"])
	}
}
