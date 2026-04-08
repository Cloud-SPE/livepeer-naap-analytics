package runtime_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListOrchestrators_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body []any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON array: %v", err)
	}
	// Noop returns nil → handler normalises to [].
	if body == nil {
		t.Error("expected non-null array, got null")
	}
}

func TestListOrchestrators_WithParams(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/net/orchestrators?org=daydream&active_only=true&limit=10&offset=0", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
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
