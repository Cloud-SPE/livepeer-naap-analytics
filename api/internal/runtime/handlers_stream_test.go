package runtime_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetActiveStreams_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/active", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	for _, field := range []string{"TotalActive", "ByOrg", "ByPipeline", "ByState"} {
		if _, ok := body[field]; !ok {
			t.Errorf("missing field %s", field)
		}
	}
}

func TestGetActiveStreams_WithOrg(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/active?org=cloudspe", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestGetStreamSummary_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/summary", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	for _, field := range []string{"TotalStarted", "SuccessRate"} {
		if _, ok := body[field]; !ok {
			t.Errorf("missing field %s", field)
		}
	}
}

func TestGetStreamSummary_WithTimeWindow(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/summary?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestGetStreamSummary_BadStartParam_SilentFallback(t *testing.T) {
	// parseQueryParams silently substitutes now-24h when ?start is malformed.
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/summary?start=notadate", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 (silent fallback), got %d", rr.Code)
	}
}

func TestListStreamHistory_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/history", nil)
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

func TestListStreamHistory_WithGranularity(t *testing.T) {
	// ?granularity is accepted and passed through; repo uses hourly buckets regardless.
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/history?granularity=1h", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}
