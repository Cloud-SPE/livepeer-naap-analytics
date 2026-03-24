package runtime_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetLeaderboard_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/leaderboard", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var body []any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON array: %v", err)
	}
	if body == nil {
		t.Error("expected non-null array, got null")
	}
}

func TestGetLeaderboard_WithOrg(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/leaderboard?org=cloudspe", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestGetLeaderboardProfile_NotFound(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/leaderboard/0xdeadbeef", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}
