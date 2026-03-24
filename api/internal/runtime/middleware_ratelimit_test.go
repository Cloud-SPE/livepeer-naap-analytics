package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimitMiddleware_Disabled(t *testing.T) {
	// rps=0 → middleware is a no-op; 100 rapid requests all pass.
	h := rateLimitMiddleware(0, 0)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := range 100 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rr.Code)
		}
	}
}

func TestRateLimitMiddleware_EnforcesBurst(t *testing.T) {
	// burst=2, rps=1: token bucket starts full so first 2 pass, rest are rejected.
	h := rateLimitMiddleware(1, 2)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var pass, blocked int
	for range 5 {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:9999"
		h.ServeHTTP(rr, req)
		switch rr.Code {
		case http.StatusOK:
			pass++
		case http.StatusTooManyRequests:
			blocked++
		default:
			t.Errorf("unexpected status %d", rr.Code)
		}
	}

	if pass != 2 {
		t.Errorf("expected 2 requests to pass (burst=2), got %d", pass)
	}
	if blocked != 3 {
		t.Errorf("expected 3 requests blocked, got %d", blocked)
	}
}

func TestRateLimitMiddleware_PerIP(t *testing.T) {
	// Two different IPs each get their own bucket; both burst through independently.
	h := rateLimitMiddleware(1, 1)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, ip := range []string{"10.0.0.1:1", "10.0.0.2:2"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = ip
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("ip %s: first request should pass, got %d", ip, rr.Code)
		}
	}
}

func TestRateLimitMiddleware_RetryAfterHeader(t *testing.T) {
	h := rateLimitMiddleware(1, 1)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	makeReq := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.168.1.1:1"
		h.ServeHTTP(rr, req)
		return rr
	}

	makeReq() // consume the burst token
	rr := makeReq()
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 429 response")
	}
}
