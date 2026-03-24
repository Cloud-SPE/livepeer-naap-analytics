package runtime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/providers"
	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/runtime"
	"github.com/livepeer/naap-analytics/internal/service"
)

func newTestServer(t *testing.T) *runtime.Server {
	t.Helper()
	cfg := &config.Config{Port: "8000", Env: "development", LogLevel: "debug", KafkaBrokers: "localhost:9092"}
	p, err := providers.New(cfg)
	if err != nil {
		t.Fatalf("init providers: %v", err)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	return runtime.New(cfg, p, service.New(&repo.NoopAnalyticsRepo{}))
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutes_NotImplemented(t *testing.T) {
	routes := []string{
		"/v1/net/summary",
		"/v1/net/orchestrators",
		"/v1/net/gpu",
		"/v1/net/models",
		"/v1/streams/active",
		"/v1/streams/summary",
		"/v1/streams/history",
		"/v1/perf/fps",
		"/v1/perf/fps/history",
		"/v1/perf/latency",
		"/v1/perf/webrtc",
		"/v1/payments/summary",
		"/v1/payments/history",
		"/v1/payments/by-pipeline",
		"/v1/payments/by-orch",
		"/v1/reliability/summary",
		"/v1/reliability/history",
		"/v1/reliability/orchs",
		"/v1/failures",
		"/v1/leaderboard",
		"/v1/leaderboard/0xdeadbeef",
	}

	srv := newTestServer(t)
	for _, path := range routes {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusNotImplemented {
				t.Errorf("%s: expected 501, got %d", path, rr.Code)
			}
		})
	}
}
