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

func newTestServer(tb testing.TB) *runtime.Server {
	tb.Helper()
	return newTestServerWithRepo(tb, &repo.NoopAnalyticsRepo{})
}

func newTestServerWithRepo(tb testing.TB, analyticsRepo repo.AnalyticsRepo) *runtime.Server {
	tb.Helper()
	cfg := &config.Config{Port: "8000", Env: "development", LogLevel: "debug", KafkaBrokers: "localhost:9092"}
	p, err := providers.New(cfg)
	if err != nil {
		tb.Fatalf("init providers: %v", err)
	}
	tb.Cleanup(func() { p.Close(context.Background()) })
	return runtime.New(cfg, p, service.New(analyticsRepo))
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
