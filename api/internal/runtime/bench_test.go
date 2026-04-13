package runtime_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Benchmarks measure the full HTTP handler stack (routing + JSON encode)
// with the NoopAnalyticsRepo so results reflect framework overhead, not ClickHouse.
// Run with: cd api && go test ./internal/runtime/... -bench=. -benchmem -run='^$'

func BenchmarkListOrchestrators(b *testing.B) {
	benchHandler(b, "/v1/network/orchestrators")
}

func BenchmarkListModels(b *testing.B) {
	benchHandler(b, "/v1/network/models")
}

// benchHandler is the shared benchmark helper.
func benchHandler(b *testing.B, path string) {
	b.Helper()
	srv := newTestServer(b)
	h := srv.Handler()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d for %s", rr.Code, path)
		}
	}
}
