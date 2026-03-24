package runtime_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Benchmarks measure the full HTTP handler stack (routing + JSON encode)
// with the NoopAnalyticsRepo so results reflect framework overhead, not ClickHouse.
// Run with: cd api && go test ./internal/runtime/... -bench=. -benchmem -run='^$'

func BenchmarkGetNetworkSummary(b *testing.B) {
	benchHandler(b, "/v1/net/summary")
}

func BenchmarkListOrchestrators(b *testing.B) {
	benchHandler(b, "/v1/net/orchestrators")
}

func BenchmarkGetGPUSummary(b *testing.B) {
	benchHandler(b, "/v1/net/gpu")
}

func BenchmarkListModels(b *testing.B) {
	benchHandler(b, "/v1/net/models")
}

func BenchmarkGetActiveStreams(b *testing.B) {
	benchHandler(b, "/v1/streams/active")
}

func BenchmarkGetStreamSummary(b *testing.B) {
	benchHandler(b, "/v1/streams/summary")
}

func BenchmarkListStreamHistory(b *testing.B) {
	benchHandler(b, "/v1/streams/history")
}

func BenchmarkGetFPSSummary(b *testing.B) {
	benchHandler(b, "/v1/perf/fps")
}

func BenchmarkListFPSHistory(b *testing.B) {
	benchHandler(b, "/v1/perf/fps/history")
}

func BenchmarkGetLatencySummary(b *testing.B) {
	benchHandler(b, "/v1/perf/latency")
}

func BenchmarkGetWebRTCQuality(b *testing.B) {
	benchHandler(b, "/v1/perf/webrtc")
}

func BenchmarkGetPaymentSummary(b *testing.B) {
	benchHandler(b, "/v1/payments/summary")
}

func BenchmarkListPaymentHistory(b *testing.B) {
	benchHandler(b, "/v1/payments/history")
}

func BenchmarkGetReliabilitySummary(b *testing.B) {
	benchHandler(b, "/v1/reliability/summary")
}

func BenchmarkListReliabilityHistory(b *testing.B) {
	benchHandler(b, "/v1/reliability/history")
}

func BenchmarkGetLeaderboard(b *testing.B) {
	benchHandler(b, "/v1/leaderboard")
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
