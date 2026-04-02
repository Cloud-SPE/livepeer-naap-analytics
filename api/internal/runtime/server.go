// Package runtime is the entry-point layer for the API service.
// It wires together config, providers, repo, and service, then starts the HTTP server.
// This is layer 5 — the only layer permitted to import all other layers.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/providers"
	"github.com/livepeer/naap-analytics/internal/service"
)

// Server is the HTTP runtime for the analytics API.
type Server struct {
	cfg       *config.Config
	providers *providers.Providers
	svc       service.AnalyticsService
	router    chi.Router
}

// New constructs a Server with all dependencies wired.
func New(cfg *config.Config, p *providers.Providers, svc service.AnalyticsService) *Server {
	s := &Server{cfg: cfg, providers: p, svc: svc}
	s.router = s.buildRouter()
	return s
}

// Start runs the HTTP server and blocks until a shutdown signal is received.
func (s *Server) Start() error {
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", s.cfg.Port),
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.providers.Logger.Sugar().Infof("API server listening on %s (env: %s)", srv.Addr, s.cfg.Env)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case sig := <-quit:
		s.providers.Logger.Sugar().Infof("shutdown signal received: %s", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	s.providers.Close(ctx)
	return nil
}

// Handler returns the HTTP handler, primarily for testing.
func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) buildRouter() chi.Router {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(metricsMiddleware)

	r.Handle("/metrics", promhttp.Handler())
	r.Get("/healthz", s.handleHealth)

	// API documentation
	r.Get("/docs", handleSwaggerUI)
	r.Get("/docs/openapi.yaml", handleOpenAPISpec)

	r.Route("/v1", func(r chi.Router) {
		// Network state (R1)
		r.Get("/net/summary", s.handleGetNetworkSummary)
		r.Get("/net/orchestrators", s.handleListOrchestrators)
		r.Get("/net/orchestrators/{address}", s.handleGetOrchProfile)
		r.Get("/net/gpu", s.handleGetGPUSummary)
		r.Get("/net/models", s.handleListModels)

		// Stream activity (R2)
		r.Get("/streams/active", s.handleGetActiveStreams)
		r.Get("/streams/summary", s.handleGetStreamSummary)
		r.Get("/streams/history", s.handleListStreamHistory)

		// Performance (R3)
		r.Get("/perf/fps", s.handleGetFPSSummary)
		r.Get("/perf/fps/history", s.handleListFPSHistory)
		r.Get("/perf/latency", s.handleGetLatencySummary)
		r.Get("/perf/webrtc", s.handleGetWebRTCQuality)

		// Payments (R4)
		r.Get("/payments/summary", s.handleGetPaymentSummary)
		r.Get("/payments/history", s.handleListPaymentHistory)
		r.Get("/payments/by-pipeline", s.handleListPaymentsByPipeline)
		r.Get("/payments/by-orch", s.handleListPaymentsByOrch)

		// Reliability (R5)
		r.Get("/reliability/summary", s.handleGetReliabilitySummary)
		r.Get("/reliability/history", s.handleListReliabilityHistory)
		r.Get("/reliability/orchs", s.handleListOrchReliability)
		r.Get("/failures", s.handleListFailures)

		// Leaderboard (R6)
		r.Get("/leaderboard", s.handleGetLeaderboard)
		r.Get("/leaderboard/{address}", s.handleGetLeaderboardProfile)

		// Gateways (R7)
		r.Get("/net/gateways", s.handleListGateways)
		r.Get("/net/gateways/{address}/orchs", s.handleListGatewayOrchestrators)
		r.Get("/net/gateways/{address}", s.handleGetGatewayProfile)

		// Network capacity & pricing (R11, R12, R15)
		r.Get("/net/pricing", s.handleListPricing)
		r.Get("/net/pricing/{address}", s.handleGetOrchPricingProfile)
		r.Get("/net/model", s.handleGetModelDetail)
		r.Get("/net/capacity", s.handleGetCapacitySummary)

		// Stream extensions (R8)
		r.Get("/streams/samples", s.handleListStreamSamples)
		r.Get("/streams/attribution", s.handleGetAttributionSummary)
		r.Get("/streams/{stream_id}", s.handleGetStreamDetail)

		// E2E latency (R9)
		r.Get("/perf/e2e-latency/history", s.handleListE2ELatencyHistory)
		r.Get("/perf/e2e-latency", s.handleGetE2ELatencySummary)

		// Model performance (R12)
		r.Get("/perf/by-model", s.handleListModelPerformance)

		// Pipelines (R10)
		r.Get("/pipelines", s.handleListPipelines)
		r.Get("/pipelines/{pipeline}", s.handleGetPipelineDetail)

		// Extended payments (R13)
		r.Get("/payments/by-gateway", s.handleListPaymentsByGateway)
		r.Get("/payments/by-stream", s.handleListPaymentsByStream)

		// Failure analysis (R14)
		r.Get("/failures/by-pipeline", s.handleListFailuresByPipeline)
		r.Get("/failures/by-orch", s.handleListFailuresByOrch)

		// SLA / GPU / Network Demand (ported from leaderboard-serverless)
		r.Get("/sla/compliance", s.handleListSLACompliance)
		r.Get("/network/demand", s.handleListNetworkDemand)
		r.Get("/gpu/network-demand", s.handleListGPUNetworkDemand)
		r.Get("/gpu/metrics", s.handleListGPUMetrics)

		// AI Batch Jobs (R17)
		r.Get("/ai-batch/summary", s.handleGetAIBatchSummary)
		r.Get("/ai-batch/jobs", s.handleListAIBatchJobs)
		r.Get("/ai-batch/llm/summary", s.handleGetAIBatchLLMSummary)

		// BYOC Jobs (R18)
		r.Get("/byoc/summary", s.handleGetBYOCSummary)
		r.Get("/byoc/jobs", s.handleListBYOCJobs)
		r.Get("/byoc/workers", s.handleGetBYOCWorkers)
		r.Get("/byoc/auth", s.handleGetBYOCAuthSummary)

		// Dashboard — pre-aggregated UI endpoints (R16)
		r.Get("/dashboard/kpi", s.handleGetDashboardKPI)
		r.Get("/dashboard/pipelines", s.handleGetDashboardPipelines)
		r.Get("/dashboard/orchestrators", s.handleGetDashboardOrchestrators)
		r.Get("/dashboard/gpu-capacity", s.handleGetDashboardGPUCapacity)
		r.Get("/dashboard/pipeline-catalog", s.handleGetDashboardPipelineCatalog)
		r.Get("/dashboard/pricing", s.handleGetDashboardPricing)
		r.Get("/dashboard/job-feed", s.handleGetDashboardJobFeed)
	})

	return r
}

// handleHealth returns 200 if the service is up, including a ClickHouse ping.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	httpStatus := http.StatusOK

	if err := s.svc.Ping(r.Context()); err != nil {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
		s.providers.Logger.Sugar().Warnw("healthz: clickhouse ping failed", "error", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}

// notImplemented is a placeholder handler for routes not yet implemented.
// Returns RFC 7807-style JSON error with 501 Not Implemented.
func notImplemented(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"type":   "about:blank",
		"title":  "Not Implemented",
		"status": "501",
	})
}

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// problemDetail is the RFC 7807 error response body.
type problemDetail struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// writeError writes an RFC 7807 problem+json response.
func writeError(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetail{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Detail: detail,
	})
}
