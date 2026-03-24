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

	r.Get("/healthz", s.handleHealth)
	r.Route("/v1", func(r chi.Router) {
		r.Get("/analytics/windows", s.handleGetWindows)
		r.Get("/analytics/alerts", s.handleGetAlerts)
	})

	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGetWindows(w http.ResponseWriter, r *http.Request) {
	// TODO: parse QueryParams from request, pass to service
	windows, err := s.svc.QueryWindows(r.Context(), parseQueryParams(r))
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		s.providers.Logger.Sugar().Errorw("query windows failed", "error", err)
		return
	}
	respondJSON(w, http.StatusOK, windows)
}

func (s *Server) handleGetAlerts(w http.ResponseWriter, r *http.Request) {
	alerts, err := s.svc.QueryAlerts(r.Context(), parseQueryParams(r))
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		s.providers.Logger.Sugar().Errorw("query alerts failed", "error", err)
		return
	}
	respondJSON(w, http.StatusOK, alerts)
}

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
