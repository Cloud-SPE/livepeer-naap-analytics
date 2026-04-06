package runtime

import (
	"net/http"
	"strconv"
	"strings"
)

// handleGetDashboardKPI serves GET /v1/dashboard/kpi
// Query params: window=24h (default) or window=7d — capped at 168 h.
func (s *Server) handleGetDashboardKPI(w http.ResponseWriter, r *http.Request) {
	hours := parseDashboardWindow(r, 24, 168)
	p := parseQueryParams(r)

	result, err := s.svc.GetDashboardKPI(r.Context(), hours, p.Pipeline, p.ModelID)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard kpi failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardPipelines serves GET /v1/dashboard/pipelines
// Query params: limit=5 (default, max 20).
func (s *Server) handleGetDashboardPipelines(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 20 {
				n = 20
			}
			limit = n
		}
	}

	result, err := s.svc.GetDashboardPipelines(r.Context(), limit)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pipelines failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardOrchestrators serves GET /v1/dashboard/orchestrators
// Query params: window=7d (default) or window=24h — capped at 720 h (30 days).
func (s *Server) handleGetDashboardOrchestrators(w http.ResponseWriter, r *http.Request) {
	hours := parseDashboardWindow(r, 168, 720)

	result, err := s.svc.GetDashboardOrchestrators(r.Context(), hours)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardGPUCapacity serves GET /v1/dashboard/gpu-capacity
func (s *Server) handleGetDashboardGPUCapacity(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetDashboardGPUCapacity(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard gpu capacity failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardPipelineCatalog serves GET /v1/dashboard/pipeline-catalog
func (s *Server) handleGetDashboardPipelineCatalog(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetDashboardPipelineCatalog(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pipeline catalog failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardPricing serves GET /v1/dashboard/pricing
func (s *Server) handleGetDashboardPricing(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetDashboardPricing(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pricing failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetDashboardJobFeed serves GET /v1/dashboard/job-feed
// Query params: limit=50 (default, max 200).
func (s *Server) handleGetDashboardJobFeed(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 200 {
				n = 200
			}
			limit = n
		}
	}

	result, err := s.svc.GetDashboardJobFeed(r.Context(), limit)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard job feed failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// parseDashboardWindow reads ?window=Xh or ?window=Xd and returns the value in
// hours, clamped to [1, maxHours]. Returns defaultHours if not provided.
func parseDashboardWindow(r *http.Request, defaultHours, maxHours int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("window"))
	if raw == "" {
		return defaultHours
	}
	var hours int
	switch {
	case strings.HasSuffix(raw, "h"):
		if n, err := strconv.Atoi(strings.TrimSuffix(raw, "h")); err == nil && n > 0 {
			hours = n
		}
	case strings.HasSuffix(raw, "d"):
		if n, err := strconv.Atoi(strings.TrimSuffix(raw, "d")); err == nil && n > 0 {
			hours = n * 24
		}
	default:
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			hours = n
		}
	}
	if hours <= 0 {
		return defaultHours
	}
	if hours > maxHours {
		hours = maxHours
	}
	return hours
}
