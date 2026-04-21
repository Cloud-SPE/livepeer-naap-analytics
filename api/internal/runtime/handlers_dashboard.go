package runtime

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/livepeer/naap-analytics/internal/types"
)

// handleGetDashboardKPI serves GET /v1/dashboard/kpi
func (s *Server) handleGetDashboardKPI(w http.ResponseWriter, r *http.Request) {
	hours := parseDashboardWindow(r, 24, 168)
	pipeline := r.URL.Query().Get("pipeline")
	modelID := r.URL.Query().Get("model_id")

	var (
		streaming  *types.DashboardKPI
		requests   *types.DashboardJobsOverview
		errS, errR error
		wg         sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		streaming, errS = s.svc.GetDashboardKPI(r.Context(), hours, pipeline, modelID)
	}()
	go func() {
		defer wg.Done()
		requests, errR = s.svc.GetDashboardJobsOverview(r.Context(), hours)
	}()
	wg.Wait()

	if errS != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard kpi (streaming) failed", "error", errS)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if errR != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard kpi (requests) failed", "error", errR)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}

	respondJSON(w, http.StatusOK, types.DashboardKPICombined{
		Streaming: streaming,
		Requests:  requests,
	})
}

// handleGetDashboardPipelines serves GET /v1/dashboard/pipelines
func (s *Server) handleGetDashboardPipelines(w http.ResponseWriter, r *http.Request) {
	hours := parseDashboardWindow(r, 24, 168)
	limit := 5
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 20 {
				n = 20
			}
			limit = n
		}
	}

	var (
		streaming    []types.DashboardPipelineUsage
		byPipeline   []types.DashboardJobsByPipelineRow
		byCapability []types.DashboardJobsByCapabilityRow
		errS, errP, errC error
		wg               sync.WaitGroup
	)

	wg.Add(3)
	go func() {
		defer wg.Done()
		streaming, errS = s.svc.GetDashboardPipelines(r.Context(), limit, hours)
	}()
	go func() {
		defer wg.Done()
		byPipeline, errP = s.svc.GetDashboardJobsByPipeline(r.Context(), hours)
	}()
	go func() {
		defer wg.Done()
		byCapability, errC = s.svc.GetDashboardJobsByCapability(r.Context(), hours)
	}()
	wg.Wait()

	if errS != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pipelines (streaming) failed", "error", errS)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if errP != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pipelines (by-pipeline) failed", "error", errP)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if errC != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard pipelines (by-capability) failed", "error", errC)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}

	respondJSON(w, http.StatusOK, types.DashboardPipelinesCombined{
		Streaming: streaming,
		Requests: &types.DashboardPipelinesRequestsSection{
			ByPipeline:   byPipeline,
			ByCapability: byCapability,
		},
	})
}

// handleGetDashboardOrchestrators serves GET /v1/dashboard/orchestrators
func (s *Server) handleGetDashboardOrchestrators(w http.ResponseWriter, r *http.Request) {
	hours := parseDashboardWindow(r, 24, 168)

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
