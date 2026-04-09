package runtime

import "net/http"

// handleGetDashboardJobsOverview returns top-level AI batch + BYOC stats.
// GET /v1/dashboard/jobs/overview
func (s *Server) handleGetDashboardJobsOverview(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetDashboardJobsOverview(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard jobs overview failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}

// handleGetDashboardJobsByPipeline returns AI batch breakdown by pipeline.
// GET /v1/dashboard/jobs/by-pipeline
func (s *Server) handleGetDashboardJobsByPipeline(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetDashboardJobsByPipeline(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard jobs by pipeline failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}

// handleGetDashboardJobsByCapability returns BYOC breakdown by capability.
// GET /v1/dashboard/jobs/by-capability
func (s *Server) handleGetDashboardJobsByCapability(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetDashboardJobsByCapability(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get dashboard jobs by capability failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}
