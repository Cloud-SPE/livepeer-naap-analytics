package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleGetNetworkSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetNetworkSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get network summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListOrchestrators(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListOrchestrators(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.Orchestrator{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetGPUSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetGPUSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get gpu summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByModel == nil {
		result.ByModel = []types.GPUModel{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListModels(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListModels(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list models failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.ModelAvailability{}
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetOrchProfile handles GET /v1/net/orchestrators/{address}.
// Returns 404 when the orchestrator is not found.
func (s *Server) handleGetOrchProfile(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "address is required")
		return
	}
	result, err := s.svc.GetOrchProfile(r.Context(), address)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get orch profile failed", "error", err, "address", address)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "orchestrator not found")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
