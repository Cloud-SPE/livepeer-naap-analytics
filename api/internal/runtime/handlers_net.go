package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

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

