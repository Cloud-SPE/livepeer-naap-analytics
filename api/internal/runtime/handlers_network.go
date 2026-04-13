package runtime

import (
	"errors"
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListOrchestrators(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	p := parseQueryParams(r)
	result, page, err := s.svc.ListOrchestrators(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.Orchestrator{}
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data":       result,
		"pagination": page,
		"meta":       buildMeta(r),
	})
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

// handleGetCapacitySummary serves GET /v1/network/capacity
func (s *Server) handleGetCapacitySummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetCapacitySummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get capacity summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.Entries == nil {
		result.Entries = []types.CapacityEntry{}
	}
	respondJSON(w, http.StatusOK, result)
}
