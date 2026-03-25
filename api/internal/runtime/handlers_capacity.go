package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

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
