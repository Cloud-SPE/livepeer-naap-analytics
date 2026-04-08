package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListModelPerformance(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListModelPerformance(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list model performance failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.ModelPerformance{}
	}
	respondJSON(w, http.StatusOK, result)
}

