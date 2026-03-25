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

func (s *Server) handleGetModelDetail(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	modelID := r.URL.Query().Get("model_id")
	if modelID == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "model_id query parameter is required")
		return
	}
	result, err := s.svc.GetModelDetail(r.Context(), modelID, p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get model detail failed", "error", err, "model_id", modelID)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "model not found")
		return
	}
	if result.Orchs == nil {
		result.Orchs = []types.OrchModelStats{}
	}
	respondJSON(w, http.StatusOK, result)
}
