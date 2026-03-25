package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPipelines(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list pipelines failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.PipelineSummary{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetPipelineDetail(w http.ResponseWriter, r *http.Request) {
	pipeline := chi.URLParam(r, "pipeline")
	if pipeline == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "pipeline is required")
		return
	}
	p := parseQueryParams(r)
	result, err := s.svc.GetPipelineDetail(r.Context(), pipeline, p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get pipeline detail failed", "error", err, "pipeline", pipeline)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "pipeline not found")
		return
	}
	if result.ByModel == nil {
		result.ByModel = []types.ModelPipelineStats{}
	}
	respondJSON(w, http.StatusOK, result)
}
