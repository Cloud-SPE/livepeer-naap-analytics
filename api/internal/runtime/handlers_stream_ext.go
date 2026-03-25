package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListStreamSamples(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListStreamSamples(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list stream samples failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.StreamStatusSample{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetStreamDetail(w http.ResponseWriter, r *http.Request) {
	streamID := chi.URLParam(r, "stream_id")
	if streamID == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "stream_id is required")
		return
	}
	result, err := s.svc.GetStreamDetail(r.Context(), streamID)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get stream detail failed", "error", err, "stream_id", streamID)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "stream not found")
		return
	}
	if result.Samples == nil {
		result.Samples = []types.StreamStatusSample{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetAttributionSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetAttributionSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get attribution summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
