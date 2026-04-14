package runtime

import (
	"net/http"
)

// handleGetStreamingModels serves GET /v1/streaming/models
func (s *Server) handleGetStreamingModels(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetStreamingModels(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get streaming models failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
