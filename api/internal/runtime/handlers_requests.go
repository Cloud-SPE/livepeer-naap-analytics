package runtime

import (
	"net/http"
)

// handleGetRequestsModels serves GET /v1/requests/models
func (s *Server) handleGetRequestsModels(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetRequestsModels(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get requests models failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
