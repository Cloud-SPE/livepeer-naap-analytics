package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

// handleGetLeaderboard handles GET /v1/leaderboard.
// Returns orchestrators ranked by composite score (FPS 30%, reliability 30%, volume 20%, latency 20%).
func (s *Server) handleGetLeaderboard(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	entries, err := s.svc.GetLeaderboard(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get leaderboard failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if entries == nil {
		entries = []types.LeaderboardEntry{}
	}
	respondJSON(w, http.StatusOK, entries)
}

// handleGetLeaderboardProfile handles GET /v1/leaderboard/{address}.
// Returns the full orchestrator profile including GPU, model, and scoring inputs.
// Returns 404 when the orchestrator address is not found.
func (s *Server) handleGetLeaderboardProfile(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "address is required")
		return
	}
	result, err := s.svc.GetOrchProfile(r.Context(), address)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get leaderboard profile failed", "error", err, "address", address)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "orchestrator not found")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
