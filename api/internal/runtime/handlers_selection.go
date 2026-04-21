package runtime

import (
	"net/http"
	"strings"

	"github.com/livepeer/naap-analytics/internal/types"
)

// handleDiscoverOrchestrators serves GET /v1/discover/orchestrators
//
// Optional repeated query param `caps` filters to orchestrators advertising
// ALL listed "pipeline/model" caps. Empty/whitespace caps are ignored.
//
// Example: ?caps=live-video-to-video/streamdiffusion-sdxl&caps=llm/glm-4.7-flash
func (s *Server) handleDiscoverOrchestrators(w http.ResponseWriter, r *http.Request) {
	rawCaps := r.URL.Query()["caps"]
	caps := make([]string, 0, len(rawCaps))
	for _, c := range rawCaps {
		c = strings.TrimSpace(c)
		if c != "" {
			caps = append(caps, c)
		}
	}

	result, err := s.svc.DiscoverOrchestrators(r.Context(), types.DiscoverOrchestratorsParams{Caps: caps})
	if err != nil {
		s.providers.Logger.Sugar().Errorw("discover orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
