package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleGetActiveStreams(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetActiveStreams(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get active streams failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByOrg == nil {
		result.ByOrg = map[string]int64{}
	}
	if result.ByPipeline == nil {
		result.ByPipeline = map[string]int64{}
	}
	if result.ByState == nil {
		result.ByState = map[string]int64{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetStreamSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetStreamSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get stream summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListStreamHistory(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	// TODO: route to granularity-specific aggregate tables in Phase 5
	// (p.Granularity is passed through but repo always uses hourly buckets)
	result, err := s.svc.ListStreamHistory(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list stream history failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.StreamBucket{}
	}
	respondJSON(w, http.StatusOK, result)
}
