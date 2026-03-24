package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleGetReliabilitySummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetReliabilitySummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get reliability summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListReliabilityHistory(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListReliabilityHistory(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list reliability history failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.ReliabilityBucket{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListOrchReliability(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListOrchReliability(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list orch reliability failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.OrchReliability{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListFailures(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListFailures(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list failures failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.FailureEvent{}
	}
	respondJSON(w, http.StatusOK, result)
}
