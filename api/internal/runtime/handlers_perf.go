package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleGetFPSSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetFPSSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get fps summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByPipeline == nil {
		result.ByPipeline = []types.PipelineFPS{}
	}
	if result.ByOrchestrator == nil {
		result.ByOrchestrator = []types.OrchFPS{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListFPSHistory(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListFPSHistory(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list fps history failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.FPSBucket{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetLatencySummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetLatencySummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get latency summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByOrchestrator == nil {
		result.ByOrchestrator = []types.OrchLatency{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetWebRTCQuality(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetWebRTCQuality(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get webrtc quality failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetE2ELatencySummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetE2ELatencySummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get e2e latency summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByPipeline == nil {
		result.ByPipeline = []types.PipelineE2ELatency{}
	}
	if result.ByOrchestrator == nil {
		result.ByOrchestrator = []types.OrchE2ELatency{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListE2ELatencyHistory(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListE2ELatencyHistory(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list e2e latency history failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.E2ELatencyBucket{}
	}
	respondJSON(w, http.StatusOK, result)
}
