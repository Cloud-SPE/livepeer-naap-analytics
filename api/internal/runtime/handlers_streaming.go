package runtime

import (
	"errors"
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
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

// handleGetStreamingOrchestrators serves GET /v1/streaming/orchestrators
func (s *Server) handleGetStreamingOrchestrators(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetStreamingOrchestrators(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get streaming orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleListStreamingSLA serves GET /v1/streaming/sla
func (s *Server) handleListStreamingSLA(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	p := parseTimeWindowParams(r)
	rows, next, err := s.svc.ListStreamingSLA(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list streaming sla failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, types.CursorEnvelopeStreamingSLA{
		Data:       rows,
		Pagination: types.CursorPageInfo{NextCursor: next, HasMore: next != "", PageSize: len(rows)},
		Meta:       buildMeta(r),
	})
}

// handleListStreamingDemand serves GET /v1/streaming/demand
func (s *Server) handleListStreamingDemand(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	p := parseTimeWindowParams(r)
	rows, next, err := s.svc.ListStreamingDemand(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list streaming demand failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, types.CursorEnvelopeStreamingDemand{
		Data:       rows,
		Pagination: types.CursorPageInfo{NextCursor: next, HasMore: next != "", PageSize: len(rows)},
		Meta:       buildMeta(r),
	})
}

// handleListStreamingGPUMetrics serves GET /v1/streaming/gpu-metrics
func (s *Server) handleListStreamingGPUMetrics(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	p := parseTimeWindowParams(r)
	rows, next, err := s.svc.ListStreamingGPUMetrics(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list streaming gpu metrics failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, types.CursorEnvelopeStreamingGPUMetrics{
		Data:       rows,
		Pagination: types.CursorPageInfo{NextCursor: next, HasMore: next != "", PageSize: len(rows)},
		Meta:       buildMeta(r),
	})
}
