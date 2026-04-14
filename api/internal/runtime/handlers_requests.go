package runtime

import (
	"errors"
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
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

// handleGetRequestsOrchestrators serves GET /v1/requests/orchestrators
func (s *Server) handleGetRequestsOrchestrators(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.GetRequestsOrchestrators(r.Context())
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get requests orchestrators failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetAIBatchSummary serves GET /v1/requests/ai-batch/summary
func (s *Server) handleGetAIBatchSummary(w http.ResponseWriter, r *http.Request) {
	p := parseTimeWindowParams(r)
	result, err := s.svc.GetAIBatchSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get ai batch summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleListAIBatchJobs serves GET /v1/requests/ai-batch/jobs
func (s *Server) handleListAIBatchJobs(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	p := parseTimeWindowParams(r)
	rows, page, err := s.svc.ListAIBatchJobs(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list ai batch jobs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"data":       rows,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// handleGetAIBatchLLMSummary serves GET /v1/requests/ai-batch/llm-summary
func (s *Server) handleGetAIBatchLLMSummary(w http.ResponseWriter, r *http.Request) {
	p := parseTimeWindowParams(r)
	result, err := s.svc.GetAIBatchLLMSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get ai batch llm summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetBYOCSummary serves GET /v1/requests/byoc/summary
func (s *Server) handleGetBYOCSummary(w http.ResponseWriter, r *http.Request) {
	p := parseTimeWindowParams(r)
	result, err := s.svc.GetBYOCSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleListBYOCJobs serves GET /v1/requests/byoc/jobs
func (s *Server) handleListBYOCJobs(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	p := parseTimeWindowParams(r)
	rows, page, err := s.svc.ListBYOCJobs(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		s.providers.Logger.Sugar().Errorw("list byoc jobs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"data":       rows,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// handleGetBYOCWorkers serves GET /v1/requests/byoc/workers
func (s *Server) handleGetBYOCWorkers(w http.ResponseWriter, r *http.Request) {
	p := parseTimeWindowParams(r)
	result, err := s.svc.GetBYOCWorkers(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc workers failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// handleGetBYOCAuth serves GET /v1/requests/byoc/auth
func (s *Server) handleGetBYOCAuth(w http.ResponseWriter, r *http.Request) {
	p := parseTimeWindowParams(r)
	result, err := s.svc.GetBYOCAuth(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc auth failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}
