package runtime

import (
	"errors"
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

// handleGetBYOCSummary returns per-capability aggregates for BYOC jobs.
// Capabilities are dynamic (e.g. "openai-chat-completions") and stored verbatim.
// GET /v1/byoc/summary
func (s *Server) handleGetBYOCSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetBYOCSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}

// handleListBYOCJobs returns cursor-paginated completed BYOC job records.
// GET /v1/byoc/jobs
func (s *Server) handleListBYOCJobs(w http.ResponseWriter, r *http.Request) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	p := parseQueryParams(r)
	result, page, err := s.svc.ListBYOCJobs(r.Context(), p)
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
		"data":       result,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// handleGetBYOCWorkers returns per-capability worker inventory.
// GET /v1/byoc/workers
func (s *Server) handleGetBYOCWorkers(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetBYOCWorkers(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc workers failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}

// handleGetBYOCAuthSummary returns per-capability auth event success/failure rates.
// GET /v1/byoc/auth
func (s *Server) handleGetBYOCAuthSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetBYOCAuthSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get byoc auth summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}
