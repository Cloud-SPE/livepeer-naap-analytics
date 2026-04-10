package runtime

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// ---------------------------------------------------------------------------
// GET /v1/jobs/demand
// ---------------------------------------------------------------------------

func (s *Server) handleListJobsDemand(w http.ResponseWriter, r *http.Request) {
	p, err := parseJobsParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListJobsDemand(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data":       rows,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// ---------------------------------------------------------------------------
// GET /v1/jobs/sla
// ---------------------------------------------------------------------------

func (s *Server) handleListJobsSLA(w http.ResponseWriter, r *http.Request) {
	p, err := parseJobsParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListJobsSLA(r.Context(), p)
	if err != nil {
		if errors.Is(err, types.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data":       rows,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// ---------------------------------------------------------------------------
// GET /v1/jobs/by-model
// ---------------------------------------------------------------------------

func (s *Server) handleListJobsByModel(w http.ResponseWriter, r *http.Request) {
	p, err := parseJobsParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	result, err := s.svc.ListJobsByModel(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list jobs by model failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Shared parameter parser
// ---------------------------------------------------------------------------

func parseJobsParams(r *http.Request) (types.JobsParams, error) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		return types.JobsParams{}, err
	}
	start, end, _ := parseWindowParam(r, 24*time.Hour, time.Hour, 30*24*time.Hour)
	q := r.URL.Query()
	return types.JobsParams{
		OrchestratorAddress: strings.ToLower(trimStr(q.Get("orchestrator_address"), 256)),
		Gateway:             trimStr(q.Get("gateway"), 256),
		PipelineID:          trimStr(q.Get("pipeline_id"), 256),
		ModelID:             trimStr(q.Get("model_id"), 256),
		JobType:             trimStr(q.Get("job_type"), 32),
		Org:                 trimStr(q.Get("org"), 256),
		Start:               start,
		End:                 end,
		Limit:               parseListLimit(q, defaultCursorLimit),
		Cursor:              strings.TrimSpace(q.Get("cursor")),
	}, nil
}
