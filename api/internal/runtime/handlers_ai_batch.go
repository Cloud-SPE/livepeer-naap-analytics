package runtime

import "net/http"

// handleGetAIBatchSummary returns per-pipeline aggregates for AI batch jobs.
// GET /v1/ai-batch/summary
func (s *Server) handleGetAIBatchSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetAIBatchSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get ai batch summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}

// handleListAIBatchJobs returns cursor-paginated completed AI batch job records.
// GET /v1/ai-batch/jobs
func (s *Server) handleListAIBatchJobs(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, page, err := s.svc.ListAIBatchJobs(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list ai batch jobs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data":       result,
		"pagination": page,
		"meta":       buildMeta(r),
	})
}

// handleGetAIBatchLLMSummary returns per-model LLM performance aggregates.
// GET /v1/ai-batch/llm/summary
func (s *Server) handleGetAIBatchLLMSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetAIBatchLLMSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get ai batch llm summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": buildMeta(r),
	})
}
