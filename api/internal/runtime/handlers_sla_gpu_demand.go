package runtime

import (
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/livepeer/naap-analytics/internal/types"
)

// buildPagination constructs a Pagination struct from the given values.
func buildPagination(page, pageSize, total int) types.Pagination {
	pages := 0
	if pageSize > 0 {
		pages = (total + pageSize - 1) / pageSize
	}
	return types.Pagination{
		Page:       page,
		PageSize:   pageSize,
		TotalCount: total,
		TotalPages: pages,
	}
}

// parseWindowParam parses the ?window query param as a Go duration string.
// Day-suffix shorthand (e.g. "30d") is also accepted.
// Returns the computed start/end and whether the window was within [minW, maxW].
func parseWindowParam(r *http.Request, defaultW, minW, maxW time.Duration) (start, end time.Time, ok bool) {
	end = time.Now().UTC()
	w := defaultW

	if raw := r.URL.Query().Get("window"); raw != "" {
		// Support "Nd" day suffix
		if strings.HasSuffix(raw, "d") {
			if days, err := strconv.Atoi(strings.TrimSuffix(raw, "d")); err == nil && days > 0 {
				w = time.Duration(days) * 24 * time.Hour
			}
		} else if d, err := time.ParseDuration(raw); err == nil {
			w = d
		}
	}

	if w < minW {
		w = minW
	}
	if w > maxW {
		w = maxW
	}

	start = end.Add(-w)
	return start, end, true
}

// parsePageParams parses ?page and ?page_size with sensible defaults and caps.
func parsePageParams(r *http.Request, defaultSize int) (page, pageSize int) {
	page = 1
	pageSize = defaultSize

	q := r.URL.Query()
	if v := q.Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			page = n
		}
	}
	if v := q.Get("page_size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 500 {
			pageSize = n
		}
	}
	return page, pageSize
}

// trimStr trims whitespace and truncates s to maxLen UTF-8 characters.
func trimStr(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) <= maxLen {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxLen])
}

// ---------------------------------------------------------------------------
// GET /v1/sla/compliance
// ---------------------------------------------------------------------------

func (s *Server) handleListSLACompliance(w http.ResponseWriter, r *http.Request) {
	p := parseSLAComplianceParams(r)
	rows, total, err := s.svc.ListSLACompliance(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"compliance": rows,
		"pagination": buildPagination(p.Page, p.PageSize, total),
	})
}

func parseSLAComplianceParams(r *http.Request) types.SLAComplianceParams {
	// window: default 3h, min 1m, max 30d
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
	page, pageSize := parsePageParams(r, 50)
	q := r.URL.Query()
	return types.SLAComplianceParams{
		OrchestratorAddress: strings.ToLower(trimStr(q.Get("orchestrator_address"), 256)),
		Region:              trimStr(q.Get("region"), 64),
		PipelineID:          trimStr(q.Get("pipeline_id"), 256),
		ModelID:             trimStr(q.Get("model_id"), 256),
		GPUID:               trimStr(q.Get("gpu_id"), 256),
		Org:                 trimStr(q.Get("org"), 256),
		Start:               start,
		End:                 end,
		Page:                page,
		PageSize:            pageSize,
	}
}

// ---------------------------------------------------------------------------
// GET /v1/network/demand
// ---------------------------------------------------------------------------

func (s *Server) handleListNetworkDemand(w http.ResponseWriter, r *http.Request) {
	p := parseNetworkDemandParams(r)
	rows, total, err := s.svc.ListNetworkDemand(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"demand":     rows,
		"pagination": buildPagination(p.Page, p.PageSize, total),
	})
}

func parseNetworkDemandParams(r *http.Request) types.NetworkDemandParams {
	// window: default 3h, min 1m, max 30d
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
	page, pageSize := parsePageParams(r, 50)
	q := r.URL.Query()
	return types.NetworkDemandParams{
		Gateway:    trimStr(q.Get("gateway"), 256),
		Region:     trimStr(q.Get("region"), 64),
		PipelineID: trimStr(q.Get("pipeline_id"), 256),
		ModelID:    trimStr(q.Get("model_id"), 256),
		Org:        trimStr(q.Get("org"), 256),
		Start:      start,
		End:        end,
		Page:       page,
		PageSize:   pageSize,
	}
}

// ---------------------------------------------------------------------------
// GET /v1/gpu/network-demand
// ---------------------------------------------------------------------------

func (s *Server) handleListGPUNetworkDemand(w http.ResponseWriter, r *http.Request) {
	p := parseGPUNetworkDemandParams(r)
	rows, total, err := s.svc.ListGPUNetworkDemand(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"demand":     rows,
		"pagination": buildPagination(p.Page, p.PageSize, total),
	})
}

func parseGPUNetworkDemandParams(r *http.Request) types.GPUNetworkDemandParams {
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
	page, pageSize := parsePageParams(r, 50)
	q := r.URL.Query()
	return types.GPUNetworkDemandParams{
		Gateway:             trimStr(q.Get("gateway"), 256),
		OrchestratorAddress: strings.ToLower(trimStr(q.Get("orchestrator_address"), 256)),
		Region:              trimStr(q.Get("region"), 64),
		PipelineID:          trimStr(q.Get("pipeline_id"), 256),
		ModelID:             trimStr(q.Get("model_id"), 256),
		GPUID:               trimStr(q.Get("gpu_id"), 256),
		Org:                 trimStr(q.Get("org"), 256),
		Start:               start,
		End:                 end,
		Page:                page,
		PageSize:            pageSize,
	}
}

// ---------------------------------------------------------------------------
// GET /v1/gpu/metrics
// ---------------------------------------------------------------------------

func (s *Server) handleListGPUMetrics(w http.ResponseWriter, r *http.Request) {
	p := parseGPUMetricsParams(r)
	rows, total, err := s.svc.ListGPUMetrics(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"metrics":    rows,
		"pagination": buildPagination(p.Page, p.PageSize, total),
	})
}

func parseGPUMetricsParams(r *http.Request) types.GPUMetricsParams {
	// window: default 24h, min 1m, max 72h
	start, end, _ := parseWindowParam(r, 24*time.Hour, time.Minute, 72*time.Hour)
	page, pageSize := parsePageParams(r, 50)
	q := r.URL.Query()
	return types.GPUMetricsParams{
		OrchestratorAddress: strings.ToLower(trimStr(q.Get("orchestrator_address"), 256)),
		GPUID:               trimStr(q.Get("gpu_id"), 256),
		Region:              trimStr(q.Get("region"), 64),
		PipelineID:          trimStr(q.Get("pipeline_id"), 256),
		ModelID:             trimStr(q.Get("model_id"), 256),
		GPUModelName:        trimStr(q.Get("gpu_model_name"), 256),
		RunnerVersion:       trimStr(q.Get("runner_version"), 64),
		CudaVersion:         trimStr(q.Get("cuda_version"), 32),
		Org:                 trimStr(q.Get("org"), 256),
		Start:               start,
		End:                 end,
		Page:                page,
		PageSize:            pageSize,
	}
}
