package runtime

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/livepeer/naap-analytics/internal/types"
)

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
	p, err := parseSLAComplianceParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListSLACompliance(r.Context(), p)
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

func parseSLAComplianceParams(r *http.Request) (types.SLAComplianceParams, error) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		return types.SLAComplianceParams{}, err
	}
	// window: default 3h, min 1m, max 30d
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
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
		Limit:               parseListLimit(q, defaultCursorLimit),
		Cursor:              strings.TrimSpace(q.Get("cursor")),
	}, nil
}

// ---------------------------------------------------------------------------
// GET /v1/network/demand
// ---------------------------------------------------------------------------

func (s *Server) handleListNetworkDemand(w http.ResponseWriter, r *http.Request) {
	p, err := parseNetworkDemandParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListNetworkDemand(r.Context(), p)
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

func parseNetworkDemandParams(r *http.Request) (types.NetworkDemandParams, error) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		return types.NetworkDemandParams{}, err
	}
	// window: default 3h, min 1m, max 30d
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
	q := r.URL.Query()
	return types.NetworkDemandParams{
		Gateway:    trimStr(q.Get("gateway"), 256),
		Region:     trimStr(q.Get("region"), 64),
		PipelineID: trimStr(q.Get("pipeline_id"), 256),
		ModelID:    trimStr(q.Get("model_id"), 256),
		Org:        trimStr(q.Get("org"), 256),
		Start:      start,
		End:        end,
		Limit:      parseListLimit(q, defaultCursorLimit),
		Cursor:     strings.TrimSpace(q.Get("cursor")),
	}, nil
}

// ---------------------------------------------------------------------------
// GET /v1/gpu/network-demand
// ---------------------------------------------------------------------------

func (s *Server) handleListGPUNetworkDemand(w http.ResponseWriter, r *http.Request) {
	p, err := parseGPUNetworkDemandParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListGPUNetworkDemand(r.Context(), p)
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

func parseGPUNetworkDemandParams(r *http.Request) (types.GPUNetworkDemandParams, error) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		return types.GPUNetworkDemandParams{}, err
	}
	start, end, _ := parseWindowParam(r, 3*time.Hour, time.Minute, 30*24*time.Hour)
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
		Limit:               parseListLimit(q, defaultCursorLimit),
		Cursor:              strings.TrimSpace(q.Get("cursor")),
	}, nil
}

// ---------------------------------------------------------------------------
// GET /v1/gpu/metrics
// ---------------------------------------------------------------------------

func (s *Server) handleListGPUMetrics(w http.ResponseWriter, r *http.Request) {
	p, err := parseGPUMetricsParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	rows, page, err := s.svc.ListGPUMetrics(r.Context(), p)
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

func parseGPUMetricsParams(r *http.Request) (types.GPUMetricsParams, error) {
	if err := rejectLegacyPaginationParams(r); err != nil {
		return types.GPUMetricsParams{}, err
	}
	// window: default 24h, min 1m, max 72h
	start, end, _ := parseWindowParam(r, 24*time.Hour, time.Minute, 72*time.Hour)
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
		Limit:               parseListLimit(q, defaultCursorLimit),
		Cursor:              strings.TrimSpace(q.Get("cursor")),
	}, nil
}
