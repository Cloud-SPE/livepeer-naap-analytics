package runtime

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

const (
	defaultCursorLimit = 50
	maxCursorLimit     = 1000
)

// parseQueryParams extracts common query parameters from an HTTP request.
// Invalid or missing values fall back to safe defaults.
func parseQueryParams(r *http.Request) types.QueryParams {
	q := r.URL.Query()

	start := time.Now().Add(-24 * time.Hour)
	if s := q.Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			start = t
		}
	}

	end := time.Now()
	if e := q.Get("end"); e != "" {
		if t, err := time.Parse(time.RFC3339, e); err == nil {
			end = t
		}
	}

	limit := parseListLimit(q, defaultCursorLimit)

	activeOnly := strings.EqualFold(q.Get("active_only"), "true")

	return types.QueryParams{
		Org:         q.Get("org"),
		Pipeline:    q.Get("pipeline"),
		ModelID:     q.Get("model_id"),
		OrchAddress: strings.ToLower(q.Get("orch_address")),
		StreamID:    q.Get("stream_id"),
		FailureType: q.Get("failure_type"),
		JobType:     q.Get("job_type"),
		StartTime:   start,
		EndTime:     end,
		Granularity: q.Get("granularity"),
		ActiveOnly:  activeOnly,
		Limit:       limit,
		Cursor:      strings.TrimSpace(q.Get("cursor")),
	}
}

func parseListLimit(q url.Values, defaultLimit int) int {
	limit := defaultLimit
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= maxCursorLimit {
			limit = n
		}
	}
	return limit
}

func rejectLegacyPaginationParams(r *http.Request) error {
	q := r.URL.Query()
	for _, key := range []string{"offset", "page", "page_size"} {
		if q.Has(key) {
			return fmt.Errorf("query parameter %q is no longer supported; use limit and cursor", key)
		}
	}
	return nil
}
