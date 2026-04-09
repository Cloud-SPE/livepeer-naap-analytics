package runtime

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
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

	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	offset := 0
	if o := q.Get("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil && n >= 0 {
			offset = n
		}
	}

	activeOnly := strings.EqualFold(q.Get("active_only"), "true")

	return types.QueryParams{
		Org:         q.Get("org"),
		Pipeline:    q.Get("pipeline"),
		OrchAddress: strings.ToLower(q.Get("orch_address")),
		StreamID:    q.Get("stream_id"),
		FailureType: q.Get("failure_type"),
		StartTime:   start,
		EndTime:     end,
		Granularity: q.Get("granularity"),
		ActiveOnly:  activeOnly,
		Limit:       limit,
		Offset:      offset,
		Cursor:      q.Get("cursor"),
	}
}
