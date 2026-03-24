package runtime

import (
	"net/http"
	"strconv"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

// parseQueryParams extracts common query parameters from an HTTP request.
// Invalid or missing values fall back to safe defaults.
func parseQueryParams(r *http.Request) types.QueryParams {
	q := r.URL.Query()

	nodeID := q.Get("node_id")

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

	limit := 100
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	return types.QueryParams{
		NodeID:    nodeID,
		StartTime: start,
		EndTime:   end,
		Limit:     limit,
	}
}
