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

// parseTimeWindowParams extracts start/end/limit/cursor from query params.
func parseTimeWindowParams(r *http.Request) types.TimeWindowParams {
	q := r.URL.Query()

	start := time.Now().UTC().Add(-24 * time.Hour)
	if s := q.Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			start = t
		}
	}

	end := time.Now().UTC()
	if e := q.Get("end"); e != "" {
		if t, err := time.Parse(time.RFC3339, e); err == nil {
			end = t
		}
	}

	return types.TimeWindowParams{
		Start:  start,
		End:    end,
		Limit:  parseListLimit(q, defaultCursorLimit),
		Cursor: strings.TrimSpace(q.Get("cursor")),
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

// parseDashboardWindow reads ?window=Xh or ?window=Xd and returns the value in
// hours, clamped to [1, maxHours]. Returns defaultHours if not provided.
func parseDashboardWindow(r *http.Request, defaultHours, maxHours int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("window"))
	if raw == "" {
		return defaultHours
	}
	var hours int
	switch {
	case strings.HasSuffix(raw, "h"):
		if n, err := strconv.Atoi(strings.TrimSuffix(raw, "h")); err == nil && n > 0 {
			hours = n
		}
	case strings.HasSuffix(raw, "d"):
		if n, err := strconv.Atoi(strings.TrimSuffix(raw, "d")); err == nil && n > 0 {
			hours = n * 24
		}
	default:
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			hours = n
		}
	}
	if hours <= 0 {
		return defaultHours
	}
	if hours > maxHours {
		hours = maxHours
	}
	return hours
}
