//go:build validation

package validation

import "testing"

func TestTierContract_BootstrapExcludesLegacyObjectFamilies(t *testing.T) {
	h := newHarness(t)

	checks := []struct {
		name  string
		query string
	}{
		{
			name:  "canonical_refresh",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name LIKE 'canonical_refresh_%'`,
		},
		{
			name:  "typed_tables",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name LIKE 'typed_%'`,
		},
		{
			name:  "typed_materialized_views",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name LIKE 'mv_typed_%'`,
		},
		{
			name:  "legacy_events",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name = 'events'`,
		},
		{
			name:  "compatibility_views",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name LIKE 'v_api_%'`,
		},
		{
			name:  "compatibility_store",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name = 'canonical_session_latest_store'`,
		},
		{
			name:  "deprecated_api_stores",
			query: `SELECT count() FROM system.tables WHERE database = 'naap' AND name IN ('api_status_samples_store', 'api_active_stream_state_store')`,
		},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if got := h.queryInt(t, check.query); got != 0 {
				t.Fatalf("%s legacy objects still present in bootstrap surface: %d", check.name, got)
			}
		})
	}
}
