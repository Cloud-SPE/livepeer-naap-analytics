//go:build validation

package validation

// ── RULE-SERVING-001 ─────────────────────────────────────────────────────────
// Public serving families must expose stable logical grains and required
// fields, including org-aware variants.
//
// ── RULE-SERVING-002 ─────────────────────────────────────────────────────────
// API read models must remain derivable from canonical facts and aggregates,
// not reinterpret raw ingest payloads independently.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleServing001_APIOutputsExposeStableRequiredFields(t *testing.T) {
	h := newHarness(t)

	type fieldCheck struct {
		table  string
		fields []string
	}

	checks := []fieldCheck{
		{
			table: "api_sla_compliance_by_org",
			fields: []string{
				"window_start", "org", "orchestrator_address", "pipeline_id", "model_id", "gpu_id",
				"known_sessions_count", "startup_success_sessions", "startup_failed_sessions", "excused_failure_rate", "sla_score",
			},
		},
		{
			table: "api_network_demand_by_org",
			fields: []string{
				"window_start", "org", "gateway", "pipeline_id", "model_id", "sessions_count",
				"requested_sessions", "startup_success_sessions", "startup_failed_sessions", "startup_success_rate", "ticket_face_value_eth",
			},
		},
		{
			table: "api_gpu_metrics_by_org",
			fields: []string{
				"window_start", "org", "orchestrator_address", "pipeline_id", "model_id", "gpu_id",
				"status_samples", "error_status_samples", "known_sessions_count", "swap_rate",
			},
		},
	}

	for _, check := range checks {
		t.Run(check.table, func(t *testing.T) {
			fields := "'" + strings.Join(check.fields, "','") + "'"
			got := h.queryInt(t, fmt.Sprintf(
				`SELECT count() FROM system.columns WHERE database = 'naap' AND table = '%s' AND name IN (%s)`,
				check.table, fields,
			))
			if got != uint64(len(check.fields)) {
				t.Fatalf("%s exposed %d/%d required fields", check.table, got, len(check.fields))
			}
		})
	}

	for _, publicTable := range []string{"api_sla_compliance", "api_network_demand", "api_gpu_metrics"} {
		t.Run(publicTable, func(t *testing.T) {
			if got := h.queryInt(t, fmt.Sprintf(
				`SELECT count() FROM system.columns WHERE database = 'naap' AND table = '%s' AND name = 'org'`,
				publicTable,
			)); got != 1 {
				t.Fatalf("%s must expose one nullable org placeholder column", publicTable)
			}
		})
	}
}

func TestRuleServing002_APIOutputsRemainDerivableFromCanonicalFacts(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(5 * time.Minute)
	streamID := uid("serve")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	windowStart := ts.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-serve","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(10 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(20 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":15.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(20 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM system.columns WHERE database = 'naap' AND table = 'api_network_demand_by_org' AND name = 'canonical_session_key'`); got != 0 {
		t.Fatalf("api_network_demand_by_org unexpectedly exposed canonical_session_key")
	}
	if got := h.queryInt(t, `SELECT status_samples FROM naap.api_gpu_metrics_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_gpu_metrics_by_org status_samples = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT known_sessions_count FROM naap.api_gpu_metrics_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_gpu_metrics_by_org known_sessions_count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_latest WHERE canonical_session_key = ? AND startup_outcome = 'success' AND attributed_orch_address = ?`, key, orchAddr); got != 1 {
		t.Fatalf("canonical_session_latest did not retain the canonical success row for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ? AND is_terminal_tail_artifact = 0 AND status_samples = 1`, key, windowStart); got != 1 {
		t.Fatalf("canonical_status_hours did not retain the non-tail hourly status row for %s", key)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_pipeline, '') FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key); got != "text-to-image" {
		t.Fatalf("canonical_session_latest canonical_pipeline = %q, want text-to-image", got)
	}
}
