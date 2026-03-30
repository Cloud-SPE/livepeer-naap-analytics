//go:build validation

package validation

// ── RULE-PARITY-001 ──────────────────────────────────────────────────────────
// Raw replay must be provable all the way through normalized, canonical, and
// serving outputs.
//
// ── RULE-PARITY-002 ──────────────────────────────────────────────────────────
// Parity must cover ingest, lineage, attribution, lifecycle, aggregate, and
// serving layers in one deterministic replay slice.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleParity001_RawReplayPropagatesThroughFinalOutputs(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("parity")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-replay",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(10 * time.Second), Org: h.org, Gateway: "gw-replay",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(20 * time.Second), Org: h.org, Gateway: "gw-replay",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(20 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.raw_events WHERE org = ?`, h.org); got != 4 {
		t.Fatalf("raw replay rows = %d, want 4", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.normalized_stream_trace WHERE org = ?`, h.org); got != 2 {
		t.Fatalf("normalized_stream_trace rows = %d, want 2", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.normalized_ai_stream_status WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("normalized_ai_stream_status rows = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current WHERE canonical_session_key = ? AND attribution_status = 'resolved'`, key); got != 1 {
		t.Fatalf("canonical_session_current did not produce one resolved row for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND is_terminal_tail_artifact = 0`, key); got != 1 {
		t.Fatalf("canonical_status_hours did not produce one active hour for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_status_samples WHERE canonical_session_key = ?`, key); got != 1 {
		t.Fatalf("api_status_samples did not expose one row for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_stream_sessions WHERE canonical_session_key = ?`, key); got != 1 {
		t.Fatalf("api_stream_sessions did not expose one row for %s", key)
	}
}

func TestRuleParity002_ReplaySliceCoversAllSemanticLayers(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(10 * time.Minute)
	streamID := uid("cover")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	windowStart := ts.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.1","hardware":[{"pipeline":"text-to-image","model_id":"model-b","gpu_info":[{"id":"gpu-b","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-parity",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(5 * time.Second), Org: h.org, Gateway: "gw-parity",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(5 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(20 * time.Second), Org: h.org, Gateway: "gw-parity",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":14.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(20 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.raw_events WHERE org = ? AND gateway = 'gw-parity'`, h.org); got != 3 {
		t.Fatalf("raw parity slice = %d, want 3 non-capability service events", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current WHERE canonical_session_key = ? AND startup_outcome = 'success' AND attribution_status = 'resolved'`, key); got != 1 {
		t.Fatalf("canonical_session_current did not preserve success + resolved semantics for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND status_samples = 1 AND is_terminal_tail_artifact = 0`, key); got != 1 {
		t.Fatalf("canonical_status_hours did not preserve aggregate semantics for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-parity' AND pipeline_id = 'text-to-image' AND requested_sessions = 1 AND startup_success_sessions = 1`, h.org, windowStart); got != 1 {
		t.Fatalf("api_network_demand_by_org did not preserve serving parity for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND startup_success_sessions = 1`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_sla_compliance_by_org did not preserve serving parity for %s", key)
	}
}
