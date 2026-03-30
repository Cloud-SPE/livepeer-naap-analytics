//go:build validation

package validation

// ── RULE-AGGREGATE-004 ────────────────────────────────────────────────────────
// Status-hour and reliability outputs must preserve degraded-mode splits and
// same-hour failed no-output sessions without coercing missing values to zero.
//
// ── RULE-LIFECYCLE-009 ───────────────────────────────────────────────────────
// Tail filtering must be narrow and deterministic: only true end-of-session
// rollover noise is suppressed from serving outputs.
//
// ── RULE-TYPED_RAW-002 ───────────────────────────────────────────────────────
// Capability payloads must expand into historical snapshot rows without losing
// orchestrator identity.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleAggregate004_DegradedModesRemainSplitInReliabilityFacts(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a"}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":"deg-in","request_id":"req-in","type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":"deg-in","request_id":"req-in","pipeline":"text-to-image","state":"DEGRADED_INPUT","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":6.0,"restart_count":0,"last_error":""}}`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":"deg-out","request_id":"req-out","type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, orchAddr, orchURI),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":"deg-out","request_id":"req-out","pipeline":"text-to-image","state":"DEGRADED_INFERENCE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":5.0,"restart_count":0,"last_error":""}}`, orchAddr, orchURI),
			IngestedAt: ts.Add(time.Second),
		},
	})

	hour := ts.Truncate(time.Hour)
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_input_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_input_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_inference_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_inference_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_input_samples > 0 or degraded_inference_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 2 {
		t.Errorf("RULE-AGGREGATE-004: degraded_count did not total 2")
	}
}

func TestRuleAggregate004_SameHourFailedNoOutputSessionIsRetained(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(10 * time.Minute)
	streamID := uid("fail")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":0.0,"restart_count":0,"last_error":"boom"}}`, streamID, requestID),
			IngestedAt: ts.Add(time.Second),
		},
	})

	if h.queryInt(t, `SELECT error_samples FROM naap.canonical_status_hours WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-AGGREGATE-004: same-hour failed no-output session lost error_samples")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ?`, key) != 0 {
		t.Errorf("RULE-AGGREGATE-004: same-hour failed no-output session was incorrectly tail-filtered")
	}
}

func TestRuleLifecycle009_TailFilteringIsNarrowAndDeterministic(t *testing.T) {
	h := newHarness(t)
	baseHour := anchor().Add(-time.Hour)
	streamID := uid("tail")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: baseHour.Add(-10 * time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image"}`, streamID, requestID),
			IngestedAt: baseHour.Add(-10 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: baseHour.Add(10 * time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"LOADING","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":0.0,"restart_count":0,"last_error":""}}`, streamID, requestID),
			IngestedAt: baseHour.Add(10 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: baseHour.Add(time.Hour + 5*time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"LOADING","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":0.0,"restart_count":0,"last_error":""}}`, streamID, requestID),
			IngestedAt: baseHour.Add(time.Hour + 5*time.Minute),
		},
	})

	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour) != 0 {
		t.Errorf("RULE-LIFECYCLE-009: previous active hour was incorrectly tail-filtered")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour.Add(time.Hour)) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: terminal rollover hour was not tail-filtered")
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE org = ? AND is_terminal_tail_artifact = 0`, h.org) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: serving reliability kept tail noise instead of filtering it")
	}
}

func TestRuleLifecycle008And009_DemandUsesStartHourWhileReliabilityUsesStatusHour(t *testing.T) {
	h := newHarness(t)
	startTS := anchor().Add(-30 * time.Minute)
	statusTS := startTS.Add(70 * time.Minute)
	streamID := uid("cross-hour")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	startHour := startTS.Truncate(time.Hour)
	statusHour := statusTS.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: startTS, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: startTS,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: startTS, Org: h.org, Gateway: "gw-cross-hour",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: startTS,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: statusTS, Org: h.org, Gateway: "gw-cross-hour",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":11.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: statusTS,
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_demand_input_current WHERE canonical_session_key = ? AND window_start = ?`, key, startHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-008: demand row count at session start hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_demand_input_current WHERE canonical_session_key = ? AND window_start = ?`, key, statusHour); got != 0 {
		t.Fatalf("RULE-LIFECYCLE-008: demand row leaked into status hour, got %d", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ? AND is_terminal_tail_artifact = 0 AND status_samples = 1`, key, statusHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-009: status-hour row count at active hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour' AND pipeline_id = 'text-to-image'`, h.org, startHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-008: demand serving row at start hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 1 {
		t.Fatalf("RULE-AGGREGATE-002: perf-backed demand row at status hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT requested_sessions FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 0 {
		t.Fatalf("RULE-AGGREGATE-002: perf-only demand row requested_sessions = %d, want 0", got)
	}
	if got := h.queryFloat(t, `SELECT avg_output_fps FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 11 {
		t.Fatalf("RULE-AGGREGATE-002: perf-only demand row avg_output_fps = %v, want 11", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND known_sessions_count = 1 AND startup_failed_sessions = 1`, h.org, statusHour, orchAddr); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-009: SLA serving row at status hour = %d, want 1", got)
	}
}

func TestRuleAggregate002And005_DemandAndSLAUseContractedFailureSemantics(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(20 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	successStream := uid("success")
	successRequest := uid("req")
	zeroStream := uid("zero")
	zeroRequest := uid("req")

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(4 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(4 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(10 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, zeroStream, zeroRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(12 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":0.0,"restart_count":0,"last_error":"boom"}}`, zeroStream, zeroRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(12 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT effective_failed_sessions FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-contract' AND pipeline_id = 'text-to-image'`, h.org, windowStart); got != 1 {
		t.Fatalf("RULE-AGGREGATE-002: effective_failed_sessions = %d, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT round(effective_success_rate, 6) FROM naap.api_network_demand_by_org WHERE org = ? AND window_start = ? AND gateway = 'gw-contract' AND pipeline_id = 'text-to-image'`, h.org, windowStart); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-002: effective_success_rate = %v, want 0.5", got)
	}
	if got := h.queryFloat(t, `SELECT round(health_signal_coverage_ratio, 6) FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.833333 {
		t.Fatalf("RULE-AGGREGATE-005: health_signal_coverage_ratio = %v, want 0.833333", got)
	}
	if got := h.queryFloat(t, `SELECT round(effective_success_rate, 6) FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: effective_success_rate = %v, want 0.5", got)
	}
	if got := h.queryFloat(t, `SELECT round(output_viability_rate, 6) FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: output_viability_rate = %v, want 0.5", got)
	}
	if got := h.queryFloat(t, `SELECT round(sla_score, 6) FROM naap.api_sla_compliance_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 50 {
		t.Fatalf("RULE-AGGREGATE-005: sla_score = %v, want 50", got)
	}
}

func TestRuleAggregate004AndServing001_HardwareUnresolvedRowsStayOutOfGPUMetricsButRemainVisibleInGPUDemand(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(25 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	streamID := uid("hwless")
	requestID := uid("req")
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-hwless"}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-hwless",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-hwless",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.api_gpu_metrics_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ?`, h.org, windowStart, orchAddr); got != 0 {
		t.Fatalf("RULE-AGGREGATE-004: hardware-unresolved row leaked into gpu metrics, got %d rows", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_gpu_network_demand_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND gpu_identity_status = 'hardware_unresolved'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-SERVING-001: gpu-sliced demand row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_gpu_network_demand_by_org WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND gpu_id IS NULL`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-SERVING-001: gpu-sliced demand did not preserve null gpu_id for hardware-unresolved row")
	}
}

func TestRuleTypedRaw002_CapabilityArrayFansOutWithoutBlankAddresses(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchA := strings.ToLower(fmt.Sprintf("0x%s", uid("a")))
	orchB := strings.ToLower(fmt.Sprintf("0x%s", uid("b")))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[
			{"address":%q,"local_address":"orch-a","uri":"https://a.example.com","version":"0.7.0"},
			{"address":%q,"local_address":"orch-b","uri":"https://b.example.com","version":"0.7.0"},
			{"address":"","local_address":"blank","uri":"https://blank.example.com","version":"0.7.0"}
		]`, orchA, orchB),
		IngestedAt: ts,
	}})

	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ?`, h.org) != 2 {
		t.Errorf("RULE-TYPED_RAW-002: capability fanout did not produce exactly 2 canonical snapshots")
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ? AND orch_address = ''`, h.org) != 0 {
		t.Errorf("RULE-TYPED_RAW-002: blank orchestrator address leaked into capability snapshots")
	}
}

func TestRuleTypedRaw002_LatestCapabilityStateKeepsNameFallback(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchWithName := strings.ToLower(fmt.Sprintf("0x%s", uid("named")))
	orchNoName := strings.ToLower(fmt.Sprintf("0x%s", uid("addr")))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[
			{"address":%q,"local_address":"friendly-name","uri":"https://named.example.com","version":"0.7.0"},
			{"address":%q,"local_address":"","uri":"https://addr.example.com","version":"0.7.0"}
		]`, orchWithName, orchNoName),
		IngestedAt: ts,
	}})

	if got := h.queryString(t, `SELECT name FROM naap.canonical_latest_orchestrator_state WHERE orch_address = ?`, orchWithName); got != "friendly-name" {
		t.Errorf("RULE-TYPED_RAW-002: latest capability name = %q, want friendly-name", got)
	}
	if got := h.queryString(t, `SELECT name FROM naap.canonical_latest_orchestrator_state WHERE orch_address = ?`, orchNoName); got != orchNoName {
		t.Errorf("RULE-TYPED_RAW-002: latest capability fallback name = %q, want %q", got, orchNoName)
	}
}
