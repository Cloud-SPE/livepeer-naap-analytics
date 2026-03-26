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
	if h.queryInt(t, `SELECT degraded_input_count FROM naap.serving_orch_reliability_hourly WHERE org = ? AND hour = ? AND orch_address = ?`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_input_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT degraded_inference_count FROM naap.serving_orch_reliability_hourly WHERE org = ? AND hour = ? AND orch_address = ?`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_inference_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT degraded_count FROM naap.serving_orch_reliability_hourly WHERE org = ? AND hour = ? AND orch_address = ?`, h.org, hour, orchAddr) != 2 {
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

	if h.queryInt(t, `SELECT error_samples FROM naap.fact_workflow_status_hours WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-AGGREGATE-004: same-hour failed no-output session lost error_samples")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.fact_workflow_status_hours WHERE canonical_session_key = ?`, key) != 0 {
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

	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.fact_workflow_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour) != 0 {
		t.Errorf("RULE-LIFECYCLE-009: previous active hour was incorrectly tail-filtered")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.fact_workflow_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour.Add(time.Hour)) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: terminal rollover hour was not tail-filtered")
	}
	if h.queryInt(t, `SELECT count() FROM naap.serving_orch_reliability_hourly WHERE org = ?`, h.org) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: serving reliability kept tail noise instead of filtering it")
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

	if h.queryInt(t, `SELECT count() FROM naap.capability_snapshots WHERE org = ?`, h.org) != 2 {
		t.Errorf("RULE-TYPED_RAW-002: capability fanout did not produce exactly 2 canonical snapshots")
	}
	if h.queryInt(t, `SELECT count() FROM naap.capability_snapshots WHERE org = ? AND orch_address = ''`, h.org) != 0 {
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

	if got := h.queryString(t, `SELECT name FROM naap.serving_latest_orchestrator_state WHERE orch_address = ?`, orchWithName); got != "friendly-name" {
		t.Errorf("RULE-TYPED_RAW-002: latest capability name = %q, want friendly-name", got)
	}
	if got := h.queryString(t, `SELECT name FROM naap.serving_latest_orchestrator_state WHERE orch_address = ?`, orchNoName); got != orchNoName {
		t.Errorf("RULE-TYPED_RAW-002: latest capability fallback name = %q, want %q", got, orchNoName)
	}
}
