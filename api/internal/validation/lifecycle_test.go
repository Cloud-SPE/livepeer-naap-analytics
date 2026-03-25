//go:build validation

package validation

// ── RULE-LIFECYCLE-002 ────────────────────────────────────────────────────────
// Trace Subtypes Must Map To Fixed Lifecycle Semantics
// Contracted trace types must populate canonical session facts once, not via
// ad hoc aggregate logic.
//
// ── RULE-LIFECYCLE-003 ────────────────────────────────────────────────────────
// Startup Outcome Classification Must Use Fixed Precedence
// Canonical startup outcome is derived from fixed edges: success when playable
// is seen, excused when no orchestrator is seen without success, otherwise
// unexcused for started sessions.
//
// ── RULE-LIFECYCLE-007 ────────────────────────────────────────────────────────
// Session Health Signals Must Be Explicit Additive Facts
// Sessions with no status or playable evidence remain visible with zero health
// coverage instead of being dropped from canonical facts.

import (
	"fmt"
	"testing"
	"time"
)

func TestRuleLifecycle002_ContractedSubtypesDriveCanonicalFacts(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	startStream := uid("start")
	startRequest := uid("req")
	startKey := canonicalSessionKey(h.org, startStream, startRequest)
	closeStream := uid("close")
	closeRequest := uid("req")
	closeKey := canonicalSessionKey(h.org, closeStream, closeRequest)
	noOrchStream := uid("noorch")
	noOrchRequest := uid("req")
	noOrchKey := canonicalSessionKey(h.org, noOrchStream, noOrchRequest)
	swapStream := uid("swap")
	swapRequest := uid("req")
	swapKey := canonicalSessionKey(h.org, swapStream, swapRequest)

	events := []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`,
				startStream, startRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_ingest_stream_closed"}`,
				closeStream, closeRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`,
				noOrchStream, noOrchRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(3 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_no_orchestrators_available"}`,
				noOrchStream, noOrchRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(4 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`,
				swapStream, swapRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(5 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"orchestrator_swap"}`,
				swapStream, swapRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(6 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments"}`,
				swapStream, swapRequest),
			IngestedAt: ts,
		},
	}
	h.insert(t, events)

	if h.queryInt(t, `SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, startKey) != 1 {
		t.Errorf("RULE-LIFECYCLE-002: started trace did not set started=1 for %s", startKey)
	}
	if h.queryInt(t, `SELECT toUInt64(completed) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, closeKey) != 1 {
		t.Errorf("RULE-LIFECYCLE-002: close trace did not set completed=1 for %s", closeKey)
	}
	if h.queryInt(t, `SELECT toUInt64(no_orch) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, noOrchKey) != 1 {
		t.Errorf("RULE-LIFECYCLE-002: no-orch trace did not set no_orch=1 for %s", noOrchKey)
	}
	if h.queryInt(t, `SELECT swap_count FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, swapKey) != 1 {
		t.Errorf("RULE-LIFECYCLE-002: swap trace did not set swap_count=1 for %s", swapKey)
	}
	if h.queryInt(t, `SELECT toUInt64(playable_seen) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, swapKey) != 1 {
		t.Errorf("RULE-LIFECYCLE-002: playable trace did not set playable_seen=1 for %s", swapKey)
	}
}

func TestRuleLifecycle003_CleanSuccessStreamUsesCanonicalPrecedence(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("success")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments"}`, streamID, requestID),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_ingest_stream_closed"}`, streamID, requestID),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT startup_outcome FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key); got != "success" {
		t.Errorf("RULE-LIFECYCLE-003: clean success startup_outcome = %q, want success", got)
	}
	if h.queryInt(t, `SELECT toUInt64(completed) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-LIFECYCLE-003: clean success session did not retain completed=1")
	}
}

func TestRuleLifecycle003_NoOrchAndUnexcusedFailuresRemainDistinct(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	noOrchStream := uid("noorch")
	noOrchRequest := uid("req")
	noOrchKey := canonicalSessionKey(h.org, noOrchStream, noOrchRequest)
	unexcusedStream := uid("unexcused")
	unexcusedRequest := uid("req")
	unexcusedKey := canonicalSessionKey(h.org, unexcusedStream, unexcusedRequest)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, noOrchStream, noOrchRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_no_orchestrators_available"}`, noOrchStream, noOrchRequest),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, unexcusedStream, unexcusedRequest),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT startup_outcome FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, noOrchKey); got != "excused" {
		t.Errorf("RULE-LIFECYCLE-003: no-orch startup_outcome = %q, want excused", got)
	}
	if got := h.queryString(t, `SELECT startup_outcome FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, unexcusedKey); got != "unexcused" {
		t.Errorf("RULE-LIFECYCLE-003: unexcused startup_outcome = %q, want unexcused", got)
	}
}

func TestRuleLifecycle007_DarkSessionsRemainVisibleWithZeroCoverage(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	litStream := uid("lit")
	litRequest := uid("req")
	litKey := canonicalSessionKey(h.org, litStream, litRequest)
	darkStream := uid("dark")
	darkRequest := uid("req")
	darkKey := canonicalSessionKey(h.org, darkStream, darkRequest)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, litStream, litRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""},"inference_status":{"fps":8.0,"restart_count":0,"last_error":""}}`, litStream, litRequest),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, darkStream, darkRequest),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if h.queryInt(t, `SELECT health_signal_count FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, darkKey) != 0 {
		t.Errorf("RULE-LIFECYCLE-007: dark session should keep health_signal_count=0")
	}
	if h.queryInt(t, `SELECT health_expected_signal_count FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, darkKey) != 3 {
		t.Errorf("RULE-LIFECYCLE-007: dark session should keep health_expected_signal_count=3")
	}
	if got := h.queryFloat(t, `SELECT health_signal_coverage_ratio FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, darkKey); got != 0 {
		t.Errorf("RULE-LIFECYCLE-007: dark session coverage ratio = %.2f, want 0", got)
	}
	if got := h.queryFloat(t, `SELECT health_signal_coverage_ratio FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, litKey); got <= 0 {
		t.Errorf("RULE-LIFECYCLE-007: lit session coverage ratio = %.2f, want > 0", got)
	}
}
