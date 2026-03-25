//go:build validation

package validation

// ── RULE-LINEAGE-001 ─────────────────────────────────────────────────────────
// Canonical accepted-event identity must remain replay-safe from raw events to
// canonical facts.
//
// ── RULE-LINEAGE-002 ─────────────────────────────────────────────────────────
// Derived outputs must preserve traceable lineage coverage, including orphaned
// status-only sessions that never had a true start edge.

import (
	"fmt"
	"testing"
	"time"
)

func TestRuleLineage001_DuplicateEventIdCollapsesUnderFinal(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	eventID := uid("dup")
	streamID := uid("s")
	requestID := uid("req")

	h.insert(t, []rawEvent{
		{
			EventID: eventID, EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: eventID, EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
	})

	if h.queryInt(t, `SELECT count() FROM naap.events FINAL WHERE org = ? AND event_id = ?`, h.org, eventID) != 1 {
		t.Errorf("RULE-LINEAGE-001: duplicate raw event_id did not collapse under FINAL")
	}
}

func TestRuleLineage001_ReplayDoesNotInflateCanonicalFacts(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	eventID := uid("replay")
	streamID := uid("s")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: eventID, EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: eventID, EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
	})

	if h.queryInt(t, `SELECT count() FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-LINEAGE-001: replay produced duplicate canonical session rows for %s", key)
	}
	if h.queryInt(t, `SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-LINEAGE-001: replay inflated started semantics for %s", key)
	}
}

func TestRuleLineage001_AllAcceptedEventsHaveNonBlankId(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r1","type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org, Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r2","pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "create_new_payment", EventTs: ts, Org: h.org, Data: `{"sessionID":"s","manifestID":"m","faceValue":"100 WEI","sender":"0xa","recipient":"0xb","orchestrator":"https://o","numTickets":"1","price":"1 wei","winProb":"0.001"}`, IngestedAt: ts},
	})

	if h.queryInt(t, `SELECT countIf(event_id = '') FROM naap.events WHERE org = ?`, h.org) != 0 {
		t.Errorf("RULE-LINEAGE-001: blank event_id leaked into accepted events")
	}
}

func TestRuleLineage002_StatusWithoutTraceStartRemainsDetectable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	knownStream := uid("known")
	knownRequest := uid("req")
	knownKey := canonicalSessionKey(h.org, knownStream, knownRequest)
	orphanStream := uid("orphan")
	orphanRequest := uid("req")
	orphanKey := canonicalSessionKey(h.org, orphanStream, orphanRequest)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, knownStream, knownRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`, knownStream, knownRequest),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`, orphanStream, orphanRequest),
			IngestedAt: ts,
		},
	})

	if h.queryInt(t, `SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, knownKey) != 1 {
		t.Errorf("RULE-LINEAGE-002: known session did not retain started=1")
	}
	if h.queryInt(t, `SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, orphanKey) != 0 {
		t.Errorf("RULE-LINEAGE-002: orphan status session was incorrectly coerced into started=1")
	}
	if h.queryInt(t, `SELECT status_sample_count FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, orphanKey) != 1 {
		t.Errorf("RULE-LINEAGE-002: orphan status session lost its status sample lineage")
	}
}

func TestRuleLineage002_ClosedStreamHasTraceableLineage(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_ingest_stream_closed"}`, streamID, requestID),
			IngestedAt: ts.Add(time.Minute),
		},
	})

	if h.queryInt(t, `SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-LINEAGE-002: closed session lost start lineage")
	}
	if h.queryInt(t, `SELECT toUInt64(completed) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-LINEAGE-002: closed session lost close lineage")
	}
}
