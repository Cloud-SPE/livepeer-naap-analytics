//go:build validation

package validation

// ── RULE-INGEST-001 ──────────────────────────────────────────────────────────
// Supported Envelope And Family Contract
// Every accepted event must have a non-empty event_id, event_type, and a
// parseable event_ts. The ingest MV (004_ingest_mvs.sql) already guards
// against blank id/type from Kafka; these tests verify the invariant holds
// on the events table itself and catches any bypass path.

import (
	"fmt"
	"testing"
	"time"
)

func TestRuleIngest001_AcceptedEventsHaveNonBlankEnvelope(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       `{"stream_id":"s1","request_id":"r1","type":"gateway_receive_stream_request"}`,
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       `{"stream_id":"s1","pipeline":"streamdiffusion","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"}}`,
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "discovery_results", EventTs: ts, Org: h.org,
			Data:       `[{"address":"0xabc","latency_ms":"50","url":"https://orch.example.com"}]`,
			IngestedAt: ts,
		},
	})

	// All accepted events must have non-empty event_id and event_type.
	blank := h.queryInt(t,
		`SELECT countIf(event_id = '' OR event_type = '') FROM naap.accepted_raw_events WHERE org = ?`,
		h.org)
	if blank != 0 {
		t.Errorf("RULE-INGEST-001: %d events have blank event_id or event_type (expected 0)", blank)
	}
}

func TestRuleIngest001_SupportedFamiliesAreContracted(t *testing.T) {
	// Verify that the only event_type values in this org are from the contracted family set.
	// Any unexpected type leaking through signals a routing or schema problem.
	h := newHarness(t)
	ts := anchor()

	contractedFamilies := []string{
		"stream_trace", "ai_stream_status", "ai_stream_events",
		"stream_ingest_metrics", "network_capabilities",
		"discovery_results", "create_new_payment",
	}

	for i, family := range contractedFamilies {
		h.insert(t, []rawEvent{{
			EventID: uid("e"), EventType: family, EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":"s%d","type":"gateway_receive_stream_request"}`, i),
			IngestedAt: ts,
		}})
	}

	contracted := h.queryInt(t, `
		SELECT countIf(event_type IN (
			'stream_trace','ai_stream_status','ai_stream_events',
			'stream_ingest_metrics','network_capabilities',
			'discovery_results','create_new_payment'
		))
		FROM naap.accepted_raw_events WHERE org = ?`, h.org)

	total := h.queryInt(t,
		`SELECT count() FROM naap.accepted_raw_events WHERE org = ?`, h.org)

	if contracted != total {
		t.Errorf("RULE-INGEST-001: %d of %d events have non-contracted event_type", total-contracted, total)
	}
}

// ── RULE-INGEST-002 ──────────────────────────────────────────────────────────
// Family-Specific Minimum Raw Elements
// stream_trace and ai_stream_status must carry data.stream_id.
// create_new_payment must carry faceValue, manifestID, sender, recipient, etc.

func TestRuleIngest002_StreamFamiliesHaveStreamId(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Insert one valid (has stream_id) and one invalid (missing stream_id) of each family.
	h.insert(t, []rawEvent{
		// Valid stream_trace
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: `{"stream_id":"s1","request_id":"r1","type":"gateway_receive_stream_request"}`, IngestedAt: ts},
		// Invalid stream_trace — no stream_id
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: `{"type":"gateway_receive_stream_request","request_id":"r2"}`, IngestedAt: ts},
		// Valid ai_stream_status
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: `{"stream_id":"s2","pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, IngestedAt: ts},
		// Invalid ai_stream_status — no stream_id
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: `{"pipeline":"p","state":"ONLINE"}`, IngestedAt: ts},
	})

	// Detectable: stream_trace/ai_stream_status with blank stream_id in data.
	missing := h.queryInt(t, `
		SELECT countIf(JSONExtractString(data, 'stream_id') = '')
		FROM naap.accepted_raw_events
		WHERE org = ? AND event_type IN ('stream_trace', 'ai_stream_status')`,
		h.org)

	// We deliberately inserted 2 invalid events above — assert they are detectable.
	if missing != 2 {
		t.Errorf("RULE-INGEST-002: expected 2 detectable events with missing stream_id, got %d", missing)
	}
}

func TestRuleIngest002_PaymentHasRequiredFields(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Valid payment.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "create_new_payment", EventTs: ts, Org: h.org,
		Data: `{
			"sessionID":"sess1","manifestID":"35_streamdiffusion-sdxl",
			"requestID":"req1","sender":"0xsender","recipient":"0xrecipient",
			"orchestrator":"https://orch.example.com","faceValue":"2402400000000000 WEI",
			"numTickets":"2","price":"1635 wei/pixel","winProb":"0.001"
		}`,
		IngestedAt: ts,
	}})

	// A payment missing faceValue is detectable.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "create_new_payment", EventTs: ts, Org: h.org,
		Data:       `{"sessionID":"sess2","manifestID":"35_streamdiffusion-sdxl","requestID":"req2"}`,
		IngestedAt: ts,
	}})

	missingFaceValue := h.queryInt(t, `
		SELECT countIf(
			JSONExtractString(data, 'faceValue') = '' OR
			JSONExtractString(data, 'manifestID') = '' OR
			JSONExtractString(data, 'sender') = '' OR
			JSONExtractString(data, 'recipient') = ''
		)
		FROM naap.accepted_raw_events
		WHERE org = ? AND event_type = 'create_new_payment'`, h.org)

	if missingFaceValue != 1 {
		t.Errorf("RULE-INGEST-002: expected 1 payment with missing required fields, got %d", missingFaceValue)
	}
}

// ── RULE-INGEST-003 ──────────────────────────────────────────────────────────
// Ignored Raw Families And Subtypes Must Be Explicit
// non-core app traces and scope-client traces must not contribute to canonical
// started-session facts.

func TestRuleIngest003_NonCoreAppStreamTraceDoesNotCountAsStarted(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")
	requestID := "r1"
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"app_send_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"app_param_update"}`, streamID, requestID),
			IngestedAt: ts.Add(time.Second),
		},
	})

	started := h.queryInt(t,
		`SELECT countIf(started = 1) FROM naap.canonical_session_latest WHERE canonical_session_key = ?`,
		key)
	if started != 0 {
		t.Errorf("RULE-INGEST-003: non-core app traces contributed %d to canonical started semantics (expected 0)", started)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_selection_events WHERE org = ?`, h.org); got != 0 {
		t.Errorf("RULE-INGEST-003: non-core app traces created %d selection-centered events (expected 0)", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current_store FINAL WHERE org = ?`, h.org); got != 0 {
		t.Errorf("RULE-INGEST-003: non-core app traces created %d selection-centered session rows (expected 0)", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.ignored_raw_event_diagnostics WHERE org = ? AND ignored_reason = 'ignored_stream_trace_non_core_app'`, h.org); got != 2 {
		t.Errorf("RULE-INGEST-003: non-core app diagnostics rows = %d, want 2", got)
	}
}

func TestRuleIngest003_ScopeClientTracesDoNotCountAsStarted(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Scope client traces have no stream_id; they must not produce canonical session rows.
	scopeTypes := []string{
		"stream_heartbeat", "pipeline_load_start", "pipeline_loaded",
		"session_created", "stream_started", "playback_ready",
		"session_closed", "stream_stopped", "pipeline_unloaded",
		"websocket_connected", "websocket_disconnected",
	}

	var events []rawEvent
	for _, typ := range scopeTypes {
		events = append(events, rawEvent{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			// No stream_id — matches scope client shape from the rule.
			Data:       fmt.Sprintf(`{"type":%q,"client_source":"scope"}`, typ),
			IngestedAt: ts,
		})
	}
	h.insert(t, events)

	if h.queryInt(t, `SELECT count() FROM naap.canonical_session_latest WHERE org = ?`, h.org) != 0 {
		t.Errorf("RULE-INGEST-003: scope client traces unexpectedly produced canonical session rows")
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current_store FINAL WHERE org = ?`, h.org); got != 0 {
		t.Errorf("RULE-INGEST-003: scope client traces created %d selection-centered session rows", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.ignored_raw_event_diagnostics WHERE org = ? AND ignored_reason = 'ignored_stream_trace_scope_client_noise'`, h.org); got != uint64(len(scopeTypes)) {
		t.Errorf("RULE-INGEST-003: scope client diagnostics rows = %d, want %d", got, len(scopeTypes))
	}
}

func TestRuleIngest003_UnknownTraceTypesAreDetectable(t *testing.T) {
	// Any trace data.type not in the contracted or explicit-ignore set is a signal
	// that upstream added a new subtype. The harness detects it; a human decides
	// whether to add it to the ignore list or the contracted set.
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		// Contracted type
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")),
			IngestedAt: ts},
		// Unknown type
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"some_new_unknown_type_xyz"}`, uid("s")),
			IngestedAt: ts},
	})

	unknown := h.queryInt(t, `
		SELECT count()
		FROM naap.ignored_raw_events
		WHERE org = ?
		  AND event_type = 'stream_trace'
		  AND event_subtype = 'some_new_unknown_type_xyz'
		  AND ignore_reason = 'unsupported_stream_trace_type'`, h.org)

	if unknown != 1 {
		t.Errorf("RULE-INGEST-003: expected 1 unknown trace type in ignored diagnostics, got %d", unknown)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.ignored_raw_event_diagnostics WHERE org = ? AND ignored_reason = 'unsupported_stream_trace_type'`, h.org); got != 1 {
		t.Errorf("RULE-INGEST-003: expected 1 unsupported_stream_trace_type diagnostic row, got %d", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_selection_events WHERE org = ?`, h.org); got != 0 {
		t.Errorf("RULE-INGEST-003: unsupported trace types should not inflate selection events; got %d", got)
	}
}
