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
		`SELECT countIf(event_id = '' OR event_type = '') FROM naap.events WHERE org = ?`,
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
		FROM naap.events WHERE org = ?`, h.org)

	total := h.queryInt(t,
		`SELECT count() FROM naap.events WHERE org = ?`, h.org)

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
		FROM naap.events
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
		FROM naap.events
		WHERE org = ? AND event_type = 'create_new_payment'`, h.org)

	if missingFaceValue != 1 {
		t.Errorf("RULE-INGEST-002: expected 1 payment with missing required fields, got %d", missingFaceValue)
	}
}

// ── RULE-INGEST-003 ──────────────────────────────────────────────────────────
// Ignored Raw Families And Subtypes Must Be Explicit
// app_send_stream_request traces and scope-client traces (no stream_id) must
// not contribute to canonical started-session facts.

func TestRuleIngest003_AppSendStreamRequestDoesNotCountAsStarted(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")
	requestID := "r1"
	key := canonicalSessionKey(h.org, streamID, requestID)

	// This trace type is in the explicit ignore list (RULE-INGEST-003).
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
		Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"app_send_stream_request"}`, streamID, requestID),
		IngestedAt: ts,
	}})

	started := h.queryInt(t,
		`SELECT toUInt64(started) FROM naap.fact_workflow_sessions WHERE canonical_session_key = ?`,
		key)
	if started != 0 {
		t.Errorf("RULE-INGEST-003: app_send_stream_request contributed %d to canonical started semantics (expected 0)", started)
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
			Data:       fmt.Sprintf(`{"type":%q}`, typ),
			IngestedAt: ts,
		})
	}
	h.insert(t, events)

	if h.queryInt(t, `SELECT count() FROM naap.fact_workflow_sessions WHERE org = ?`, h.org) != 0 {
		t.Errorf("RULE-INGEST-003: scope client traces unexpectedly produced canonical session rows")
	}
}

func TestRuleIngest003_UnknownTraceTypesAreDetectable(t *testing.T) {
	// mv_typed_stream_trace uses a whitelist — only contracted subtypes enter
	// typed_stream_trace. Any subtype not in the whitelist is silently dropped
	// by the MV; it never reaches canonical facts or serving views.
	//
	// This test inserts one contracted type and one unknown type into naap.events,
	// then asserts that typed_stream_trace received only the contracted one.
	// A count > 0 in the unknown check means the MV whitelist is out of sync
	// with reality — the new subtype must be triaged and either added to the
	// whitelist (migration 019) or documented as intentionally excluded.
	//
	// Whitelist source of truth: infra/clickhouse/migrations/019_stream_trace_subtype_whitelist.sql
	// Event catalog reference:   docs/data/EVENT_CATALOG.md §stream_trace subtypes
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		// Contracted type — must appear in typed_stream_trace
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")),
			IngestedAt: ts},
		// Unknown type — must be rejected by the MV whitelist
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"some_new_unknown_type_xyz"}`, uid("s")),
			IngestedAt: ts},
	})

	// Check naap.events (raw) — unknown type must be detectable there for triage.
	unknownInRaw := h.queryInt(t, `
		SELECT countIf(
			JSONExtractString(data, 'type') NOT IN (
				-- contracted gateway lifecycle (EVENT_CATALOG.md §stream_trace subtypes)
				'gateway_receive_stream_request','gateway_ingest_stream_closed',
				'gateway_send_first_ingest_segment','gateway_receive_first_processed_segment',
				'gateway_receive_few_processed_segments','gateway_receive_first_data_segment',
				'gateway_no_orchestrators_available','orchestrator_swap',
				-- runner-side informational (pending EVENT_CATALOG.md entry)
				'runner_receive_stream_request',
				'runner_receive_first_ingest_segment','runner_send_first_processed_segment',
				-- known excluded subtypes (app-client and scope-client instrumentation)
				'app_send_stream_request','app_start_broadcast_stream','app_param_update',
				'app_receive_first_segment','app_user_page_unload',
				'app_capacity_query_response','app_capacity_error_shown',
				'app_stream_terminated_freeze','app_user_page_visibility_change',
				'app_user_input_mode_change',
				'stream_heartbeat','pipeline_load_start','pipeline_loaded',
				'session_created','session_closed','stream_started','stream_stopped',
				'playback_ready','pipeline_unloaded',
				'websocket_connected','websocket_disconnected','error'
			)
		)
		FROM naap.events
		WHERE org = ? AND event_type = 'stream_trace'`, h.org)

	if unknownInRaw != 1 {
		t.Errorf("RULE-INGEST-003: expected 1 unknown trace type detectable in naap.events, got %d", unknownInRaw)
	}

	// Check typed_stream_trace — the MV whitelist must have rejected the unknown type.
	unknownInTyped := h.queryInt(t, `
		SELECT countIf(
			trace_type NOT IN (
				'gateway_receive_stream_request','gateway_ingest_stream_closed',
				'gateway_send_first_ingest_segment','gateway_receive_first_processed_segment',
				'gateway_receive_few_processed_segments','gateway_receive_first_data_segment',
				'gateway_no_orchestrators_available','orchestrator_swap',
				'runner_receive_stream_request',
				'runner_receive_first_ingest_segment','runner_send_first_processed_segment'
			)
		)
		FROM naap.typed_stream_trace
		WHERE org = ?`, h.org)

	if unknownInTyped != 0 {
		t.Errorf("RULE-INGEST-003: MV whitelist leak — %d non-contracted trace types reached typed_stream_trace", unknownInTyped)
	}
}
