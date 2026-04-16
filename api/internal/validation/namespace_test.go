//go:build validation

package validation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTierContract_RawAndNormalizedAliasesExposeExpectedRows(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("stream")
	requestID := uid("req")

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`, streamID, requestID),
			IngestedAt: ts,
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.raw_events WHERE org = ?`, h.org); got != 2 {
		t.Fatalf("raw_events row count = %d, want 2", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.normalized_stream_trace WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("normalized_stream_trace row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.normalized_ai_stream_status WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("normalized_ai_stream_status row count = %d, want 1", got)
	}
}

func TestTierContract_CanonicalAndAPIViewsMirrorServingSurfaces(t *testing.T) {
	h := newHarness(t)
	ts := time.Now().UTC().Add(-30 * time.Second)
	streamID := uid("stream")
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
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Fatalf("canonical_session_current attribution_status = %q, want resolved", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_samples_recent WHERE canonical_session_key = ?`, key); got != 1 {
		t.Fatalf("canonical_status_samples_recent count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_current_active_stream_state WHERE org = ? AND stream_id = ? AND request_id = ?`, h.org, streamID, requestID); got != 1 {
		t.Fatalf("api_current_active_stream_state count = %d, want 1", got)
	}
}

func TestTierContract_TailFilteringRemainsVisibleThroughAPISurfaces(t *testing.T) {
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

	if got := h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour.Add(time.Hour)); got != 1 {
		t.Fatalf("canonical_status_hours terminal tail flag = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("api_hourly_streaming_sla count = %d, want 1", got)
	}
}

func TestTierContract_APIActiveStreamStateExcludesBlankStreamIDs(t *testing.T) {
	h := newHarness(t)
	ts := time.Now().UTC()

	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_active_stream_state_latest_store
		(
			canonical_session_key, event_id, sample_ts, org, stream_id, request_id, gateway,
			pipeline, model_id, orch_address, attribution_status, attribution_reason, state,
			output_fps, input_fps, e2e_latency_ms, started_at, last_seen, completed,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		VALUES
		(?, ?, ?, ?, '', '', 'gw-a', 'text-to-image', NULL, NULL, 'resolved', 'ok', 'ONLINE', 12.0, 0.0, NULL, NULL, ?, 0, 'test-run', 'validation', ?),
		(?, ?, ?, ?, ?, ?, 'gw-a', 'text-to-image', NULL, NULL, 'resolved', 'ok', 'ONLINE', 12.0, 0.0, NULL, NULL, ?, 0, 'test-run', 'validation', ?)
	`,
		h.org+"|_missing_stream|_missing_request", uid("e"), ts, h.org, ts, ts,
		h.org+"|stream-ok|req-ok", uid("e"), ts, h.org, "stream-ok", "req-ok", ts, ts,
	); err != nil {
		t.Fatalf("insert canonical_active_stream_state_latest_store: %v", err)
	}

	if got := h.queryInt(t, `SELECT count() FROM naap.api_current_active_stream_state WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("api_current_active_stream_state count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_current_active_stream_state WHERE org = ? AND stream_id = ''`, h.org); got != 0 {
		t.Fatalf("api_current_active_stream_state blank stream count = %d, want 0", got)
	}
}

func TestTierContract_APIActiveStreamStateDoesNotJoinSessionStateAcrossOrgs(t *testing.T) {
	h := newHarness(t)
	ts := time.Now().UTC()
	otherOrg := h.org + "_other"
	sessionKey := "shared-session-key"

	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_active_stream_state_latest_store
		(
			canonical_session_key, event_id, sample_ts, org, stream_id, request_id, gateway,
			pipeline, model_id, orch_address, attribution_status, attribution_reason, state,
			output_fps, input_fps, e2e_latency_ms, started_at, last_seen, completed,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		VALUES
		(?, ?, ?, ?, ?, ?, 'gw-a', 'text-to-image', NULL, '0xaaa', 'resolved', 'ok', 'ONLINE', 12.0, 0.0, NULL, NULL, ?, 0, 'test-run', 'validation', ?)
	`,
		sessionKey, uid("e"), ts, h.org, "stream-ok", "req-ok", ts, ts,
	); err != nil {
		t.Fatalf("insert canonical_active_stream_state_latest_store: %v", err)
	}

	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_session_current_store
		(
			canonical_session_key, org, gpu_id, last_seen, startup_outcome,
			attribution_reason, attribution_status, attributed_orch_address, materialized_at
		)
		VALUES
		(?, ?, 'gpu-foreign', ?, 'success', 'ok', 'resolved', '0xforeign', ?)
	`,
		sessionKey, otherOrg, ts, ts,
	); err != nil {
		t.Fatalf("insert canonical_session_current_store: %v", err)
	}

	if got := h.queryInt(t, `SELECT count() FROM naap.api_current_active_stream_state WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != 1 {
		t.Fatalf("api_current_active_stream_state count = %d, want 1", got)
	}
	if got := h.queryString(t, `SELECT ifNull(gpu_id, '') FROM naap.api_current_active_stream_state WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "" {
		t.Fatalf("api_current_active_stream_state gpu_id = %q, want empty", got)
	}
	if got := h.queryString(t, `SELECT ifNull(attributed_orch_address, '') FROM naap.api_current_active_stream_state WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "" {
		t.Fatalf("api_current_active_stream_state attributed_orch_address = %q, want empty", got)
	}
}
