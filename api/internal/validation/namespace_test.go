//go:build validation

package validation

import (
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

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Fatalf("canonical_session_latest attribution_status = %q, want resolved", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_status_samples WHERE canonical_session_key = ?`, key); got != 1 {
		t.Fatalf("api_status_samples count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_active_stream_state WHERE org = ? AND stream_id = ? AND request_id = ?`, h.org, streamID, requestID); got != 1 {
		t.Fatalf("api_active_stream_state count = %d, want 1", got)
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
	if got := h.queryInt(t, `SELECT count() FROM naap.api_orchestrator_reliability_hourly WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("api_orchestrator_reliability_hourly count = %d, want 1", got)
	}
}
