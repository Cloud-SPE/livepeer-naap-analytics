//go:build validation

package validation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/resolver"
)

func TestSelectionCenteredResolverSmoke_PopulatesCanonicalSpine(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("resolver_stream")
	requestID := uid("resolver_req")
	sessionKey := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0xResolver%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	events := []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-5 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"resolver-orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-selection","gpu_info":[{"id":"gpu-selection","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, orchURI),
			IngestedAt: ts.Add(-5 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-selection",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(5 * time.Second), Org: h.org, Gateway: "gw-selection",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(5 * time.Second),
		},
	}

	h.insertRaw(t, events)
	stats := h.resolveSelectionCentered(t, ts.Add(-10*time.Minute), ts.Add(2*time.Minute))

	if stats.SelectionEvents != 1 {
		t.Fatalf("selection-centered resolver selection events = %d, want 1", stats.SelectionEvents)
	}
	if stats.Decisions != 1 {
		t.Fatalf("selection-centered resolver decisions = %d, want 1", stats.Decisions)
	}
	if stats.SessionRows != 1 {
		t.Fatalf("selection-centered resolver session rows = %d, want 1", stats.SessionRows)
	}
	if stats.StatusHourRows == 0 {
		t.Fatalf("selection-centered resolver status hour rows = 0, want > 0")
	}

	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_selection_events WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("canonical_selection_events rows = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_selection_attribution_decisions WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("canonical_selection_attribution_decisions rows = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_selection_attribution_current FINAL WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("canonical_selection_attribution_current rows = %d, want 1", got)
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "resolved" {
		t.Fatalf("canonical_selection_attribution_current attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "model-selection" {
		t.Fatalf("canonical_selection_attribution_current canonical_model = %q, want model-selection", got)
	}
	if got := h.queryString(t, `SELECT ifNull(attributed_orch_address, '') FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != orchAddr {
		t.Fatalf("canonical_selection_attribution_current attributed_orch_address = %q, want %q", got, orchAddr)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current_store WHERE org = ?`, h.org); got != 1 {
		t.Fatalf("canonical_session_current_store rows = %d, want 1", got)
	}
	if got := h.queryString(t, `SELECT ifNull(current_selection_event_id, '') FROM naap.canonical_session_current_store WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got == "" {
		t.Fatalf("canonical_session_current_store current_selection_event_id = empty, want populated selection event")
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current_store WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "resolved" {
		t.Fatalf("canonical_session_current_store attribution_status = %q, want resolved", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours_store WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got == 0 {
		t.Fatalf("canonical_status_hours_store rows = 0, want > 0")
	}
}

func TestSelectionCenteredResolverSmoke_LateCapabilitySnapshotRepairsExistingSelection(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("repair_stream")
	requestID := uid("repair_req")
	sessionKey := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0xRepair%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insertRaw(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-selection",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(5 * time.Second), Org: h.org, Gateway: "gw-selection",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(5 * time.Second),
		},
	})

	h.resolveSelectionCentered(t, ts.Add(-time.Minute), ts.Add(time.Minute))
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "unresolved" {
		t.Fatalf("pre-repair attribution_status = %q, want unresolved", got)
	}

	h.insertRaw(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(30 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"repair-orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-repair","gpu_info":[{"id":"gpu-repair","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Minute),
		},
	})

	h.resolveSelectionCentered(t, ts, ts.Add(time.Minute))
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "resolved" {
		t.Fatalf("post-repair attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "model-repair" {
		t.Fatalf("post-repair canonical_model = %q, want model-repair", got)
	}
}

func TestSelectionCenteredResolverSmoke_AutoRepairsLateHistoricalAcceptedRaw(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(-48 * time.Hour)
	streamID := uid("auto_stream")
	requestID := uid("auto_req")
	sessionKey := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0xAuto%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insertRaw(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-auto",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: time.Now().UTC(),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(5 * time.Second), Org: h.org, Gateway: "gw-auto",
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: time.Now().UTC().Add(5 * time.Second),
		},
	})

	bootstrapStart := ts.UTC().Truncate(24 * time.Hour)
	bootstrapEnd := bootstrapStart.Add(24 * time.Hour)
	if _, err := h.resolverEngine.Execute(context.Background(), resolver.RunRequest{
		Mode:  resolver.ModeBackfill,
		Org:   h.org,
		Start: &bootstrapStart,
		End:   &bootstrapEnd,
	}); err != nil {
		t.Fatalf("resolver backfill: %v", err)
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "unresolved" {
		t.Fatalf("backfill attribution_status = %q, want unresolved", got)
	}

	h.insertRaw(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(30 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"late-auto","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-auto","gpu_info":[{"id":"gpu-auto","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, orchURI),
			IngestedAt: time.Now().UTC().Add(10 * time.Minute),
		},
	})

	h.resolveAuto(t)

	if got := h.queryInt(t, `SELECT count() FROM naap.resolver_dirty_partitions FINAL WHERE org = ? AND event_date = ? AND status = 'success'`, h.org, bootstrapStart); got != 1 {
		t.Fatalf("dirty repair success rows = %d, want 1", got)
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "resolved" {
		t.Fatalf("auto repair attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_selection_attribution_current FINAL WHERE org = ? AND canonical_session_key = ?`, h.org, sessionKey); got != "model-auto" {
		t.Fatalf("auto repair canonical_model = %q, want model-auto", got)
	}
}
