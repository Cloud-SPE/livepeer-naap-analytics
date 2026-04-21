//go:build validation

package validation

// ── RULE-ATTRIBUTION-001 ─────────────────────────────────────────────────────
// Canonical attribution must come from historical capability snapshots, with
// fail-safe states kept explicit in canonical facts and serving outputs.
//
// ── RULE-ATTRIBUTION-007 ─────────────────────────────────────────────────────
// Attribution coverage must remain observable from raw-event lineage signals
// such as blank gateway rate.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleAttribution001_CapabilitySnapshotsNormalizeAndConverge(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	mixedCaseAddr := fmt.Sprintf("0xAbCdEf%s", uid("orch"))
	wantAddr := strings.ToLower(mixedCaseAddr)
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-v1","uri":%q,"version":"0.7.0"}]`,
				mixedCaseAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-v2","uri":%q,"version":"0.8.0"}]`,
				mixedCaseAddr, orchURI),
			IngestedAt: ts.Add(time.Minute),
		},
	})

	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ? AND orch_address = ?`, h.org, wantAddr) != 2 {
		t.Errorf("RULE-ATTRIBUTION-001: expected 2 historical capability snapshots for %s", wantAddr)
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshot_latest WHERE org = ? AND orch_address = ?`, h.org, wantAddr) != 1 {
		t.Errorf("RULE-ATTRIBUTION-001: expected 1 latest capability snapshot row for %s", wantAddr)
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_orchestrator_inventory WHERE org = ? AND orch_address = ?`, h.org, wantAddr) != 2 {
		t.Errorf("RULE-ATTRIBUTION-001: expected 2 observed orchestrator inventory rows for %s", wantAddr)
	}
	if got := h.queryString(t, `SELECT argMax(version, last_seen) FROM naap.canonical_capability_orchestrator_inventory WHERE org = ? AND orch_address = ?`, h.org, wantAddr); got != "0.8.0" {
		t.Errorf("RULE-ATTRIBUTION-001: latest observed version = %q, want 0.8.0", got)
	}
}

func TestRuleAttribution001_LateCapabilitySnapshotRepairsHistoricalAttribution(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("stream")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0xAbCd%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "unresolved" {
		t.Errorf("RULE-ATTRIBUTION-001: pre-snapshot attribution_status = %q, want unresolved", got)
	}

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(30 * time.Second), Org: h.org,
		Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`,
			orchAddr, orchURI),
		IngestedAt: ts.Add(30 * time.Second),
	}})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Errorf("RULE-ATTRIBUTION-001: repaired attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(attributed_orch_address, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != orchAddr {
		t.Errorf("RULE-ATTRIBUTION-001: repaired attributed_orch_address = %q, want %q", got, orchAddr)
	}
}

func TestRuleAttribution001_HistoricalMatchUsesNearestSnapshotNotNewestOverall(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("stream")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0xNear%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-5 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-near","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-near","gpu_info":[{"id":"gpu-near","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, orchURI),
			IngestedAt: ts.Add(-5 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(30 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-late","uri":%q,"version":"1.0.0","hardware":[{"pipeline":"text-to-image","model_id":"model-late","gpu_info":[{"id":"gpu-late","name":"L40","memory_total":49152}]}]}]`,
				orchAddr, orchURI),
			IngestedAt: ts.Add(30 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Fatalf("RULE-ATTRIBUTION-001: attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "model-near" {
		t.Errorf("RULE-ATTRIBUTION-001: canonical_model = %q, want model-near", got)
	}
}

func TestRuleAttribution001_URIWinsOverConflictingObservedAddress(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("stream")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	addrA := strings.ToLower(fmt.Sprintf("0xAa%s", uid("orch")))
	uriA := fmt.Sprintf("https://orch-a-%s.example.com", uid("uri"))
	addrB := strings.ToLower(fmt.Sprintf("0xBb%s", uid("orch")))
	uriB := fmt.Sprintf("https://orch-b-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-a","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`,
				addrA, uriA),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-b","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-b","gpu_info":[{"id":"gpu-b","name":"L4","memory_total":24576}]}]}]`,
				addrB, uriB),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(5 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				streamID, requestID, addrA, uriB),
			IngestedAt: ts.Add(5 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(10 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":11.0,"restart_count":0,"last_error":""}}`,
				streamID, requestID, addrA, uriB),
			IngestedAt: ts.Add(10 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Fatalf("RULE-ATTRIBUTION-001: attribution_status = %q, want resolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(attributed_orch_address, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != addrB {
		t.Errorf("RULE-ATTRIBUTION-001: attributed_orch_address = %q, want URI-matched %q", got, addrB)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "model-b" {
		t.Errorf("RULE-ATTRIBUTION-001: canonical_model = %q, want URI-matched model-b", got)
	}
}

func TestRuleAttribution001_FailSafeStatesRemainVisibleInServingOutputs(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	hwStream := uid("hw")
	hwRequest := uid("req")
	hwKey := canonicalSessionKey(h.org, hwStream, hwRequest)
	hwAddr := strings.ToLower(fmt.Sprintf("0xAbCd%s", uid("orch")))
	hwURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	unresolvedStream := uid("missing")
	unresolvedRequest := uid("req")
	unresolvedKey := canonicalSessionKey(h.org, unresolvedStream, unresolvedRequest)
	unresolvedAddr := strings.ToLower(fmt.Sprintf("0xDeAd%s", uid("orch")))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-hwless","uri":%q,"version":"0.9.0"}]`,
				hwAddr, hwURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				hwStream, hwRequest, hwAddr, hwURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`,
				hwStream, hwRequest, hwAddr, hwURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":"https://missing.example.com"}}`,
				unresolvedStream, unresolvedRequest, unresolvedAddr),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://missing.example.com"},"inference_status":{"fps":11.0,"restart_count":0,"last_error":""}}`,
				unresolvedStream, unresolvedRequest, unresolvedAddr),
			IngestedAt: ts.Add(time.Second),
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, hwKey); got != "hardware_less" {
		t.Errorf("RULE-ATTRIBUTION-001: hardware-less attribution_status = %q, want hardware_less", got)
	}
	if got := h.queryString(t, `SELECT ifNull(attributed_orch_address, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, hwKey); got != hwAddr {
		t.Errorf("RULE-ATTRIBUTION-001: hardware-less attributed_orch_address = %q, want %q", got, hwAddr)
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_status_samples_recent WHERE canonical_session_key = ? LIMIT 1`, hwKey); got != "hardware_less" {
		t.Errorf("RULE-ATTRIBUTION-001: hardware-less serving attribution_status = %q, want hardware_less", got)
	}
	if h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND orchestrator_address = ? AND gpu_id IS NULL`, h.org, hwAddr) == 0 {
		t.Errorf("RULE-ATTRIBUTION-001: hardware-less session was dropped from hourly streaming SLA for %s", hwAddr)
	}

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, unresolvedKey); got != "unresolved" {
		t.Errorf("RULE-ATTRIBUTION-001: unresolved attribution_status = %q, want unresolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(orch_address, '') FROM naap.canonical_status_samples_recent WHERE canonical_session_key = ? LIMIT 1`, unresolvedKey); got != "" {
		t.Errorf("RULE-ATTRIBUTION-001: unresolved serving orch_address = %q, want empty canonical address", got)
	}
}

func TestRuleAttribution001_AmbiguousAndStaleRemainExplicit(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	staleStream := uid("stale")
	staleRequest := uid("req")
	staleKey := canonicalSessionKey(h.org, staleStream, staleRequest)
	staleAddr := strings.ToLower(fmt.Sprintf("0xStale%s", uid("orch")))
	staleURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	ambiguousStream := uid("amb")
	ambiguousRequest := uid("req")
	ambiguousKey := canonicalSessionKey(h.org, ambiguousStream, ambiguousRequest)
	ambAddrA := strings.ToLower(fmt.Sprintf("0xAa%s", uid("orch")))
	ambAddrB := strings.ToLower(fmt.Sprintf("0xBb%s", uid("orch")))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-20 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-old","uri":%q,"version":"0.8.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-old","name":"L4","memory_total":24576}]}]}]`,
				staleAddr, staleURI),
			IngestedAt: ts.Add(-20 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`,
				staleStream, staleRequest, staleAddr, staleURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`,
				staleStream, staleRequest, staleAddr, staleURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":"https://amb-a.example.com"}}`,
				ambiguousStream, ambiguousRequest, ambAddrA),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://amb-a.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`,
				ambiguousStream, ambiguousRequest, ambAddrA),
			IngestedAt: ts.Add(2 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(3 * time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://amb-b.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`,
				ambiguousStream, ambiguousRequest, ambAddrB),
			IngestedAt: ts.Add(3 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, staleKey); got != "stale" {
		t.Errorf("RULE-ATTRIBUTION-001: stale attribution_status = %q, want stale", got)
	}
	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, ambiguousKey); got != "ambiguous" {
		t.Errorf("RULE-ATTRIBUTION-001: ambiguous attribution_status = %q, want ambiguous", got)
	}
}

func TestRuleAttribution002_PipelineAndModelStayDistinctAcrossCanonicalAndAPIViews(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("pm")
	requestID := uid("req")
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-pm","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline_id":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(5 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline_id":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(5 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(10 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":11.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT ifNull(canonical_pipeline, '') FROM naap.canonical_session_current WHERE org = ? AND stream_id = ?`, h.org, streamID); got != "text-to-image" {
		t.Fatalf("RULE-ATTRIBUTION-002: canonical_pipeline = %q, want text-to-image", got)
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_offer_inventory WHERE org = ? AND orch_address = ? AND canonical_pipeline = 'text-to-image' AND model_id = 'model-a' AND hardware_present = 1`, h.org, orchAddr) == 0 {
		t.Fatalf("RULE-ATTRIBUTION-002: expected hardware inventory row for %s", orchAddr)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_pipeline, '') FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ?`, h.org, orchAddr); got != "text-to-image" {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware pipeline = %q, want text-to-image", got)
	}
	if got := h.queryString(t, `SELECT ifNull(model_id, '') FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ?`, h.org, orchAddr); got != "model-a" {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware model_id = %q, want model-a", got)
	}
	if h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND canonical_pipeline = model_id`, h.org, orchAddr) != 0 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware surface duplicated model identity into pipeline")
	}
}

func TestRuleAttribution002_HardwareInventoryCapturesAllGPUEntriesFromArrayShape(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-array","gpu_info":[{"id":"gpu-array-a","name":"L4","memory_total":24576},{"id":"gpu-array-b","name":"L40","memory_total":49152}]}]}]`,
			orchAddr, orchURI),
		IngestedAt: ts,
	}})

	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_capability_offer_inventory WHERE org = ? AND orch_address = ? AND canonical_pipeline = 'text-to-image' AND model_id = 'model-array' AND hardware_present = 1`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: canonical hardware rows = %d, want 2", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND canonical_pipeline = 'text-to-image' AND model_id = 'model-array'`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware rows = %d, want 2", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND gpu_id IN ('gpu-array-a', 'gpu-array-b')`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware missing one or more array GPUs")
	}
}

func TestRuleAttribution002_HardwareInventoryCapturesAllGPUEntriesFromObjectShapeAndSkipsEmptyIDs(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"image-to-video","model_id":"model-object","gpu_info":{"0":{"id":"","name":"ignore-me","memory_total":1},"1":{"id":"gpu-object-a","name":"RTX 4090","memory_total":25769803776},"2":{"id":"gpu-object-b","name":"RTX 5090","memory_total":34190917632}}}]}]`,
			orchAddr, orchURI),
		IngestedAt: ts,
	}})

	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_capability_offer_inventory WHERE org = ? AND orch_address = ? AND canonical_pipeline = 'image-to-video' AND model_id = 'model-object' AND hardware_present = 1`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: canonical hardware rows = %d, want 2 after skipping empty gpu id", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND canonical_pipeline = 'image-to-video' AND model_id = 'model-object'`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware rows = %d, want 2 after skipping empty gpu id", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND gpu_id = ''`, h.org, orchAddr); got != 0 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware retained empty gpu ids")
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_observed_capability_hardware WHERE org = ? AND orch_address = ? AND gpu_id IN ('gpu-object-a', 'gpu-object-b')`, h.org, orchAddr); got != 2 {
		t.Fatalf("RULE-ATTRIBUTION-002: observed hardware missing one or more object GPUs")
	}
}

func TestRuleAttribution003_InWindowSnapshotWinsOverOnlyStaleEvidence(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("fresh")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-20 * time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch-old","uri":%q,"version":"0.8.0","hardware":[{"pipeline":"text-to-image","model_id":"old-model","gpu_info":[{"id":"gpu-old","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-20 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-30 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch-new","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"new-model","gpu_info":[{"id":"gpu-new","name":"L40","memory_total":49152}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-30 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "resolved" {
		t.Fatalf("RULE-ATTRIBUTION-003: attribution_status = %q, want resolved", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current WHERE canonical_session_key = ? AND attribution_snapshot_ts = ?`, key, ts.Add(-30*time.Second)); got != 1 {
		t.Fatalf("RULE-ATTRIBUTION-003: exact in-window snapshot was not selected")
	}
}

func TestRuleAttribution004_FailSafeReasonsRemainExplicit(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	missingStream := uid("missing")
	missingKey := canonicalSessionKey(h.org, missingStream, "req")
	staleStream := uid("stale")
	staleKey := canonicalSessionKey(h.org, staleStream, "req")
	ambStream := uid("amb")
	ambKey := canonicalSessionKey(h.org, ambStream, "req")
	staleAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("stale")))
	ambA := strings.ToLower(fmt.Sprintf("0x%s", uid("a")))
	ambB := strings.ToLower(fmt.Sprintf("0x%s", uid("b")))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":"0xmissing","url":"https://missing.example.com"}}`, missingStream),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xmissing","url":"https://missing.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, missingStream),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-20 * time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"stale","uri":"https://stale.example.com","version":"0.8.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-stale","name":"L4","memory_total":24576}]}]}]`, staleAddr),
			IngestedAt: ts.Add(-20 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":"https://stale.example.com"}}`, staleStream, staleAddr),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://stale.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, staleStream, staleAddr),
			IngestedAt: ts.Add(time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":"https://amb-a.example.com"}}`, ambStream, ambA),
			IngestedAt: ts.Add(2 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(3 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://amb-a.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, ambStream, ambA),
			IngestedAt: ts.Add(3 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(4 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":"req","pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":"https://amb-b.example.com"},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, ambStream, ambB),
			IngestedAt: ts.Add(4 * time.Second),
		},
	})

	if got := h.queryString(t, `SELECT attribution_reason FROM naap.canonical_session_current WHERE canonical_session_key = ?`, missingKey); got != "missing_candidate" {
		t.Fatalf("RULE-ATTRIBUTION-004: missing reason = %q, want missing_candidate", got)
	}
	if got := h.queryString(t, `SELECT attribution_reason FROM naap.canonical_session_current WHERE canonical_session_key = ?`, staleKey); got != "stale_candidate" {
		t.Fatalf("RULE-ATTRIBUTION-004: stale reason = %q, want stale_candidate", got)
	}
	if got := h.queryString(t, `SELECT attribution_reason FROM naap.canonical_session_current WHERE canonical_session_key = ?`, ambKey); got != "ambiguous_candidates" {
		t.Fatalf("RULE-ATTRIBUTION-004: ambiguous reason = %q, want ambiguous_candidates", got)
	}
}

func TestRuleAttribution005_UnresolvedRowsDoNotInventCanonicalFields(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("unresolved")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	windowStart := ts.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":"0xdeadbeef","url":"https://missing.example.com"}}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xdeadbeef","url":"https://missing.example.com"},"inference_status":{"fps":8.0,"restart_count":0,"last_error":""}}`, streamID, requestID),
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "unresolved" {
		t.Fatalf("RULE-ATTRIBUTION-005: attribution_status = %q, want unresolved", got)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_model, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "" {
		t.Fatalf("RULE-ATTRIBUTION-005: canonical_model = %q, want blank", got)
	}
	if got := h.queryString(t, `SELECT ifNull(model_id, '') FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ''`, h.org, windowStart); got != "" {
		t.Fatalf("RULE-ATTRIBUTION-005: api model_id = %q, want blank", got)
	}
	if h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '' AND gpu_id IS NOT NULL`, h.org, windowStart) != 0 {
		t.Fatalf("RULE-ATTRIBUTION-005: unresolved row invented gpu identity")
	}
}

func TestRuleAttribution006_HardwareLessRowsRemainVisibleWithoutGPU(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("hwless")
	requestID := uid("req")
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch-hwless","uri":%q,"version":"0.9.0"}]`, orchAddr, orchURI),
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

	if got := h.queryString(t, `SELECT attribution_status FROM naap.canonical_session_current WHERE org = ? AND stream_id = ?`, h.org, streamID); got != "hardware_less" {
		t.Fatalf("RULE-ATTRIBUTION-006: attribution_status = %q, want hardware_less", got)
	}
	if h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND gpu_id IS NULL`, h.org, windowStart, orchAddr) != 1 {
		t.Fatalf("RULE-ATTRIBUTION-006: hardware-less row was not preserved with null gpu_id")
	}
}

func TestRuleAttribution007_BlankGatewayRateIsComputable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r1","type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r2","type":"gateway_ingest_stream_closed"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org, Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r3","pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-eu-west-1", Data: fmt.Sprintf(`{"stream_id":%q,"request_id":"r4","type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
	})

	total := h.queryInt(t, `SELECT count() FROM naap.accepted_raw_events WHERE org = ?`, h.org)
	blank := h.queryInt(t, `SELECT countIf(gateway = '') FROM naap.accepted_raw_events WHERE org = ?`, h.org)

	if total != 4 {
		t.Errorf("RULE-ATTRIBUTION-007: total events = %d, want 4", total)
	}
	if blank != 3 {
		t.Errorf("RULE-ATTRIBUTION-007: blank gateway events = %d, want 3", blank)
	}
}
