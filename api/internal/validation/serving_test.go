//go:build validation

package validation

// ── RULE-SERVING-001 ─────────────────────────────────────────────────────────
// Public serving families must expose stable logical grains and required
// fields on the minimal capability-aware spine.
//
// ── RULE-SERVING-002 ─────────────────────────────────────────────────────────
// API read models must remain derivable from canonical facts and aggregates,
// not reinterpret raw ingest payloads independently.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleServing001_APIOutputsExposeStableRequiredFields(t *testing.T) {
	h := newHarness(t)

	type fieldCheck struct {
		table  string
		fields []string
	}

	checks := []fieldCheck{
		{
			table: "api_hourly_streaming_sla",
			fields: []string{
				"window_start", "org", "orchestrator_address", "pipeline_id", "model_id", "gpu_id",
				"gpu_model_name", "known_sessions_count", "requested_sessions", "output_failed_sessions",
				"effective_failed_sessions", "health_signal_count", "startup_success_sessions",
				"startup_failed_sessions", "effective_success_rate", "output_viability_rate",
				"output_fps_sum", "status_samples", "prompt_to_first_frame_sum_ms",
				"prompt_to_first_frame_sample_count", "e2e_latency_sum_ms", "e2e_latency_sample_count",
				"avg_output_fps", "avg_prompt_to_first_frame_ms", "avg_e2e_latency_ms",
				"reliability_score", "ptff_score", "e2e_score", "latency_score", "fps_score",
				"quality_score", "sla_semantics_version", "sla_score",
			},
		},
		{
			table: "api_hourly_streaming_demand",
			fields: []string{
				"window_start", "org", "gateway", "pipeline_id", "model_id", "sessions_count",
				"output_fps_sum", "status_samples", "requested_sessions", "effective_failed_sessions",
				"health_signal_count", "startup_success_sessions", "startup_failed_sessions",
				"startup_success_rate", "effective_success_rate", "ticket_face_value_eth",
			},
		},
		{
			table: "api_hourly_streaming_gpu_metrics",
			fields: []string{
				"window_start", "org", "orchestrator_address", "pipeline_id", "model_id", "gpu_id",
				"output_fps_sum", "status_samples", "error_status_samples", "health_signal_count",
				"health_expected_signal_count", "prompt_to_first_frame_sum_ms",
				"startup_latency_sum_ms", "e2e_latency_sum_ms", "known_sessions_count", "swap_rate",
			},
		},
		{
			table: "api_hourly_request_demand",
			fields: []string{
				"window_start", "org", "gateway", "execution_mode", "capability_family",
				"capability_name", "canonical_pipeline", "canonical_model", "orchestrator_address",
				"orchestrator_uri", "job_count", "selected_count", "no_orch_count",
				"success_count", "duration_ms_sum", "price_sum",
			},
		},
	}

	for _, check := range checks {
		t.Run(check.table, func(t *testing.T) {
			fields := "'" + strings.Join(check.fields, "','") + "'"
			got := h.queryInt(t, fmt.Sprintf(
				`SELECT count() FROM system.columns WHERE database = 'naap' AND table = '%s' AND name IN (%s)`,
				check.table, fields,
			))
			if got != uint64(len(check.fields)) {
				t.Fatalf("%s exposed %d/%d required fields", check.table, got, len(check.fields))
			}
		})
	}
}

func TestRuleServing002_APIOutputsRemainDerivableFromCanonicalFacts(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(5 * time.Minute)
	streamID := uid("serve")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	windowStart := ts.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-serve","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(10 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(20 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":15.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(20 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM system.columns WHERE database = 'naap' AND table = 'api_hourly_streaming_demand' AND name = 'canonical_session_key'`); got != 0 {
		t.Fatalf("api_hourly_streaming_demand unexpectedly exposed canonical_session_key")
	}
	if got := h.queryInt(t, `SELECT status_samples FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_gpu_metrics status_samples = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image' AND gpu_id = 'gpu-serve'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_sla row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT known_sessions_count FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_gpu_metrics known_sessions_count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_current WHERE canonical_session_key = ? AND startup_outcome = 'success' AND attributed_orch_address = ?`, key, orchAddr); got != 1 {
		t.Fatalf("canonical_session_current did not retain the canonical success row for %s", key)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ? AND is_terminal_tail_artifact = 0 AND status_samples = 1`, key, windowStart); got != 1 {
		t.Fatalf("canonical_status_hours did not retain the non-tail hourly status row for %s", key)
	}
	if got := h.queryString(t, `SELECT ifNull(canonical_pipeline, '') FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != "text-to-image" {
		t.Fatalf("canonical_session_current canonical_pipeline = %q, want text-to-image", got)
	}
}

func TestRuleServing002_SessionEdgeLatencyFlowsToServingOutputs(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(15 * time.Minute)
	streamID := uid("latency")
	requestID := uid("latency_req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	windowStart := ts.Truncate(time.Hour)
	startTime := ts.Add(1 * time.Second)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-5 * time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-latency","gpu_info":[{"id":"gpu-latency","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-5 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_send_first_ingest_segment","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(4 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_first_processed_segment","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(4 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(5 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"runner_send_first_processed_segment","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(5 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(7 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(7 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(20 * time.Second), Org: h.org, Gateway: "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","start_time":%q,"state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":15.0,"restart_count":0,"last_error":""},"input_status":{"fps":15.0}}`, streamID, requestID, startTime.Format(time.RFC3339Nano), orchAddr, orchURI),
			IngestedAt: ts.Add(20 * time.Second),
		},
	})

	if got := h.queryFloat(t, `SELECT ifNull(startup_latency_ms, -1) FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != 4000 {
		t.Fatalf("canonical_session_current startup_latency_ms = %v, want 4000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(e2e_latency_ms, -1) FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != 3000 {
		t.Fatalf("canonical_session_current e2e_latency_ms = %v, want 3000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(prompt_to_playable_latency_ms, -1) FROM naap.canonical_session_current WHERE canonical_session_key = ?`, key); got != 6000 {
		t.Fatalf("canonical_session_current prompt_to_playable_latency_ms = %v, want 6000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(startup_latency_ms, -1) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, windowStart); got != 4000 {
		t.Fatalf("canonical_status_hours startup_latency_ms = %v, want 4000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(avg_e2e_latency_ms, -1) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, windowStart); got != 3000 {
		t.Fatalf("canonical_status_hours avg_e2e_latency_ms = %v, want 3000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(prompt_to_playable_latency_ms, -1) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, windowStart); got != 6000 {
		t.Fatalf("canonical_status_hours prompt_to_playable_latency_ms = %v, want 6000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(e2e_latency_ms, -1) FROM naap.canonical_status_samples_recent WHERE canonical_session_key = ? LIMIT 1`, key); got != 3000 {
		t.Fatalf("canonical_status_samples_recent e2e_latency_ms = %v, want 3000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(avg_prompt_to_first_frame_ms, -1) FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 6000 {
		t.Fatalf("api_hourly_streaming_gpu_metrics avg_prompt_to_first_frame_ms = %v, want 6000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(avg_startup_latency_ms, -1) FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 4000 {
		t.Fatalf("api_hourly_streaming_gpu_metrics avg_startup_latency_ms = %v, want 4000", got)
	}
	if got := h.queryFloat(t, `SELECT ifNull(avg_e2e_latency_ms, -1) FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 3000 {
		t.Fatalf("api_hourly_streaming_gpu_metrics avg_e2e_latency_ms = %v, want 3000", got)
	}
	if got := h.queryInt(t, `SELECT prompt_to_first_frame_sample_count FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_gpu_metrics prompt_to_first_frame_sample_count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT startup_latency_sample_count FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_gpu_metrics startup_latency_sample_count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT e2e_latency_sample_count FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("api_hourly_streaming_gpu_metrics e2e_latency_sample_count = %d, want 1", got)
	}
}
