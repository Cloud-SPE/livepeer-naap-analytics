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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type slaStoreSeed struct {
	WindowStart                   time.Time
	Org                           string
	OrchestratorAddress           string
	PipelineID                    string
	ModelID                       *string
	GPUID                         *string
	GPUModelName                  *string
	Region                        *string
	RequestedSessions             uint64
	StartupSuccessSessions        uint64
	StartupExcusedSessions        uint64
	StartupFailedSessions         uint64
	OutputFailedSessions          uint64
	EffectiveFailedSessions       uint64
	TotalSwappedSessions          uint64
	HealthSignalCount             uint64
	HealthExpectedSignalCount     uint64
	OutputFPSSum                  float64
	StatusSamples                 uint64
	PromptToFirstFrameSumMS       float64
	PromptToFirstFrameSampleCount uint64
	E2ELatencySumMS               float64
	E2ELatencySampleCount         uint64
}

func insertSLAStoreSeed(t *testing.T, h *harness, seed slaStoreSeed) {
	t.Helper()
	refreshRunID := fmt.Sprintf("seed-input-%s-%s", seed.Org, seed.WindowStart.UTC().Format(time.RFC3339))
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_streaming_sla_input_hourly_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			output_failed_sessions, effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error, error_status_samples, health_signal_count,
			health_expected_signal_count, output_fps_sum, status_samples, prompt_to_first_frame_sum_ms,
			prompt_to_first_frame_sample_count, e2e_latency_sum_ms, e2e_latency_sample_count,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		VALUES (
			?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?,
			?, ?,
			now64()
		)
	`,
		seed.WindowStart, seed.Org, seed.OrchestratorAddress, seed.PipelineID, seed.ModelID, seed.GPUID, seed.GPUModelName, seed.Region,
		seed.RequestedSessions, seed.RequestedSessions, seed.StartupSuccessSessions, uint64(0),
		seed.StartupExcusedSessions, seed.StartupFailedSessions, seed.OutputFailedSessions, uint64(0),
		seed.OutputFailedSessions, seed.EffectiveFailedSessions, uint64(0),
		uint64(0), seed.TotalSwappedSessions, uint64(0), uint64(0),
		seed.HealthSignalCount, seed.HealthExpectedSignalCount,
		seed.OutputFPSSum, seed.StatusSamples, seed.PromptToFirstFrameSumMS, seed.PromptToFirstFrameSampleCount,
		seed.E2ELatencySumMS, seed.E2ELatencySampleCount, refreshRunID, "validation",
	); err != nil {
		t.Fatalf("insert sla store seed: %v", err)
	}
	republishFinalSLAPublicWindow(t, h, seed.WindowStart)
}

func republishFinalSLAPublicWindow(t *testing.T, h *harness, windowStart time.Time) {
	t.Helper()
	refreshRunID := fmt.Sprintf("seed-final-public-%d", time.Now().UTC().UnixNano())
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_streaming_sla_hourly_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			output_failed_sessions, effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error, error_status_samples, health_signal_count,
			health_expected_signal_count, health_signal_coverage_ratio, startup_success_rate, excused_failure_rate,
			effective_success_rate, no_swap_rate, output_viability_rate, output_fps_sum, status_samples,
			avg_output_fps, prompt_to_first_frame_sum_ms, prompt_to_first_frame_sample_count,
			avg_prompt_to_first_frame_ms, e2e_latency_sum_ms, e2e_latency_sample_count, avg_e2e_latency_ms,
			reliability_score, ptff_score, e2e_score, latency_score, fps_score, quality_score,
			sla_semantics_version, sla_score, refresh_run_id, artifact_checksum, refreshed_at
		)
		SELECT
			s.window_start, s.org, s.orchestrator_address, s.pipeline_id, s.model_id, s.gpu_id, s.gpu_model_name, cast(null as Nullable(String)),
			s.known_sessions_count, s.requested_sessions, s.startup_success_sessions, s.no_orch_sessions,
			s.startup_excused_sessions, s.startup_failed_sessions, s.loading_only_sessions, s.zero_output_fps_sessions,
			s.output_failed_sessions, s.effective_failed_sessions, s.confirmed_swapped_sessions, s.inferred_swap_sessions,
			s.total_swapped_sessions, s.sessions_ending_in_error, s.error_status_samples, s.health_signal_count,
			s.health_expected_signal_count, s.health_signal_coverage_ratio, s.startup_success_rate, s.excused_failure_rate,
			s.effective_success_rate, s.no_swap_rate, s.output_viability_rate, s.output_fps_sum, s.status_samples,
			s.avg_output_fps, s.prompt_to_first_frame_sum_ms, s.prompt_to_first_frame_sample_count,
			s.avg_prompt_to_first_frame_ms, s.e2e_latency_sum_ms, s.e2e_latency_sample_count, s.avg_e2e_latency_ms,
			s.reliability_score, s.ptff_score, s.e2e_score, s.latency_score, s.fps_score, s.quality_score,
			s.sla_semantics_version, s.sla_score, ?, 'validation', now64()
		FROM naap.api_base_sla_compliance_scored_by_org s
		WHERE s.window_start = ?
	`, refreshRunID, windowStart); err != nil {
		t.Fatalf("republish public final sla window: %v", err)
	}
}

func ptr[T any](v T) *T {
	return &v
}

func insertSLABenchmarkHistory(
	t *testing.T,
	h *harness,
	currentWindow time.Time,
	pipelineID string,
	modelID *string,
	gpuID *string,
	region *string,
	baseOutputFPSSum float64,
	baseStatusSamples uint64,
	basePTFFSumMS float64,
	basePTFFSamples uint64,
	baseE2ESumMS float64,
	baseE2ESamples uint64,
) {
	t.Helper()
	currentDate := currentWindow.UTC().Truncate(24 * time.Hour)
	idx := 0
	for dayOffset := 7; dayOffset >= 1; dayOffset-- {
		dayStart := currentDate.AddDate(0, 0, -dayOffset)
		for hour := 0; hour < 8; hour++ {
			insertSLAStoreSeed(t, h, slaStoreSeed{
				WindowStart:                   dayStart.Add(time.Duration(hour) * time.Hour),
				Org:                           h.org,
				OrchestratorAddress:           fmt.Sprintf("0xbenchhist%02d", idx),
				PipelineID:                    pipelineID,
				ModelID:                       modelID,
				GPUID:                         gpuID,
				Region:                        region,
				RequestedSessions:             10,
				StartupSuccessSessions:        10,
				EffectiveFailedSessions:       0,
				HealthSignalCount:             10,
				HealthExpectedSignalCount:     10,
				OutputFPSSum:                  baseOutputFPSSum + float64(idx),
				StatusSamples:                 baseStatusSamples,
				PromptToFirstFrameSumMS:       basePTFFSumMS + float64(idx*10),
				PromptToFirstFrameSampleCount: basePTFFSamples,
				E2ELatencySumMS:               baseE2ESumMS + float64(idx*10),
				E2ELatencySampleCount:         baseE2ESamples,
			})
			idx++
		}
	}
}

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
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_input_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_input_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_inference_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 1 {
		t.Errorf("RULE-AGGREGATE-004: degraded_inference_count did not stay split at 1")
	}
	if h.queryInt(t, `SELECT uniqExactIf(canonical_session_key, degraded_input_samples > 0 or degraded_inference_samples > 0) FROM naap.canonical_status_hours WHERE org = ? AND hour = ? AND orch_address = ? AND is_terminal_tail_artifact = 0`, h.org, hour, orchAddr) != 2 {
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

	if h.queryInt(t, `SELECT error_samples FROM naap.canonical_status_hours WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-AGGREGATE-004: same-hour failed no-output session lost error_samples")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ?`, key) != 0 {
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

	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour) != 0 {
		t.Errorf("RULE-LIFECYCLE-009: previous active hour was incorrectly tail-filtered")
	}
	if h.queryInt(t, `SELECT toUInt64(is_terminal_tail_artifact) FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ?`, key, baseHour.Add(time.Hour)) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: terminal rollover hour was not tail-filtered")
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE org = ? AND is_terminal_tail_artifact = 0`, h.org) != 1 {
		t.Errorf("RULE-LIFECYCLE-009: serving reliability kept tail noise instead of filtering it")
	}
}

func TestRuleLifecycle008And009_DemandUsesStartHourWhileReliabilityUsesStatusHour(t *testing.T) {
	h := newHarness(t)
	startTS := anchor().Add(-30 * time.Minute)
	statusTS := startTS.Add(70 * time.Minute)
	streamID := uid("cross-hour")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	startHour := startTS.Truncate(time.Hour)
	statusHour := statusTS.Truncate(time.Hour)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: startTS, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: startTS,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: startTS, Org: h.org, Gateway: "gw-cross-hour",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: startTS,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: statusTS, Org: h.org, Gateway: "gw-cross-hour",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":11.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: statusTS,
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_demand_input_current WHERE canonical_session_key = ? AND window_start = ?`, key, startHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-008: demand row count at session start hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_session_demand_input_current WHERE canonical_session_key = ? AND window_start = ?`, key, statusHour); got != 0 {
		t.Fatalf("RULE-LIFECYCLE-008: demand row leaked into status hour, got %d", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.canonical_status_hours WHERE canonical_session_key = ? AND hour = ? AND is_terminal_tail_artifact = 0 AND status_samples = 1`, key, statusHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-009: status-hour row count at active hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour' AND pipeline_id = 'text-to-image'`, h.org, startHour); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-008: demand serving row at start hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 1 {
		t.Fatalf("RULE-AGGREGATE-002: perf-backed demand row at status hour = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT requested_sessions FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 0 {
		t.Fatalf("RULE-AGGREGATE-002: perf-only demand row requested_sessions = %d, want 0", got)
	}
	if got := h.queryFloat(t, `SELECT avg_output_fps FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-cross-hour'`, h.org, statusHour); got != 11 {
		t.Fatalf("RULE-AGGREGATE-002: perf-only demand row avg_output_fps = %v, want 11", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND known_sessions_count = 1 AND startup_failed_sessions = 1`, h.org, statusHour, orchAddr); got != 1 {
		t.Fatalf("RULE-LIFECYCLE-009: SLA serving row at status hour = %d, want 1", got)
	}
}

func TestRuleAggregate002And005_DemandAndSLAUseContractedFailureSemantics(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(20 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	successStream := uid("success")
	successRequest := uid("req")
	zeroStream := uid("zero")
	zeroRequest := uid("req")

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-a","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(4 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":12.0,"restart_count":0,"last_error":""}}`, successStream, successRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(4 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(10 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, zeroStream, zeroRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(10 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(12 * time.Second), Org: h.org, Gateway: "gw-contract",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":0.0,"restart_count":0,"last_error":"boom"}}`, zeroStream, zeroRequest, orchAddr, orchURI),
			IngestedAt: ts.Add(12 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT effective_failed_sessions FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-contract' AND pipeline_id = 'text-to-image'`, h.org, windowStart); got != 1 {
		t.Fatalf("RULE-AGGREGATE-002: effective_failed_sessions = %d, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT round(effective_success_rate, 6) FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-contract' AND pipeline_id = 'text-to-image'`, h.org, windowStart); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-002: effective_success_rate = %v, want 0.5", got)
	}
	if got := h.queryFloat(t, `SELECT round(health_signal_coverage_ratio, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.833333 {
		t.Fatalf("RULE-AGGREGATE-005: health_signal_coverage_ratio = %v, want 0.833333", got)
	}
	if got := h.queryFloat(t, `SELECT round(effective_success_rate, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: effective_success_rate = %v, want 0.5", got)
	}
	if got := h.queryFloat(t, `SELECT round(output_viability_rate, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: output_viability_rate = %v, want 0.5", got)
	}
	if got := h.queryString(t, `SELECT sla_semantics_version FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != "quality-benchmark-v1" {
		t.Fatalf("RULE-AGGREGATE-005: sla_semantics_version = %q, want quality-benchmark-v1", got)
	}
	if got := h.queryFloat(t, `SELECT round(sla_score, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 47.5 {
		t.Fatalf("RULE-AGGREGATE-005: sla_score = %v, want 47.5", got)
	}
}

func TestRuleAggregate005_OutputFailedSessionsUsesUnionSemantics(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(22 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))
	streamID := uid("overlap")
	requestID := uid("req")

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-overlap","gpu_info":[{"id":"gpu-overlap","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-overlap",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-overlap",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"LOADING","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":0.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT loading_only_sessions FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: loading_only_sessions = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT zero_output_fps_sessions FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: zero_output_fps_sessions = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT output_failed_sessions FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: output_failed_sessions = %d, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT round(output_viability_rate, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got != 0 {
		t.Fatalf("RULE-AGGREGATE-005: output_viability_rate = %v, want 0", got)
	}
	if got := h.queryFloat(t, `SELECT round(sla_score, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image'`, h.org, windowStart, orchAddr); got < 0 || got > 100 {
		t.Fatalf("RULE-AGGREGATE-005: sla_score = %v, want bounded [0,100]", got)
	}
}

func TestRuleAggregate005_StoreClampPreventsImpossibleCoverageLeakage(t *testing.T) {
	h := newHarness(t)
	windowStart := anchor().Add(48 * time.Hour).Truncate(time.Hour)
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:               windowStart,
		Org:                       h.org,
		OrchestratorAddress:       "0xclamp",
		PipelineID:                "text-to-image",
		ModelID:                   ptr("model-clamp"),
		GPUID:                     ptr("gpu-clamp"),
		RequestedSessions:         1,
		StartupSuccessSessions:    1,
		HealthSignalCount:         5,
		HealthExpectedSignalCount: 1,
	})

	if got := h.queryFloat(t, `SELECT health_signal_coverage_ratio FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xclamp'`, h.org, windowStart); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: by-org coverage = %v, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT health_signal_coverage_ratio FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xclamp'`, h.org, windowStart); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: hourly coverage = %v, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xclamp'`, h.org, windowStart); got > 100 {
		t.Fatalf("RULE-AGGREGATE-005: by-org sla_score = %v, want <= 100", got)
	}
	if got := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xclamp'`, h.org, windowStart); got > 100 {
		t.Fatalf("RULE-AGGREGATE-005: hourly sla_score = %v, want <= 100", got)
	}
}

func TestRuleAggregate005_QualityAwareSLARewardsBetterQualityAtEqualReliability(t *testing.T) {
	h := newHarness(t)
	currentWindow := anchor().Add(96 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	modelID := ptr("model-quality")
	gpuID := ptr("gpu-quality")
	region := ptr("us-east")

	insertSLABenchmarkHistory(t, h, currentWindow, "text-to-image", modelID, gpuID, region, 1000, 100, 100000, 100, 70000, 100)

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xqualitygood",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  2400,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       60000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               40000,
		E2ELatencySampleCount:         100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xqualitybad",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  400,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       240000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               180000,
		E2ELatencySampleCount:         100,
	})

	good := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xqualitygood'`, h.org, currentWindow)
	bad := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xqualitybad'`, h.org, currentWindow)
	if !(good > bad) {
		t.Fatalf("RULE-AGGREGATE-005: quality-aware sla_score good=%v bad=%v, want good > bad", good, bad)
	}
	if got := h.queryFloat(t, `SELECT reliability_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xqualitygood'`, h.org, currentWindow); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: reliability_score = %v, want 1", got)
	}
}

func TestRuleAggregate005_LatencyWeightingFavorsPTFFOverE2E(t *testing.T) {
	h := newHarness(t)
	currentWindow := anchor().Add(120 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	modelID := ptr("model-latency-weight")
	gpuID := ptr("gpu-latency-weight")
	region := ptr("us-east")

	insertSLABenchmarkHistory(t, h, currentWindow, "text-to-image", modelID, gpuID, region, 1000, 100, 100000, 100, 100000, 100)

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xptffwins",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       10000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               200000,
		E2ELatencySampleCount:         100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xe2ewins",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       200000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               10000,
		E2ELatencySampleCount:         100,
	})

	if got := h.queryFloat(t, `SELECT latency_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xptffwins'`, h.org, currentWindow); got <= 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: ptff-weighted latency_score = %v, want > 0.5", got)
	}
	ptffWins := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xptffwins'`, h.org, currentWindow)
	e2eWins := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xe2ewins'`, h.org, currentWindow)
	if !(ptffWins > e2eWins) {
		t.Fatalf("RULE-AGGREGATE-005: ptff-weighted sla_score ptff=%v e2e=%v, want ptff > e2e", ptffWins, e2eWins)
	}
}

func TestRuleAggregate005_SparseQualityEvidenceShrinksTowardNeutral(t *testing.T) {
	h := newHarness(t)
	currentWindow := anchor().Add(144 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	modelID := ptr("model-sparse")
	gpuID := ptr("gpu-sparse")
	region := ptr("us-east")

	insertSLABenchmarkHistory(t, h, currentWindow, "text-to-image", modelID, gpuID, region, 1000, 100, 100000, 100, 100000, 100)

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xsparse",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  100,
		StatusSamples:                 1,
		PromptToFirstFrameSumMS:       100,
		PromptToFirstFrameSampleCount: 1,
		E2ELatencySumMS:               100,
		E2ELatencySampleCount:         1,
	})

	if got := h.queryFloat(t, `SELECT round(ptff_score, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xsparse'`, h.org, currentWindow); got != 0.55 {
		t.Fatalf("RULE-AGGREGATE-005: ptff_score = %v, want 0.55", got)
	}
	if got := h.queryFloat(t, `SELECT round(e2e_score, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xsparse'`, h.org, currentWindow); got != 0.55 {
		t.Fatalf("RULE-AGGREGATE-005: e2e_score = %v, want 0.55", got)
	}
	if got := h.queryFloat(t, `SELECT round(fps_score, 6) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xsparse'`, h.org, currentWindow); got != 0.516667 {
		t.Fatalf("RULE-AGGREGATE-005: fps_score = %v, want 0.516667", got)
	}
}

func TestRuleAggregate005_BenchmarkHistoryDefaultsToNeutralWhenSparse(t *testing.T) {
	h := newHarness(t)
	currentWindow := anchor().Add(168 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	modelID := ptr("model-fallback")
	region := ptr("us-east")

	insertSLABenchmarkHistory(t, h, currentWindow, "text-to-image", modelID, ptr("gpu-history"), region, 1000, 100, 100000, 100, 100000, 100)

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xbenchmarked",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         ptr("gpu-current"),
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       100000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               100000,
		E2ELatencySampleCount:         100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   currentWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xnohistory",
		PipelineID:                    "speech-to-text",
		ModelID:                       ptr("model-no-history"),
		GPUID:                         ptr("gpu-none"),
		Region:                        ptr("eu-west"),
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       100000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               100000,
		E2ELatencySampleCount:         100,
	})

	if got := h.queryFloat(t, `SELECT ptff_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xbenchmarked'`, h.org, currentWindow); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: benchmarked ptff_score = %v, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT quality_score FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xnohistory'`, h.org, currentWindow); got != 0.5 {
		t.Fatalf("RULE-AGGREGATE-005: no-history quality_score = %v, want 0.5", got)
	}
}

func TestRuleAggregate005_BenchmarkStateUsesAdditiveInputs(t *testing.T) {
	h := newHarness(t)
	benchmarkDay := anchor().Add(216 * time.Hour).Truncate(24 * time.Hour)
	modelID := ptr("model-benchmark-state")

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   benchmarkDay.Add(2 * time.Hour),
		Org:                           h.org,
		OrchestratorAddress:           "0xstate-good",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1200,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       60000,
		PromptToFirstFrameSampleCount: 60,
		E2ELatencySumMS:               48000,
		E2ELatencySampleCount:         60,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:               benchmarkDay.Add(3 * time.Hour),
		Org:                       h.org,
		OrchestratorAddress:       "0xstate-ignored",
		PipelineID:                "text-to-image",
		ModelID:                   modelID,
		RequestedSessions:         10,
		StartupSuccessSessions:    10,
		HealthSignalCount:         10,
		HealthExpectedSignalCount: 10,
	})

	if got := h.queryInt(t, `SELECT sumMerge(ptff_row_count_state) FROM naap.api_base_sla_quality_cohort_daily_state WHERE cohort_date = ? AND pipeline_id = 'text-to-image' AND model_id = 'model-benchmark-state'`, benchmarkDay); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: ptff benchmark row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT sumMerge(e2e_row_count_state) FROM naap.api_base_sla_quality_cohort_daily_state WHERE cohort_date = ? AND pipeline_id = 'text-to-image' AND model_id = 'model-benchmark-state'`, benchmarkDay); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: e2e benchmark row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT sumMerge(fps_row_count_state) FROM naap.api_base_sla_quality_cohort_daily_state WHERE cohort_date = ? AND pipeline_id = 'text-to-image' AND model_id = 'model-benchmark-state'`, benchmarkDay); got != 1 {
		t.Fatalf("RULE-AGGREGATE-005: fps benchmark row count = %d, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT toFloat64(round(quantileTDigestIfMerge(0.5)(ptff_p50_state), 6)) FROM naap.api_base_sla_quality_cohort_daily_state WHERE cohort_date = ? AND pipeline_id = 'text-to-image' AND model_id = 'model-benchmark-state'`, benchmarkDay); got != 1000 {
		t.Fatalf("RULE-AGGREGATE-005: ptff p50 benchmark = %v, want 1000", got)
	}
}

func TestRuleAggregate005_GPUModelNameIsSurfacedWhenKnown(t *testing.T) {
	h := newHarness(t)
	windowStart := anchor().Add(240 * time.Hour).Truncate(time.Hour)
	modelID := ptr("model-gpu-name")
	gpuID := ptr("gpu-gpu-name")
	gpuModelName := ptr("NVIDIA H100")

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   windowStart,
		Org:                           h.org,
		OrchestratorAddress:           "0xgpu-model",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		GPUModelName:                  gpuModelName,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       100000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               100000,
		E2ELatencySampleCount:         100,
	})

	if got := h.queryString(t, `SELECT ifNull(gpu_model_name, '') FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xgpu-model'`, h.org, windowStart); got != "NVIDIA H100" {
		t.Fatalf("RULE-AGGREGATE-005: by-org gpu_model_name = %q, want NVIDIA H100", got)
	}
	if got := h.queryString(t, `SELECT ifNull(gpu_model_name, '') FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xgpu-model'`, h.org, windowStart); got != "NVIDIA H100" {
		t.Fatalf("RULE-AGGREGATE-005: hourly gpu_model_name = %q, want NVIDIA H100", got)
	}
}

func TestRuleAggregate005_PublicSLARecomputesQualityInputsFromSupportFields(t *testing.T) {
	h := newHarness(t)
	windowStart := anchor().Add(192 * time.Hour).Truncate(time.Hour)
	modelID := ptr("model-public-sla")
	gpuID := ptr("gpu-public-sla")
	region := ptr("us-east")

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   windowStart,
		Org:                           h.org,
		OrchestratorAddress:           "0xpublicsla",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  1000,
		StatusSamples:                 100,
		PromptToFirstFrameSumMS:       100000,
		PromptToFirstFrameSampleCount: 100,
		E2ELatencySumMS:               100000,
		E2ELatencySampleCount:         100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   windowStart,
		Org:                           h.org + "_b",
		OrchestratorAddress:           "0xpublicsla",
		PipelineID:                    "text-to-image",
		ModelID:                       modelID,
		GPUID:                         gpuID,
		Region:                        region,
		RequestedSessions:             10,
		StartupSuccessSessions:        10,
		EffectiveFailedSessions:       0,
		HealthSignalCount:             10,
		HealthExpectedSignalCount:     10,
		OutputFPSSum:                  100,
		StatusSamples:                 1,
		PromptToFirstFrameSumMS:       10000,
		PromptToFirstFrameSampleCount: 1,
		E2ELatencySumMS:               5000,
		E2ELatencySampleCount:         1,
	})

	if diff := h.queryFloat(t, `SELECT abs(avg_output_fps - (output_fps_sum / nullIf(toFloat64(status_samples), 0.0))) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xpublicsla'`, h.org, windowStart); diff > 0.000001 {
		t.Fatalf("RULE-AGGREGATE-005: hourly avg_output_fps drift = %v, want <= 1e-6", diff)
	}
	if diff := h.queryFloat(t, `SELECT abs(avg_prompt_to_first_frame_ms - (prompt_to_first_frame_sum_ms / nullIf(toFloat64(prompt_to_first_frame_sample_count), 0.0))) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xpublicsla'`, h.org, windowStart); diff > 0.000001 {
		t.Fatalf("RULE-AGGREGATE-005: hourly avg_prompt_to_first_frame_ms drift = %v, want <= 1e-6", diff)
	}
	if diff := h.queryFloat(t, `SELECT abs(avg_e2e_latency_ms - (e2e_latency_sum_ms / nullIf(toFloat64(e2e_latency_sample_count), 0.0))) FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = '0xpublicsla'`, h.org, windowStart); diff > 0.000001 {
		t.Fatalf("RULE-AGGREGATE-005: hourly avg_e2e_latency_ms drift = %v, want <= 1e-6", diff)
	}
}

func TestRuleAggregate006_PublicRollupsUseSupportFieldsInsteadOfAveragingAverages(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(30 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	buildSession := func(org, streamID, requestID string, fps float64, samples int) []rawEvent {
		events := []rawEvent{
			{
				EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: org,
				Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-weighted","gpu_info":[{"id":"gpu-weighted","name":"L4","memory_total":24576}]}]}]`, orchAddr, orchURI),
				IngestedAt: ts.Add(-time.Minute),
			},
			{
				EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: org, Gateway: "gw-weighted",
				Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
				IngestedAt: ts,
			},
			{
				EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: org, Gateway: "gw-weighted",
				Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_few_processed_segments","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
				IngestedAt: ts.Add(time.Second),
			},
		}
		for i := 0; i < samples; i++ {
			eventTS := ts.Add(time.Duration(i+2) * time.Second)
			events = append(events, rawEvent{
				EventID: uid("e"), EventType: "ai_stream_status", EventTs: eventTS, Org: org, Gateway: "gw-weighted",
				Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":%.1f,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI, fps),
				IngestedAt: eventTS,
			})
		}
		return events
	}

	events := append(
		buildSession(h.org, uid("orga"), uid("req"), 1.0, 10),
		buildSession(h.org+"_b", uid("orgb"), uid("req"), 100.0, 1)...,
	)
	h.insert(t, events)

	wantAvgFPS := 110.0 / 11.0

	if got := h.queryFloat(t, `SELECT round(sum(output_fps_sum) / nullIf(toFloat64(sum(status_samples)), 0.0), 6) FROM naap.api_hourly_streaming_demand WHERE window_start = ? AND gateway = 'gw-weighted' AND pipeline_id = 'text-to-image'`, windowStart); got != 10 {
		t.Fatalf("RULE-AGGREGATE-006: hourly demand avg_output_fps = %v, want 10", got)
	}
	if got := h.queryFloat(t, `SELECT round(sum(output_fps_sum) / nullIf(toFloat64(sum(status_samples)), 0.0), 6) FROM naap.api_hourly_streaming_gpu_metrics WHERE window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image' AND gpu_id = 'gpu-weighted'`, windowStart, orchAddr); got != 10 {
		t.Fatalf("RULE-AGGREGATE-006: hourly gpu metrics support-field rollup = %v, want 10", got)
	}
	if got := h.queryFloat(t, `SELECT round(sum(output_fps_sum) / nullIf(toFloat64(sum(status_samples)), 0.0), 6) FROM naap.api_hourly_streaming_demand WHERE window_start = ? AND gateway = 'gw-weighted' AND pipeline_id = 'text-to-image'`, windowStart); got != 10 {
		t.Fatalf("RULE-AGGREGATE-006: hourly demand support-field recomputation = %v, want 10", got)
	}
	if got := h.queryFloat(t, `SELECT round(sum(output_fps_sum) / nullIf(toFloat64(sum(status_samples)), 0.0), 6) FROM naap.api_hourly_streaming_gpu_metrics WHERE window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image' AND gpu_id = 'gpu-weighted'`, windowStart, orchAddr); got != 10 {
		t.Fatalf("RULE-AGGREGATE-006: hourly gpu metrics support-field recomputation = %v, want 10", got)
	}
	if got := h.queryFloat(t, `SELECT round(avg_output_fps, 6) FROM naap.api_hourly_streaming_demand WHERE org = ? AND window_start = ? AND gateway = 'gw-weighted' AND pipeline_id = 'text-to-image'`, h.org, windowStart); got != 1 {
		t.Fatalf("RULE-AGGREGATE-006: hourly demand avg_output_fps = %v, want 1", got)
	}
	if got := h.queryFloat(t, `SELECT round(avg_output_fps, 6) FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image' AND gpu_id = 'gpu-weighted'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-AGGREGATE-006: hourly gpu metrics avg_output_fps = %v, want 1", got)
	}
	if diff := h.queryFloat(t, `SELECT abs((sum(output_fps_sum) / nullIf(toFloat64(sum(status_samples)), 0.0)) - ?) FROM naap.api_hourly_streaming_gpu_metrics WHERE window_start = ? AND orchestrator_address = ? AND pipeline_id = 'text-to-image' AND gpu_id = 'gpu-weighted'`, wantAvgFPS, windowStart, orchAddr); diff > 0.000001 {
		t.Fatalf("RULE-AGGREGATE-006: hourly gpu metrics avg_output_fps drift = %v, want <= 1e-6", diff)
	}
}

func TestRuleAggregate004AndServing001_HardwareUnresolvedRowsStayOutOfGPUMetricsButRemainVisibleInGPUDemand(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(25 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	streamID := uid("hwless")
	requestID := uid("req")
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":"model-hwless"}]}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(-time.Minute),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org, Gateway: "gw-hwless",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image","orchestrator_info":{"address":%q,"url":%q}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(2 * time.Second), Org: h.org, Gateway: "gw-hwless",
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":9.0,"restart_count":0,"last_error":""}}`, streamID, requestID, orchAddr, orchURI),
			IngestedAt: ts.Add(2 * time.Second),
		},
	})

	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_gpu_metrics WHERE org = ? AND window_start = ? AND orchestrator_address = ?`, h.org, windowStart, orchAddr); got != 0 {
		t.Fatalf("RULE-AGGREGATE-004: hardware-unresolved row leaked into gpu metrics, got %d rows", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND gpu_id IS NULL`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-SERVING-001: hourly streaming SLA row count = %d, want 1", got)
	}
	if got := h.queryInt(t, `SELECT count() FROM naap.api_hourly_streaming_sla WHERE org = ? AND window_start = ? AND orchestrator_address = ? AND gpu_id IS NULL AND model_id = 'model-hwless'`, h.org, windowStart, orchAddr); got != 1 {
		t.Fatalf("RULE-SERVING-001: hourly streaming SLA did not preserve null gpu_id for hardware-unresolved row")
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

	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ?`, h.org) != 2 {
		t.Errorf("RULE-TYPED_RAW-002: capability fanout did not produce exactly 2 canonical snapshots")
	}
	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ? AND orch_address = ''`, h.org) != 0 {
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

	if got := h.queryString(t, `SELECT name FROM naap.canonical_latest_orchestrator_state WHERE orch_address = ?`, orchWithName); got != "friendly-name" {
		t.Errorf("RULE-TYPED_RAW-002: latest capability name = %q, want friendly-name", got)
	}
	if got := h.queryString(t, `SELECT name FROM naap.canonical_latest_orchestrator_state WHERE orch_address = ?`, orchNoName); got != orchNoName {
		t.Errorf("RULE-TYPED_RAW-002: latest capability fallback name = %q, want %q", got, orchNoName)
	}
}
