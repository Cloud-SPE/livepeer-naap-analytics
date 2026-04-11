//go:build validation

package validation

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func insertAIBatchStoreRow(t *testing.T, h *harness, org, requestID, pipeline string, completedAt time.Time) {
	t.Helper()
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_ai_batch_job_store
		(
			request_id, org, gateway, pipeline, model_id, received_at, completed_at, success, tries,
			duration_ms, orch_url, orch_url_norm, selection_outcome, latency_score, price_per_unit, error_type, error,
			attribution_status, attribution_reason, attribution_method, attribution_confidence,
			attributed_orch_uri, capability_version_id, attribution_snapshot_ts, gpu_id, gpu_model_name,
			gpu_memory_bytes_total, attributed_model, resolver_run_id
		)
		VALUES
		(
			?, ?, 'gw-alerts', ?, 'model-alerts', ?, ?, 1, 1,
			1000, 'https://orch.example.com', 'https://orch.example.com', 'selected', 1.0, 0.01, '', '',
			'resolved', 'validation', 'validation', 'high',
			'https://orch.example.com', NULL, NULL, NULL, NULL,
			NULL, NULL, 'validation'
		)
	`, requestID, org, pipeline, completedAt.Add(-2*time.Second), completedAt); err != nil {
		t.Fatalf("insert ai batch store row: %v", err)
	}
}

func insertBYOCStoreRow(t *testing.T, h *harness, org, eventID, capability string, completedAt time.Time) {
	t.Helper()
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.canonical_byoc_job_store
		(
			event_id, org, gateway, capability, completed_at, success, duration_ms, http_status,
			orch_address, orch_url, orch_url_norm, selection_outcome, worker_url, charged_compute, error, model,
			price_per_unit, attribution_status, attribution_reason, attribution_method,
			attribution_confidence, attributed_orch_uri, capability_version_id, attribution_snapshot_ts,
			gpu_id, gpu_model_name, gpu_memory_bytes_total, resolver_run_id
		)
		VALUES
		(
			?, ?, 'gw-alerts', ?, ?, 1, 1500, 200,
			'0xbyoc', 'https://byoc.example.com', 'https://byoc.example.com', 'selected', 'https://worker.example.com', 1, '',
			'model-byoc', 0.02, 'resolved', 'validation', 'validation',
			'high', 'https://byoc.example.com', NULL, NULL,
			NULL, NULL, NULL, 'validation'
		)
	`, eventID, org, capability, completedAt); err != nil {
		t.Fatalf("insert byoc store row: %v", err)
	}
}

func insertNormalizedAIBatchCompleted(t *testing.T, h *harness, org, eventID, requestID, pipeline string, eventTs time.Time) {
	t.Helper()
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.normalized_ai_batch_job
		(
			event_id, event_ts, org, gateway, request_id, pipeline, model_id, subtype, success, tries,
			duration_ms, orch_url, orch_url_norm, latency_score, price_per_unit, error_type, error
		)
		VALUES (?, ?, ?, 'gw-alerts', ?, ?, 'model-alerts', 'ai_batch_request_completed', 1, 1,
			1000, 'https://orch.example.com', 'https://orch.example.com', 1.0, 0.01, '', '')
	`, eventID, eventTs, org, requestID, pipeline); err != nil {
		t.Fatalf("insert normalized ai batch row: %v", err)
	}
}

func insertNormalizedBYOCCompleted(t *testing.T, h *harness, org, eventID, capability string, eventTs time.Time) {
	t.Helper()
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.normalized_byoc_job
		(
			event_id, event_ts, org, gateway, request_id, capability, subtype, source_event_type,
			success, duration_ms, http_status, orch_address, orch_url, orch_url_norm, worker_url,
			charged_compute, latency_ms, available_capacity, error
		)
		VALUES (?, ?, ?, 'gw-alerts', '', ?, 'job_gateway_completed', 'job_gateway',
			1, 1500, 200, '0xbyoc', 'https://byoc.example.com', 'https://byoc.example.com',
			'https://worker.example.com', 1, 1500, 1, '')
	`, eventID, eventTs, org, capability); err != nil {
		t.Fatalf("insert normalized byoc row: %v", err)
	}
}

func insertNetworkDemandSlice(t *testing.T, h *harness, org, gateway, pipeline string, windowStart time.Time, requested, effectiveFailed, healthSignals, healthExpected uint64) {
	t.Helper()
	refreshRunID := fmt.Sprintf("alert-net-%s-%d", org, windowStart.UnixNano())
	startupSuccess := requested - effectiveFailed
	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.api_network_demand_by_org_store
		(
			window_start, org, gateway, region, pipeline_id, model_id, sessions_count, avg_output_fps,
			output_fps_sum, status_samples, total_minutes, known_sessions_count, requested_sessions,
			startup_success_sessions, no_orch_sessions, startup_excused_sessions, startup_failed_sessions,
			loading_only_sessions, zero_output_fps_sessions, effective_failed_sessions, served_sessions,
			unserved_sessions, total_demand_sessions, startup_unexcused_sessions, confirmed_swapped_sessions,
			inferred_swap_sessions, total_swapped_sessions, sessions_ending_in_error, error_status_samples,
			health_signal_count, health_expected_signal_count, health_signal_coverage_ratio,
			startup_success_rate, excused_failure_rate, effective_success_rate, ticket_face_value_eth,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		VALUES
		(
			?, ?, ?, NULL, ?, NULL, ?, 0.0,
			0.0, ?, 0.0, ?, ?, ?,
			0, 0, ?, 0, 0, ?, ?,
			?, ?, ?, 0,
			0, 0, 0, 0,
			?, ?, ?, ?, 0.0, ?, 0.0,
			?, 'validation', now64()
		)
	`, windowStart, org, gateway, pipeline, requested, healthSignals, requested, requested, startupSuccess,
		effectiveFailed, effectiveFailed, startupSuccess, effectiveFailed, requested, effectiveFailed,
		healthSignals, healthExpected, ratioOrOne(healthSignals, healthExpected), ratioOrOne(startupSuccess, requested),
		1.0-ratioOrZero(effectiveFailed, requested), refreshRunID); err != nil {
		t.Fatalf("insert network demand slice: %v", err)
	}
}

func ratioOrOne(num, denom uint64) float64 {
	if denom == 0 {
		return 1.0
	}
	return float64(num) / float64(denom)
}

func ratioOrZero(num, denom uint64) float64 {
	if denom == 0 {
		return 0.0
	}
	return float64(num) / float64(denom)
}

func TestGrafanaAlertSQL_RequestResponseDuplicateRowsTriggersForAIBatch(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_rr_dup_rows")
	completedAt := time.Now().UTC().Add(-10 * time.Minute)
	insertAIBatchStoreRow(t, h, h.org, "dup-request", "text-to-image", completedAt)
	insertAIBatchStoreRow(t, h, h.org+"_other", "dup-request", "text-to-image", completedAt)

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE job_type = 'ai-batch'`, sql)); got != 1 {
		t.Fatalf("expected ai-batch duplicate alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_RequestResponseDuplicateRowsTriggersForBYOC(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_rr_dup_rows")
	completedAt := time.Now().UTC().Add(-10 * time.Minute)
	insertBYOCStoreRow(t, h, h.org, "dup-byoc", "openai-chat-completions", completedAt)
	insertBYOCStoreRow(t, h, h.org+"_other", "dup-byoc", "openai-chat-completions", completedAt)

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE job_type = 'byoc'`, sql)); got != 1 {
		t.Fatalf("expected byoc duplicate alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_RequestResponseCoverageTriggersForBothJobTypes(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_rr_canon_cov_low")
	now := time.Now().UTC().Add(-30 * time.Minute)
	for i := 0; i < 20; i++ {
		insertNormalizedAIBatchCompleted(t, h, h.org, fmt.Sprintf("evt-ai-%02d", i), fmt.Sprintf("req-ai-%02d", i), "text-to-image", now)
		insertNormalizedBYOCCompleted(t, h, h.org, fmt.Sprintf("evt-byoc-%02d", i), "openai-chat-completions", now)
		if i < 10 {
			insertAIBatchStoreRow(t, h, h.org, fmt.Sprintf("req-ai-%02d", i), "text-to-image", now)
			insertBYOCStoreRow(t, h, h.org, fmt.Sprintf("evt-byoc-%02d", i), "openai-chat-completions", now)
		}
	}

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE job_type = 'ai-batch'`, sql)); got != 1 {
		t.Fatalf("expected ai-batch coverage alert row, got %d", got)
	}
	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE job_type = 'byoc'`, sql)); got != 1 {
		t.Fatalf("expected byoc coverage alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_RequestResponseDensityCollapseTriggersOnDrop(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_rr_density_drop")
	currentHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	for hour := 25; hour >= 1; hour-- {
		window := currentHour.Add(-time.Duration(hour) * time.Hour)
		for i := 0; i < 100; i++ {
			insertAIBatchStoreRow(t, h, h.org, fmt.Sprintf("rr-baseline-%02d-%03d", hour, i), "rr-density", window.Add(time.Duration(i)*time.Second))
		}
	}
	for i := 0; i < 10; i++ {
		insertAIBatchStoreRow(t, h, h.org, fmt.Sprintf("rr-current-%03d", i), "rr-density", currentHour.Add(time.Duration(i)*time.Second))
	}

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE job_type = 'ai-batch' AND pipeline_id = 'rr-density'`, sql)); got != 1 {
		t.Fatalf("expected request-response density collapse alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_StreamingHealthSignalCoverageTriggersOnLowCoverage(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_stream_sig_cov_low")
	windowStart := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	insertNetworkDemandSlice(t, h, h.org, "gw-stream-health", "stream-health-alert", windowStart, 100, 10, 40, 100)

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE pipeline_id = 'stream-health-alert'`, sql)); got != 1 {
		t.Fatalf("expected streaming health coverage alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_StreamingUnservedDemandTriggersOnHighFailureRate(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_stream_unserved_high")
	windowStart := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	insertNetworkDemandSlice(t, h, h.org, "gw-unserved-alert", "stream-unserved-alert", windowStart, 100, 50, 90, 100)

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE gateway = 'gw-unserved-alert' AND pipeline_id = 'stream-unserved-alert'`, sql)); got != 1 {
		t.Fatalf("expected streaming unserved demand alert row, got %d", got)
	}
}

func TestGrafanaAlertSQL_StreamingDensityCollapseTriggersOnDrop(t *testing.T) {
	h := newHarness(t)
	sql := firstRuleSQL(t, "naap_stream_density_drop")
	currentHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	for hour := 25; hour >= 1; hour-- {
		insertNetworkDemandSlice(t, h, h.org, fmt.Sprintf("gw-%02d", hour), "stream-density", currentHour.Add(-time.Duration(hour)*time.Hour), 100, 5, 100, 100)
	}
	insertNetworkDemandSlice(t, h, h.org, "gw-current", "stream-density", currentHour, 10, 1, 10, 10)

	if got := h.queryInt(t, fmt.Sprintf(`SELECT count() FROM (%s) WHERE pipeline_id = 'stream-density'`, sql)); got != 1 {
		t.Fatalf("expected streaming density collapse alert row, got %d", got)
	}
}
