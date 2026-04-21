//go:build validation

package validation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/config"
	clickhouserepo "github.com/livepeer/naap-analytics/internal/repo/clickhouse"
	"github.com/livepeer/naap-analytics/internal/types"
)

func TestRuleServing002_DiscoverOrchestratorsUsesLatestObservedURIForRequestScoring(t *testing.T) {
	h := newHarness(t)
	ts := anchor().Add(15 * time.Minute)
	windowStart := ts.Truncate(time.Hour)
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("discover")))
	oldURI := fmt.Sprintf("https://old-%s.example.com", uid("uri"))
	newURI := fmt.Sprintf("https://new-%s.example.com", uid("uri"))
	modelID := uid("model")
	cap := "text-to-image/" + modelID

	h.insertRaw(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-40 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"discover-old","uri":%q,"version":"0.9.0","hardware":[{"pipeline":"text-to-image","model_id":%q,"gpu_info":[{"id":"gpu-discover","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, oldURI, modelID),
			IngestedAt: ts.Add(-40 * time.Minute),
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(-5 * time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"discover-new","uri":%q,"version":"0.9.1","hardware":[{"pipeline":"text-to-image","model_id":%q,"gpu_info":[{"id":"gpu-discover","name":"L4","memory_total":24576}]}]}]`,
				orchAddr, newURI, modelID),
			IngestedAt: ts.Add(-5 * time.Minute),
		},
	})

	if err := h.conn.Exec(context.Background(), `
		INSERT INTO naap.api_hourly_request_demand_store
		(
			window_start, org, gateway, execution_mode, capability_family,
			capability_name, capability_id, canonical_pipeline, pipeline_id, canonical_model,
			orchestrator_address, orchestrator_uri,
			job_count, selected_count, no_orch_count, success_count, duration_ms_sum, price_sum,
			llm_request_count, llm_success_count, llm_total_tokens_sum, llm_total_tokens_sample_count,
			llm_tokens_per_second_sum, llm_tokens_per_second_sample_count, llm_ttft_ms_sum, llm_ttft_ms_sample_count,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		VALUES
		(
			?, ?, 'gw-discover', 'request', 'builtin',
			'text-to-image', NULL, 'text-to-image', 'text-to-image', ?,
			'', ?, 5, 5, 0, 4, 5000, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			'discover-test-run', '', ?
		)
	`, windowStart, h.org, modelID, newURI, ts.UTC()); err != nil {
		t.Fatalf("insert api_hourly_request_demand_store seed: %v", err)
	}

	repo, err := clickhouserepo.New(&config.Config{
		ClickHouseAddr:     envOrDefault("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:       envOrDefault("CLICKHOUSE_DB", "naap"),
		ClickHouseUser:     envOrDefault("CLICKHOUSE_USER", "naap_reader"),
		ClickHousePassword: envOrDefault("CLICKHOUSE_PASSWORD", "naap_reader_changeme"),
		ClickHouseTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new clickhouse repo: %v", err)
	}
	defer func() { _ = repo.Close() }()

	rows, err := repo.DiscoverOrchestrators(context.Background(), types.DiscoverOrchestratorsParams{
		Caps: []string{cap},
	})
	if err != nil {
		t.Fatalf("DiscoverOrchestrators: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("DiscoverOrchestrators rows = %d, want 1", len(rows))
	}
	if rows[0].Address != newURI {
		t.Fatalf("DiscoverOrchestrators address = %q, want latest URI %q", rows[0].Address, newURI)
	}
	if rows[0].Score != 0.8 {
		t.Fatalf("DiscoverOrchestrators score = %v, want 0.8 from the latest URI demand row", rows[0].Score)
	}
	if !rows[0].RecentWork {
		t.Fatalf("DiscoverOrchestrators recent_work = false, want true")
	}
	if len(rows[0].Capabilities) != 1 || rows[0].Capabilities[0] != cap {
		t.Fatalf("DiscoverOrchestrators capabilities = %v, want [%q]", rows[0].Capabilities, cap)
	}
}
