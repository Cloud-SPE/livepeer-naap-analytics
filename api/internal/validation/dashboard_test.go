//go:build validation

package validation

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/config"
	clickhouserepo "github.com/livepeer/naap-analytics/internal/repo/clickhouse"
)

func TestRuleServing001_DashboardOrchestratorsUsesLatestHourlyContractedSLA(t *testing.T) {
	h := newHarness(t)
	olderWindow := anchor().Add(264 * time.Hour).Truncate(time.Hour)
	latestWindow := olderWindow.Add(24 * time.Hour)

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:               olderWindow,
		Org:                       h.org,
		OrchestratorAddress:       "0xdashboard-sla",
		PipelineID:                "text-to-image",
		ModelID:                   ptr("model-dashboard"),
		GPUID:                     ptr("gpu-dashboard"),
		GPUModelName:              ptr("NVIDIA L4"),
		RequestedSessions:         10,
		StartupSuccessSessions:    2,
		StartupFailedSessions:     8,
		OutputFailedSessions:      8,
		EffectiveFailedSessions:   8,
		HealthSignalCount:         10,
		HealthExpectedSignalCount: 10,
		OutputFPSSum:              100,
		StatusSamples:             100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   latestWindow,
		Org:                           h.org,
		OrchestratorAddress:           "0xdashboard-sla",
		PipelineID:                    "text-to-image",
		ModelID:                       ptr("model-dashboard"),
		GPUID:                         ptr("gpu-dashboard"),
		GPUModelName:                  ptr("NVIDIA L4"),
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

	repo, err := clickhouserepo.New(&config.Config{
		ClickHouseAddr:     os.Getenv("CLICKHOUSE_ADDR"),
		ClickHouseDB:       envOrDefault("CLICKHOUSE_DB", "naap"),
		ClickHouseUser:     envOrDefault("CLICKHOUSE_WRITER_USER", "naap_writer"),
		ClickHousePassword: envOrDefault("CLICKHOUSE_WRITER_PASSWORD", "naap_writer_changeme"),
		ClickHouseTimeout:  10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create clickhouse repo: %v", err)
	}
	defer func() { _ = repo.Close() }()

	rows, err := repo.GetDashboardOrchestrators(context.Background(), 24*30)
	if err != nil {
		t.Fatalf("GetDashboardOrchestrators: %v", err)
	}

	expectedScore := h.queryFloat(t, `SELECT sla_score FROM naap.api_hourly_streaming_sla WHERE window_start = ? AND orchestrator_address = '0xdashboard-sla'`, latestWindow)
	expectedWindow := latestWindow.UTC().Format(time.RFC3339)

	for _, row := range rows {
		if row.Address != "0xdashboard-sla" {
			continue
		}
		if row.SLAScore == nil || *row.SLAScore != expectedScore {
			t.Fatalf("RULE-SERVING-001: dashboard slaScore = %v, want %v", row.SLAScore, expectedScore)
		}
		if row.SLAWindowStart == nil || *row.SLAWindowStart != expectedWindow {
			t.Fatalf("RULE-SERVING-001: dashboard slaWindowStart = %v, want %s", row.SLAWindowStart, expectedWindow)
		}
		return
	}

	t.Fatal("RULE-SERVING-001: dashboard orchestrator row not found")
}
