//go:build validation

package validation

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/config"
	clickhouserepo "github.com/livepeer/naap-analytics/internal/repo/clickhouse"
	"github.com/livepeer/naap-analytics/internal/types"
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

// TestRuleServing002_DashboardOrchestratorsHonorsWindow seeds two hourly SLA
// rows for the same orchestrator: one inside the 1h window (perfect score)
// and one that lives only in the 168h window (half score). The 1h response
// must reflect only the recent row; the 168h response must reflect the
// weighted mix. If the two responses match, the window parameter is being
// silently ignored (the v2 regression this test guards against).
func TestRuleServing002_DashboardOrchestratorsHonorsWindow(t *testing.T) {
	h := newHarness(t)

	now := time.Now().UTC().Truncate(time.Hour)
	recentWindow := now.Add(-30 * time.Minute).Truncate(time.Hour)
	olderWindow := now.Add(-72 * time.Hour).Truncate(time.Hour)

	orchAddr := uid("0xwindowtest")

	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:               olderWindow,
		Org:                       h.org,
		OrchestratorAddress:       orchAddr,
		PipelineID:                "text-to-image",
		ModelID:                   ptr("model-window"),
		GPUID:                     ptr("gpu-window"),
		GPUModelName:              ptr("NVIDIA L4"),
		RequestedSessions:         10,
		StartupSuccessSessions:    5,
		StartupFailedSessions:     5,
		OutputFailedSessions:      5,
		EffectiveFailedSessions:   5,
		HealthSignalCount:         10,
		HealthExpectedSignalCount: 10,
		OutputFPSSum:              500,
		StatusSamples:             100,
	})
	insertSLAStoreSeed(t, h, slaStoreSeed{
		WindowStart:                   recentWindow,
		Org:                           h.org,
		OrchestratorAddress:           orchAddr,
		PipelineID:                    "text-to-image",
		ModelID:                       ptr("model-window"),
		GPUID:                         ptr("gpu-window"),
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

	findOrch := func(rows []types.DashboardOrchestrator) *types.DashboardOrchestrator {
		for i := range rows {
			if rows[i].Address == orchAddr {
				return &rows[i]
			}
		}
		return nil
	}

	rows1h, err := repo.GetDashboardOrchestrators(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetDashboardOrchestrators(1h): %v", err)
	}
	row1h := findOrch(rows1h)
	if row1h == nil {
		t.Fatalf("RULE-SERVING-002: 1h window did not return seeded orch %s", orchAddr)
	}
	if row1h.EffectiveSuccessRate == nil {
		t.Fatalf("RULE-SERVING-002: 1h window effectiveSuccessRate is nil")
	}

	rows168h, err := repo.GetDashboardOrchestrators(context.Background(), 168)
	if err != nil {
		t.Fatalf("GetDashboardOrchestrators(168h): %v", err)
	}
	row168h := findOrch(rows168h)
	if row168h == nil {
		t.Fatalf("RULE-SERVING-002: 168h window did not return seeded orch %s", orchAddr)
	}
	if row168h.EffectiveSuccessRate == nil {
		t.Fatalf("RULE-SERVING-002: 168h window effectiveSuccessRate is nil")
	}

	if *row1h.EffectiveSuccessRate == *row168h.EffectiveSuccessRate {
		t.Fatalf("RULE-SERVING-002: window is being ignored — 1h and 168h effectiveSuccessRate both = %v (should differ because older row only fits 168h)", *row1h.EffectiveSuccessRate)
	}
	if *row1h.EffectiveSuccessRate <= *row168h.EffectiveSuccessRate {
		t.Fatalf("RULE-SERVING-002: 1h effectiveSuccessRate (%v) should exceed 168h (%v) because only the perfect recent window is included at 1h",
			*row1h.EffectiveSuccessRate, *row168h.EffectiveSuccessRate)
	}
}
