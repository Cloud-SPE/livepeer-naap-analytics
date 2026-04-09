package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/types"
)

func TestBuildSLAWhere_IncludesRegionFilter(t *testing.T) {
	start := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Hour)

	where, args := buildSLAWhere(types.SLAComplianceParams{
		Start:               start,
		End:                 end,
		Org:                 "cloudspe",
		OrchestratorAddress: "0xabc",
		Region:              "us-east",
		PipelineID:          "live-video-to-video",
		ModelID:             "sdxl",
		GPUID:               "gpu-1",
	})

	if !strings.Contains(where, "region = ?") {
		t.Fatalf("expected region filter in WHERE clause, got %q", where)
	}
	if got, want := len(args), 8; got != want {
		t.Fatalf("expected %d args, got %d (%v)", want, got, args)
	}
	if got := args[4]; got != "us-east" {
		t.Fatalf("expected region arg at index 4, got %v", got)
	}
}
