package resolver

import (
	"testing"
	"time"
)

func TestNormalizeObservedHints_ModelLikePipelineAlias(t *testing.T) {
	pipeline, model := normalizeObservedHints("pip_SDXL-turbo-v2v", "")
	if pipeline != "live-video-to-video" {
		t.Fatalf("pipeline = %q, want live-video-to-video", pipeline)
	}
	if model != "streamdiffusion-sdxl-v2v" {
		t.Fatalf("model = %q, want streamdiffusion-sdxl-v2v", model)
	}
}

func TestNormalizeObservedHints_StreamdiffusionPipelineBecomesModelHint(t *testing.T) {
	pipeline, model := normalizeObservedHints("streamdiffusion-sdxl", "")
	if pipeline != "live-video-to-video" {
		t.Fatalf("pipeline = %q, want live-video-to-video", pipeline)
	}
	if model != "streamdiffusion-sdxl" {
		t.Fatalf("model = %q, want streamdiffusion-sdxl", model)
	}
}

func TestBuildSessionCurrentRows_NormalizesFallbackHints(t *testing.T) {
	lastSeen := time.Date(2026, 3, 28, 12, 5, 0, 0, time.UTC)
	rows := buildSessionCurrentRows(map[string]SessionEvidence{
		"sess-1": {
			Org:               "daydream",
			SessionKey:        "sess-1",
			EventLastSeen:     &lastSeen,
			EventPipelineHint: "pip_SDXL-turbo",
		},
	}, nil)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].CanonicalPipeline != "live-video-to-video" {
		t.Fatalf("canonical pipeline = %q, want live-video-to-video", rows[0].CanonicalPipeline)
	}
	if rows[0].CanonicalModel != "streamdiffusion-sdxl" {
		t.Fatalf("canonical model = %q, want streamdiffusion-sdxl", rows[0].CanonicalModel)
	}
}

func TestBuildSessionCurrentRows_DerivesSessionEdgeLatencies(t *testing.T) {
	startedAt := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	firstProcessedAt := startedAt.Add(4 * time.Second)
	firstIngestAt := startedAt.Add(1500 * time.Millisecond)
	runnerFirstProcessedAt := startedAt.Add(4200 * time.Millisecond)
	statusStartTime := startedAt.Add(-2 * time.Second)
	fewProcessedAt := startedAt.Add(5 * time.Second)
	lastSeen := startedAt.Add(6 * time.Second)

	rows := buildSessionCurrentRows(map[string]SessionEvidence{
		"sess-1": {
			Org:                    "daydream",
			SessionKey:             "sess-1",
			StartedAt:              &startedAt,
			FirstProcessedAt:       &firstProcessedAt,
			FirstIngestAt:          &firstIngestAt,
			RunnerFirstProcessedAt: &runnerFirstProcessedAt,
			StatusStartTime:        &statusStartTime,
			FewProcessedAt:         &fewProcessedAt,
			EventLastSeen:          &lastSeen,
		},
	}, nil)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].StartupLatencyMS == nil || *rows[0].StartupLatencyMS != 4000 {
		t.Fatalf("startup latency = %v, want 4000", rows[0].StartupLatencyMS)
	}
	if rows[0].E2ELatencyMS == nil || *rows[0].E2ELatencyMS != 2700 {
		t.Fatalf("e2e latency = %v, want 2700", rows[0].E2ELatencyMS)
	}
	if rows[0].PromptToPlayableLatencyMS == nil || *rows[0].PromptToPlayableLatencyMS != 7000 {
		t.Fatalf("prompt-to-playable latency = %v, want 7000", rows[0].PromptToPlayableLatencyMS)
	}
}

func TestBuildSessionCurrentRows_DropsNegativeSessionEdgeLatencies(t *testing.T) {
	startedAt := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	firstProcessedAt := startedAt.Add(-1 * time.Second)
	lastSeen := startedAt.Add(5 * time.Second)

	rows := buildSessionCurrentRows(map[string]SessionEvidence{
		"sess-1": {
			Org:              "daydream",
			SessionKey:       "sess-1",
			StartedAt:        &startedAt,
			FirstProcessedAt: &firstProcessedAt,
			EventLastSeen:    &lastSeen,
		},
	}, nil)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].StartupLatencyMS != nil {
		t.Fatalf("startup latency = %v, want nil", rows[0].StartupLatencyMS)
	}
	if rows[0].E2ELatencyMS != nil {
		t.Fatalf("e2e latency = %v, want nil", rows[0].E2ELatencyMS)
	}
	if rows[0].PromptToPlayableLatencyMS != nil {
		t.Fatalf("prompt-to-playable latency = %v, want nil", rows[0].PromptToPlayableLatencyMS)
	}
}
