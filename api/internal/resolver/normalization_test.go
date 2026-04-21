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

func TestNormalizeCanonicalPipeline_RecognizesAllAIBatchPipelines(t *testing.T) {
	cases := []string{
		"text-to-image", "image-to-image", "image-to-video", "text-to-video",
		"audio-to-text", "text-to-speech",
		"upscale", "llm", "image-to-text", "segment-anything-2",
		"live-video-to-video", "noop",
	}
	for _, c := range cases {
		result := normalizeCanonicalPipeline(c)
		if result != c {
			t.Errorf("normalizeCanonicalPipeline(%q) = %q, want %q", c, result, c)
		}
		// Case-insensitive
		upper := "TEXT-TO-IMAGE"
		if c == "text-to-image" {
			if normalizeCanonicalPipeline(upper) != "text-to-image" {
				t.Errorf("case-insensitive normalization failed for %q", upper)
			}
		}
	}
}

func TestNormalizeCanonicalPipeline_RejectsBYOCCapabilityString(t *testing.T) {
	// BYOC capability strings must not be recognized by the canonical allow-list.
	// They use PipelineHintVerbatim = true and bypass this function.
	if got := normalizeCanonicalPipeline("openai-chat-completions"); got != "" {
		t.Fatalf("normalizeCanonicalPipeline(%q) = %q, want empty", "openai-chat-completions", got)
	}
}

func TestCompatiblePipelineHint_RecognizesSegmentAnything2(t *testing.T) {
	if got := compatiblePipelineHint("segment-anything-2"); got != "segment-anything-2" {
		t.Fatalf("compatiblePipelineHint = %q, want segment-anything-2", got)
	}
}

func TestCompatiblePipelineHint_RecognizesImageToVideo(t *testing.T) {
	if got := compatiblePipelineHint("image-to-video"); got != "image-to-video" {
		t.Fatalf("compatiblePipelineHint = %q, want image-to-video", got)
	}
}

func TestCompatiblePipelineHint_RecognizesUpscale(t *testing.T) {
	if got := compatiblePipelineHint("upscale"); got != "upscale" {
		t.Fatalf("compatiblePipelineHint = %q, want upscale", got)
	}
}

func TestCompatiblePipelineHint_RecognizesLLM(t *testing.T) {
	if got := compatiblePipelineHint("llm"); got != "llm" {
		t.Fatalf("compatiblePipelineHint = %q, want llm", got)
	}
}

func TestCompatiblePipelineHint_RecognizesImageToText(t *testing.T) {
	if got := compatiblePipelineHint("image-to-text"); got != "image-to-text" {
		t.Fatalf("compatiblePipelineHint = %q, want image-to-text", got)
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
