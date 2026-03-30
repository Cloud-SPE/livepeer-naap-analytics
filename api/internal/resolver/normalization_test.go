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
			Org:              "daydream",
			SessionKey:       "sess-1",
			EventLastSeen:    &lastSeen,
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

