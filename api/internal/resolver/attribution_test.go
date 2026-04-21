package resolver

import (
	"testing"
	"time"
)

func TestResolveSelectionDecision_UsesAliasAddressToResolveCanonicalInterval(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-alias",
		Org:               "daydream",
		SessionKey:        "sess-alias",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xfd7db765c9bfc1493b4c9ea6ddf61a7fc7d40d12",
		ObservedModelHint: "streamdiffusion-sdxl-faceid",
		InputHash:         "input-alias",
	}

	decision := resolveSelectionDecision(selection, indexIntervals([]CapabilityInterval{{
		VersionID:       "ver-alias",
		Org:             "daydream",
		OrchAddress:     "0x75fbf65a3dfe93545c9768f163e59a02daf08d36",
		AliasAddress:    selection.ObservedAddress,
		OrchURI:         "https://ai.brzeczyszczykiewicz.work:8935",
		OrchURINorm:     "https://ai.brzeczyszczykiewicz.work:8935",
		ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		Pipeline:        "live-video-to-video",
		Model:           "streamdiffusion-sdxl-faceid",
		GPUID:           "gpu-alias",
		HardwarePresent: true,
		IntervalHash:    "hash-alias",
		SnapshotEventID: "snap-alias",
		SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
	}}))

	if decision.Status != "resolved" {
		t.Fatalf("status = %q, want resolved", decision.Status)
	}
	if decision.AttributedOrchAddress != "0x75fbf65a3dfe93545c9768f163e59a02daf08d36" {
		t.Fatalf("attributed orch address = %q", decision.AttributedOrchAddress)
	}
	if decision.Method != "alias_interval_exact" {
		t.Fatalf("method = %q, want alias_interval_exact", decision.Method)
	}
}

func TestIsCompatible_ModelMismatchOnlyReturnsFalse(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-model",
		Org:               "daydream",
		SessionKey:        "sess-model",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xabc",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-model",
	}

	if isCompatible(selection, CapabilityInterval{
		Org:      "daydream",
		Pipeline: "live-video-to-video",
		Model:    "streamdiffusion-sdxl",
	}) {
		t.Fatalf("expected model mismatch to be incompatible")
	}
}

func TestIsCompatible_RecognizedPipelineMismatchReturnsFalse(t *testing.T) {
	selection := SelectionEvent{
		ID:               "sel-pipeline",
		Org:              "daydream",
		SessionKey:       "sess-pipeline",
		SelectionTS:      time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:  "0xabc",
		ObservedPipeline: "text-to-image",
		InputHash:        "input-pipeline",
	}

	if isCompatible(selection, CapabilityInterval{
		Org:      "daydream",
		Pipeline: "live-video-to-video",
		Model:    "streamdiffusion-sdxl",
	}) {
		t.Fatalf("expected recognized pipeline mismatch to be incompatible")
	}
}

func TestIsCompatible_BYOCModelHintMustMatchWhenPresent(t *testing.T) {
	selection := SelectionEvent{
		ID:                   "sel-byoc-model",
		Org:                  "acme",
		SessionKey:           "evt-1",
		SelectionTS:          time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		ObservedAddress:      "0xabc",
		ObservedPipeline:     "openai-chat-completions",
		ObservedModelHint:    "Qwen/Qwen2.5-14B-Instruct-AWQ",
		PipelineHintVerbatim: true,
		InputHash:            "input-byoc-model",
	}

	if isCompatible(selection, CapabilityInterval{
		Org:      "acme",
		Pipeline: "openai-chat-completions",
		Model:    "Llama-3-8B",
	}) {
		t.Fatalf("expected BYOC model mismatch to be incompatible")
	}
	if !isCompatible(selection, CapabilityInterval{
		Org:      "acme",
		Pipeline: "openai-chat-completions",
		Model:    "Qwen/Qwen2.5-14B-Instruct-AWQ",
	}) {
		t.Fatalf("expected BYOC model match to be compatible")
	}
}

func TestResolveSelectionDecision_StaleOnlyCandidateIsStale(t *testing.T) {
	selection := SelectionEvent{
		ID:              "sel-stale",
		Org:             "daydream",
		SessionKey:      "sess-stale",
		SelectionTS:     time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress: "0xabc",
		InputHash:       "input-stale",
	}

	decision := resolveSelectionDecision(selection, indexIntervals([]CapabilityInterval{{
		VersionID:       "ver-stale",
		Org:             "daydream",
		OrchAddress:     "0xabc",
		ValidFromTS:     time.Date(2026, 3, 28, 9, 0, 0, 0, time.UTC),
		Pipeline:        "live-video-to-video",
		Model:           "streamdiffusion-sdxl",
		GPUID:           "gpu-stale",
		HardwarePresent: true,
		IntervalHash:    "hash-stale",
		SnapshotEventID: "snap-stale",
		SnapshotTS:      time.Date(2026, 3, 28, 9, 0, 0, 0, time.UTC),
	}}))

	if decision.Status != "stale" {
		t.Fatalf("status = %q, want stale", decision.Status)
	}
	if decision.Reason != "stale_candidate" {
		t.Fatalf("reason = %q, want stale_candidate", decision.Reason)
	}
}

func TestResolveSelectionDecision_MissingURISnapshotClassifiesAliasGap(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-uri-gap",
		Org:               "daydream",
		SessionKey:        "sess-uri-gap",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xfd7db765c9bfc1493b4c9ea6ddf61a7fc7d40d12",
		ObservedURL:       "https://missing.example.com:8935",
		ObservedModelHint: "streamdiffusion-sdxl-faceid",
		InputHash:         "input-uri-gap",
	}

	decision := resolveSelectionDecision(selection, indexIntervals([]CapabilityInterval{{
		VersionID:       "ver-alias-gap",
		Org:             "daydream",
		OrchAddress:     "0x75fbf65a3dfe93545c9768f163e59a02daf08d36",
		AliasAddress:    selection.ObservedAddress,
		OrchURI:         "https://ai.brzeczyszczykiewicz.work:8935",
		OrchURINorm:     "https://ai.brzeczyszczykiewicz.work:8935",
		ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		Pipeline:        "live-video-to-video",
		Model:           "streamdiffusion-sdxl-faceid",
		GPUID:           "gpu-alias-gap",
		HardwarePresent: true,
		IntervalHash:    "hash-alias-gap",
		SnapshotEventID: "snap-alias-gap",
		SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
	}}))

	if decision.Status != "unresolved" {
		t.Fatalf("status = %q, want unresolved", decision.Status)
	}
	if decision.Reason != "missing_uri_snapshot_local_alias_present" {
		t.Fatalf("reason = %q, want missing_uri_snapshot_local_alias_present", decision.Reason)
	}
}

func TestMaterializeDecision_CollapsesSameCanonicalMultiGPU(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-1",
		Org:               "daydream",
		SessionKey:        "sess-1",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xabc",
		ObservedURL:       "https://orch.example.com:8935",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-1",
	}
	matches := []matchedInterval{
		{
			method: "uri_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-1",
				OrchAddress:     "0xabc",
				OrchURI:         "https://orch.example.com:8935",
				OrchURINorm:     "https://orch.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-a",
				HardwarePresent: true,
				IntervalHash:    "hash-a",
				SnapshotEventID: "snap-1",
				SnapshotTS:      selection.SelectionTS,
			},
		},
		{
			method: "uri_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-1",
				OrchAddress:     "0xabc",
				OrchURI:         "https://orch.example.com:8935",
				OrchURINorm:     "https://orch.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-b",
				HardwarePresent: true,
				IntervalHash:    "hash-b",
				SnapshotEventID: "snap-1",
				SnapshotTS:      selection.SelectionTS,
			},
		},
	}

	decision := materializeDecision(selection, matches, "resolved", "matched")
	if decision.Status != "resolved" {
		t.Fatalf("status = %q, want resolved", decision.Status)
	}
	if decision.CanonicalModel != "streamdiffusion-sdxl-v2v" {
		t.Fatalf("canonical model = %q", decision.CanonicalModel)
	}
	if decision.GPUID != "" {
		t.Fatalf("gpu id = %q, want blank for session-level multi-gpu collapse", decision.GPUID)
	}
}

func TestMaterializeDecision_SameCanonicalAddressAcrossURIsRemainsAmbiguous(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-uri-fanout",
		Org:               "daydream",
		SessionKey:        "sess-uri-fanout",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xabc",
		ObservedURL:       "https://orch-1.example.com:8935",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-uri-fanout",
	}
	matches := []matchedInterval{
		{
			method: "address_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-1",
				OrchAddress:     "0xabc",
				OrchURI:         "https://orch-1.example.com:8935",
				OrchURINorm:     "https://orch-1.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-a",
				HardwarePresent: true,
				IntervalHash:    "hash-a",
				SnapshotEventID: "snap-1",
				SnapshotTS:      selection.SelectionTS,
			},
		},
		{
			method: "address_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-2",
				OrchAddress:     "0xabc",
				OrchURI:         "https://orch-2.example.com:8935",
				OrchURINorm:     "https://orch-2.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-b",
				HardwarePresent: true,
				IntervalHash:    "hash-b",
				SnapshotEventID: "snap-2",
				SnapshotTS:      selection.SelectionTS,
			},
		},
	}

	decision := materializeDecision(selection, matches, "resolved", "matched")
	if decision.Status != "ambiguous" {
		t.Fatalf("status = %q, want ambiguous", decision.Status)
	}
	if decision.Reason != "ambiguous_candidates" {
		t.Fatalf("reason = %q, want ambiguous_candidates", decision.Reason)
	}
}

func TestMaterializeDecision_TrueDifferentCanonicalAddressesRemainAmbiguous(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-ambiguous",
		Org:               "daydream",
		SessionKey:        "sess-ambiguous",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xabc",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-ambiguous",
	}
	matches := []matchedInterval{
		{
			method: "alias_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-1",
				OrchAddress:     "0xabc",
				AliasAddress:    "0xproxy",
				OrchURI:         "https://orch-1.example.com:8935",
				OrchURINorm:     "https://orch-1.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-a",
				HardwarePresent: true,
				IntervalHash:    "hash-a",
				SnapshotEventID: "snap-1",
				SnapshotTS:      selection.SelectionTS,
			},
		},
		{
			method: "alias_interval_exact",
			row: CapabilityInterval{
				VersionID:       "ver-2",
				OrchAddress:     "0xdef",
				AliasAddress:    "0xproxy",
				OrchURI:         "https://orch-2.example.com:8935",
				OrchURINorm:     "https://orch-2.example.com:8935",
				Pipeline:        "live-video-to-video",
				Model:           "streamdiffusion-sdxl-v2v",
				GPUID:           "gpu-b",
				HardwarePresent: true,
				IntervalHash:    "hash-b",
				SnapshotEventID: "snap-2",
				SnapshotTS:      selection.SelectionTS,
			},
		},
	}

	decision := materializeDecision(selection, matches, "resolved", "matched")
	if decision.Status != "ambiguous" {
		t.Fatalf("status = %q, want ambiguous", decision.Status)
	}
	if decision.Reason != "ambiguous_candidates" {
		t.Fatalf("reason = %q, want ambiguous_candidates", decision.Reason)
	}
}

func TestResolveSelectionDecision_URIFirstOverridesAddressFallback(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-uri-first",
		Org:               "daydream",
		SessionKey:        "sess-uri-first",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xproxy",
		ObservedURL:       "https://node-a.example.com:8935",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-uri-first",
	}

	indexed := indexIntervals([]CapabilityInterval{
		{
			VersionID:       "ver-uri",
			Org:             "daydream",
			OrchAddress:     "0xcanonical-a",
			AliasAddress:    "0xproxy",
			OrchURI:         "https://node-a.example.com:8935",
			OrchURINorm:     "https://node-a.example.com:8935",
			ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
			Pipeline:        "live-video-to-video",
			Model:           "streamdiffusion-sdxl-v2v",
			GPUID:           "gpu-a",
			HardwarePresent: true,
			IntervalHash:    "hash-uri",
			SnapshotEventID: "snap-uri",
			SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		},
		{
			VersionID:       "ver-alias",
			Org:             "daydream",
			OrchAddress:     "0xcanonical-b",
			AliasAddress:    "0xproxy",
			OrchURI:         "https://node-b.example.com:8935",
			OrchURINorm:     "https://node-b.example.com:8935",
			ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
			Pipeline:        "live-video-to-video",
			Model:           "streamdiffusion-sdxl-v2v",
			GPUID:           "gpu-b",
			HardwarePresent: true,
			IntervalHash:    "hash-alias",
			SnapshotEventID: "snap-alias",
			SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		},
	})

	decision := resolveSelectionDecision(selection, indexed)
	if decision.Status != "resolved" {
		t.Fatalf("status = %q, want resolved", decision.Status)
	}
	if decision.AttributedOrchAddress != "0xcanonical-a" {
		t.Fatalf("attributed orch address = %q, want 0xcanonical-a", decision.AttributedOrchAddress)
	}
	if decision.AttributedOrchURI != "https://node-a.example.com:8935" {
		t.Fatalf("attributed orch uri = %q", decision.AttributedOrchURI)
	}
	if decision.Method != "uri_interval_exact" {
		t.Fatalf("method = %q, want uri_interval_exact", decision.Method)
	}
}

func TestResolveSelectionDecision_URIAbsentAllowsAliasFallback(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-uri-absent",
		Org:               "daydream",
		SessionKey:        "sess-uri-absent",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xproxy",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-uri-absent",
	}

	decision := resolveSelectionDecision(selection, indexIntervals([]CapabilityInterval{{
		VersionID:       "ver-alias",
		Org:             "daydream",
		OrchAddress:     "0xcanonical-b",
		AliasAddress:    "0xproxy",
		OrchURI:         "https://node-b.example.com:8935",
		OrchURINorm:     "https://node-b.example.com:8935",
		ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		Pipeline:        "live-video-to-video",
		Model:           "streamdiffusion-sdxl-v2v",
		GPUID:           "gpu-b",
		HardwarePresent: true,
		IntervalHash:    "hash-alias",
		SnapshotEventID: "snap-alias",
		SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
	}}))

	if decision.Status != "resolved" {
		t.Fatalf("status = %q, want resolved", decision.Status)
	}
	if decision.AttributedOrchAddress != "0xcanonical-b" {
		t.Fatalf("attributed orch address = %q, want 0xcanonical-b", decision.AttributedOrchAddress)
	}
	if decision.Method != "alias_interval_exact" {
		t.Fatalf("method = %q, want alias_interval_exact", decision.Method)
	}
}

func TestResolveSelectionDecision_URIPresentWithoutURIMatchDoesNotFallbackToAlias(t *testing.T) {
	selection := SelectionEvent{
		ID:                "sel-no-uri-match",
		Org:               "daydream",
		SessionKey:        "sess-no-uri-match",
		SelectionTS:       time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		ObservedAddress:   "0xproxy",
		ObservedURL:       "https://node-a.example.com:8935",
		ObservedModelHint: "streamdiffusion-sdxl-v2v",
		InputHash:         "input-no-uri-match",
	}

	decision := resolveSelectionDecision(selection, indexIntervals([]CapabilityInterval{{
		VersionID:       "ver-alias",
		Org:             "daydream",
		OrchAddress:     "0xcanonical-b",
		AliasAddress:    "0xproxy",
		OrchURI:         "https://node-b.example.com:8935",
		OrchURINorm:     "https://node-b.example.com:8935",
		ValidFromTS:     time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
		Pipeline:        "live-video-to-video",
		Model:           "streamdiffusion-sdxl-v2v",
		GPUID:           "gpu-b",
		HardwarePresent: true,
		IntervalHash:    "hash-alias",
		SnapshotEventID: "snap-alias",
		SnapshotTS:      time.Date(2026, 3, 28, 9, 30, 0, 0, time.UTC),
	}}))

	if decision.Status != "unresolved" {
		t.Fatalf("status = %q, want unresolved", decision.Status)
	}
	if decision.Reason != "missing_uri_snapshot_local_alias_present" {
		t.Fatalf("reason = %q, want missing_uri_snapshot_local_alias_present", decision.Reason)
	}
}

func TestIsCompatible_VerbatimPipelineMatchesCaseInsensitive(t *testing.T) {
	selection := SelectionEvent{
		ObservedPipeline:     "openai-chat-completions",
		PipelineHintVerbatim: true,
	}
	if !isCompatible(selection, CapabilityInterval{Pipeline: "openai-chat-completions"}) {
		t.Fatalf("expected verbatim match to be compatible")
	}
	if !isCompatible(selection, CapabilityInterval{Pipeline: "OpenAI-Chat-Completions"}) {
		t.Fatalf("expected case-insensitive verbatim match to be compatible")
	}
}

func TestIsCompatible_VerbatimPipelineMismatchReturnsFalse(t *testing.T) {
	selection := SelectionEvent{
		ObservedPipeline:     "openai-chat-completions",
		PipelineHintVerbatim: true,
	}
	if isCompatible(selection, CapabilityInterval{Pipeline: "openai-image-generation"}) {
		t.Fatalf("expected verbatim mismatch to be incompatible")
	}
}

func TestIsCompatible_VerbatimEmptyObservedPipelineReturnsFalse(t *testing.T) {
	selection := SelectionEvent{
		ObservedPipeline:     "",
		PipelineHintVerbatim: true,
	}
	if isCompatible(selection, CapabilityInterval{Pipeline: "openai-chat-completions"}) {
		t.Fatalf("expected empty verbatim pipeline to be incompatible")
	}
}

func TestIsCompatible_VerbatimEmptyIntervalPipelineReturnsFalse(t *testing.T) {
	selection := SelectionEvent{
		ObservedPipeline:     "openai-chat-completions",
		PipelineHintVerbatim: true,
	}
	if isCompatible(selection, CapabilityInterval{Pipeline: ""}) {
		t.Fatalf("expected empty interval pipeline to be incompatible with verbatim match")
	}
}

func TestIsCompatible_AIBatchPipelineUsesCanonicalAllowList(t *testing.T) {
	// AI batch uses PipelineHintVerbatim = false; pipeline must be in allow-list.
	selection := SelectionEvent{
		ObservedPipeline:     "segment-anything-2",
		PipelineHintVerbatim: false,
	}
	if !isCompatible(selection, CapabilityInterval{Pipeline: "segment-anything-2"}) {
		t.Fatalf("expected segment-anything-2 to be compatible via canonical allow-list")
	}
	if isCompatible(selection, CapabilityInterval{Pipeline: "text-to-image"}) {
		t.Fatalf("expected pipeline mismatch to be incompatible")
	}
}

func TestBuildSelectionEvents_UsesExplicitModelHintField(t *testing.T) {
	rows := []selectionCandidate{{
		Org:            "daydream",
		SessionKey:     "sess-1",
		EventID:        "evt-1",
		EventType:      "ai_stream_status",
		EventTS:        time.Date(2026, 3, 28, 9, 35, 45, 0, time.UTC),
		SourcePriority: 2,
		OrchAddress:    "0xABC",
		OrchURL:        "https://orch.example.com:8935",
		PipelineHint:   "",
		ModelHint:      "streamdiffusion-sdxl-v2v",
	}}

	events := buildSelectionEvents(rows)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].ObservedModelHint != "streamdiffusion-sdxl-v2v" {
		t.Fatalf("observed model hint = %q", events[0].ObservedModelHint)
	}
	if events[0].ObservedPipeline != "live-video-to-video" {
		t.Fatalf("observed pipeline = %q, want live-video-to-video", events[0].ObservedPipeline)
	}
}
