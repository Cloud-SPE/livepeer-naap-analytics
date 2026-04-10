package resolver

import (
	"testing"
	"time"
)

// ---------- toSelectionEvent ----------

func TestAIBatchJobRecord_ToSelectionEvent_UsesReceivedAtWhenPresent(t *testing.T) {
	receivedAt := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	completedAt := time.Date(2026, 4, 1, 10, 0, 5, 0, time.UTC)
	j := AIBatchJobRecord{
		RequestID:   "req-1",
		Org:         "acme",
		Pipeline:    "text-to-image",
		ModelID:     "sdxl-turbo",
		OrchURL:     "https://orch.example.com:8935",
		OrchURLNorm: "https://orch.example.com:8935",
		ReceivedAt:  &receivedAt,
		CompletedAt: completedAt,
	}
	se := j.toSelectionEvent()

	if !se.SelectionTS.Equal(receivedAt) {
		t.Fatalf("SelectionTS = %v, want receivedAt %v", se.SelectionTS, receivedAt)
	}
	if !se.AnchorEventTS.Equal(completedAt) {
		t.Fatalf("AnchorEventTS = %v, want completedAt %v", se.AnchorEventTS, completedAt)
	}
	if se.ObservedURL != "https://orch.example.com:8935" {
		t.Fatalf("ObservedURL = %q", se.ObservedURL)
	}
	if se.PipelineHintVerbatim {
		t.Fatalf("PipelineHintVerbatim should be false for AI batch")
	}
	if se.ObservedPipeline != "text-to-image" {
		t.Fatalf("ObservedPipeline = %q, want text-to-image", se.ObservedPipeline)
	}
	if se.ObservedModelHint != "sdxl-turbo" {
		t.Fatalf("ObservedModelHint = %q, want sdxl-turbo", se.ObservedModelHint)
	}
	if se.ObservedAddress != "" {
		t.Fatalf("ObservedAddress should be empty for AI batch, got %q", se.ObservedAddress)
	}
}

func TestAIBatchJobRecord_ToSelectionEvent_FallsBackToCompletedAtWhenNoReceivedAt(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 10, 0, 5, 0, time.UTC)
	j := AIBatchJobRecord{
		RequestID:   "req-2",
		Org:         "acme",
		Pipeline:    "llm",
		ModelID:     "llama-3",
		OrchURL:     "https://orch.example.com:8935",
		OrchURLNorm: "https://orch.example.com:8935",
		ReceivedAt:  nil,
		CompletedAt: completedAt,
	}
	se := j.toSelectionEvent()

	if !se.SelectionTS.Equal(completedAt) {
		t.Fatalf("SelectionTS = %v, want completedAt %v", se.SelectionTS, completedAt)
	}
}

func TestBYOCJobRecord_ToSelectionEvent_SetsPipelineHintVerbatim(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)
	j := BYOCJobRecord{
		EventID:     "evt-byoc-1",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc123",
		OrchURL:     "https://byoc.example.com:8935",
		OrchURLNorm: "https://byoc.example.com:8935",
		CompletedAt: completedAt,
	}
	se := j.toSelectionEvent()

	if !se.PipelineHintVerbatim {
		t.Fatalf("PipelineHintVerbatim should be true for BYOC")
	}
	if se.ObservedPipeline != "openai-chat-completions" {
		t.Fatalf("ObservedPipeline = %q, want openai-chat-completions", se.ObservedPipeline)
	}
	if se.ObservedModelHint != "" {
		t.Fatalf("ObservedModelHint should be empty for BYOC (comes from worker_lifecycle), got %q", se.ObservedModelHint)
	}
	if se.ObservedAddress != "0xabc123" {
		t.Fatalf("ObservedAddress = %q, want 0xabc123", se.ObservedAddress)
	}
	if se.SessionKey != "evt-byoc-1" {
		t.Fatalf("SessionKey = %q, want evt-byoc-1", se.SessionKey)
	}
}

// ---------- resolveWorkerModels ----------

func TestResolveWorkerModels_PicksMostRecentSnapshotBeforeCompletion(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	jobs := []BYOCJobRecord{{
		EventID:     "evt-1",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc",
		CompletedAt: completedAt,
	}}
	snapshots := []workerLifecycleSnapshot{
		{
			Org:         "acme",
			Capability:  "openai-chat-completions",
			OrchAddress: "0xabc",
			EventTS:     time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC), // before completedAt
			Model:       "gpt-4o-mini",
			PricePerUnit: 0.01,
		},
		{
			Org:         "acme",
			Capability:  "openai-chat-completions",
			OrchAddress: "0xabc",
			EventTS:     time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), // older
			Model:       "gpt-3.5-turbo",
			PricePerUnit: 0.001,
		},
	}

	models := resolveWorkerModels(jobs, snapshots)
	wm, ok := models["evt-1"]
	if !ok {
		t.Fatalf("expected worker model for evt-1")
	}
	if wm.Model != "gpt-4o-mini" {
		t.Fatalf("model = %q, want gpt-4o-mini (most recent before completedAt)", wm.Model)
	}
}

func TestResolveWorkerModels_ExcludesSnapshotAfterCompletion(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	jobs := []BYOCJobRecord{{
		EventID:     "evt-1",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc",
		CompletedAt: completedAt,
	}}
	snapshots := []workerLifecycleSnapshot{
		{
			Org:         "acme",
			Capability:  "openai-chat-completions",
			OrchAddress: "0xabc",
			EventTS:     time.Date(2026, 4, 1, 13, 0, 0, 0, time.UTC), // after completedAt — must be excluded
			Model:       "gpt-4o",
			PricePerUnit: 0.02,
		},
	}

	models := resolveWorkerModels(jobs, snapshots)
	if _, ok := models["evt-1"]; ok {
		t.Fatalf("expected no worker model when only future snapshot exists")
	}
}

func TestResolveWorkerModels_NoMatchForDifferentCapability(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	jobs := []BYOCJobRecord{{
		EventID:     "evt-1",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc",
		CompletedAt: completedAt,
	}}
	snapshots := []workerLifecycleSnapshot{
		{
			Org:         "acme",
			Capability:  "openai-image-generation", // different capability
			OrchAddress: "0xabc",
			EventTS:     time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC),
			Model:       "dall-e-3",
			PricePerUnit: 0.05,
		},
	}

	models := resolveWorkerModels(jobs, snapshots)
	if _, ok := models["evt-1"]; ok {
		t.Fatalf("expected no match when capability differs")
	}
}

// ---------- buildAIBatchJobRows ----------

func TestBuildAIBatchJobRows_PropagatesAttributionDecision(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	snapshotTS := time.Date(2026, 4, 1, 11, 55, 0, 0, time.UTC)
	job := AIBatchJobRecord{
		RequestID:   "req-1",
		Org:         "acme",
		Pipeline:    "text-to-image",
		ModelID:     "sdxl-turbo",
		OrchURL:     "https://orch.example.com:8935",
		OrchURLNorm: "https://orch.example.com:8935",
		CompletedAt: completedAt,
	}
	se := job.toSelectionEvent()
	decisionsByID := map[string]SelectionDecision{
		se.ID: {
			SelectionEventID:    se.ID,
			Status:              "resolved",
			Reason:              "matched",
			Method:              "uri_interval_exact",
			Confidence:          "high",
			AttributedOrchURI:   "https://orch.example.com:8935",
			CapabilityVersionID: "ver-1",
			SnapshotTS:          &snapshotTS,
			GPUID:               "gpu-a",
			CanonicalModel:      "sdxl-turbo",
		},
	}

	rows := buildAIBatchJobRows([]AIBatchJobRecord{job}, decisionsByID)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]
	if r.AttributionStatus != "resolved" {
		t.Fatalf("status = %q, want resolved", r.AttributionStatus)
	}
	if r.GPUID != "gpu-a" {
		t.Fatalf("gpu_id = %q, want gpu-a", r.GPUID)
	}
	if r.AttributedModel != "sdxl-turbo" {
		t.Fatalf("attributed_model = %q, want sdxl-turbo", r.AttributedModel)
	}
}

func TestBuildAIBatchJobRows_DefaultsToUnresolvedWhenNoCandidateDecision(t *testing.T) {
	job := AIBatchJobRecord{
		RequestID:   "req-missing",
		Org:         "acme",
		Pipeline:    "llm",
		OrchURL:     "https://unknown.example.com:8935",
		CompletedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}

	rows := buildAIBatchJobRows([]AIBatchJobRecord{job}, map[string]SelectionDecision{})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].AttributionStatus != "unresolved" {
		t.Fatalf("status = %q, want unresolved", rows[0].AttributionStatus)
	}
	if rows[0].AttributionReason != "missing_candidate" {
		t.Fatalf("reason = %q, want missing_candidate", rows[0].AttributionReason)
	}
}

// ---------- buildBYOCJobRows ----------

func TestBuildBYOCJobRows_WorkerLifecycleModelTakesPrecedenceOverCIModel(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	job := BYOCJobRecord{
		EventID:     "evt-byoc-1",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc",
		CompletedAt: completedAt,
	}
	se := job.toSelectionEvent()
	decisionsByID := map[string]SelectionDecision{
		se.ID: {
			SelectionEventID: se.ID,
			Status:           "hardware_less",
			Reason:           "matched_without_hardware",
			Method:           "uri_interval_exact",
			Confidence:       "high",
			CanonicalModel:   "ci-model-should-not-win", // CI fallback
		},
	}
	workerModels := map[string]workerModel{
		"evt-byoc-1": {Model: "gpt-4o-mini", PricePerUnit: 0.01},
	}

	rows := buildBYOCJobRows([]BYOCJobRecord{job}, decisionsByID, workerModels)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].Model != "gpt-4o-mini" {
		t.Fatalf("model = %q, want gpt-4o-mini (worker_lifecycle takes precedence)", rows[0].Model)
	}
	if rows[0].PricePerUnit != 0.01 {
		t.Fatalf("price_per_unit = %v, want 0.01", rows[0].PricePerUnit)
	}
}

func TestBuildBYOCJobRows_FallsBackToCIModelWhenNoWorkerLifecycle(t *testing.T) {
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	job := BYOCJobRecord{
		EventID:     "evt-byoc-2",
		Org:         "acme",
		Capability:  "openai-chat-completions",
		OrchAddress: "0xabc",
		CompletedAt: completedAt,
	}
	se := job.toSelectionEvent()
	decisionsByID := map[string]SelectionDecision{
		se.ID: {
			SelectionEventID: se.ID,
			Status:           "hardware_less",
			Reason:           "matched_without_hardware",
			CanonicalModel:   "gpt-4o-from-ci",
		},
	}

	rows := buildBYOCJobRows([]BYOCJobRecord{job}, decisionsByID, map[string]workerModel{})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].Model != "gpt-4o-from-ci" {
		t.Fatalf("model = %q, want gpt-4o-from-ci (CI fallback)", rows[0].Model)
	}
}

// ---------- identities helpers ----------

func TestAIBatchIdentities_DeduplicatesURIs(t *testing.T) {
	jobs := []AIBatchJobRecord{
		{OrchURL: "https://orch.example.com:8935", OrchURLNorm: "https://orch.example.com:8935"},
		{OrchURL: "https://orch.example.com:8935", OrchURLNorm: "https://orch.example.com:8935"},
		{OrchURL: "https://other.example.com:8935", OrchURLNorm: "https://other.example.com:8935"},
	}
	ids := aiBatchIdentities(jobs)
	if len(ids) != 2 {
		t.Fatalf("got %d identities, want 2", len(ids))
	}
	for _, id := range ids {
		if id.Address != "" {
			t.Fatalf("AI batch identities should have no Address, got %q", id.Address)
		}
	}
}

func TestBYOCIdentities_IncludesBothAddressAndURI(t *testing.T) {
	jobs := []BYOCJobRecord{
		{OrchAddress: "0xabc", OrchURL: "https://byoc.example.com:8935", OrchURLNorm: "https://byoc.example.com:8935"},
	}
	ids := byocIdentities(jobs)
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	if ids[0].Address != "0xabc" {
		t.Fatalf("address = %q, want 0xabc", ids[0].Address)
	}
	if ids[0].URINorm != "https://byoc.example.com:8935" {
		t.Fatalf("uri norm = %q", ids[0].URINorm)
	}
}
