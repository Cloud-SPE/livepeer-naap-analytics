package resolver

import (
	"sort"
	"strings"
	"time"
)

// aiBatchIdentities returns the set of normalized orchestrator URIs observed
// across a batch of AI batch job records. AI batch events only carry orch_url —
// there is no address field — so identity is URL-only.
func aiBatchIdentities(jobs []AIBatchJobRecord) []orchIdentity {
	seen := make(map[string]struct{}, len(jobs))
	out := make([]orchIdentity, 0, len(jobs))
	for _, j := range jobs {
		norm := normalizeURL(j.OrchURLNorm)
		if norm == "" {
			norm = normalizeURL(j.OrchURL)
		}
		if norm == "" {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		out = append(out, orchIdentity{URINorm: norm})
	}
	return out
}

// byocIdentities returns the set of orchestrator addresses and URIs observed
// across a batch of BYOC job records. BYOC events carry both orch_address and
// orch_url, so both identity dimensions are returned.
func byocIdentities(jobs []BYOCJobRecord) []orchIdentity {
	seen := make(map[string]struct{}, len(jobs))
	out := make([]orchIdentity, 0, len(jobs)*2)
	for _, j := range jobs {
		addr := normalizeAddress(j.OrchAddress)
		norm := normalizeURL(j.OrchURLNorm)
		if norm == "" {
			norm = normalizeURL(j.OrchURL)
		}
		key := addr + "\x00" + norm
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, orchIdentity{Address: addr, URINorm: norm})
	}
	return out
}

// orchIdentity carries the addressing dimensions used to look up capability
// snapshots for a given orchestrator.
type orchIdentity struct {
	Address string
	URINorm string
}

// toSelectionEvent converts an AIBatchJobRecord into a SelectionEvent suitable
// for the standard attribution resolver.
//
// SelectionTS uses ReceivedAt when available — that is the moment the gateway
// chose the orchestrator. OrchURL always comes from the completed_at event
// because received events do not carry orch identity.
//
// PipelineHintVerbatim is false: AI batch pipelines go through the canonical
// allow-list in compatiblePipelineHint.
func (j AIBatchJobRecord) toSelectionEvent() SelectionEvent {
	selectionTS := j.CompletedAt
	if j.ReceivedAt != nil {
		selectionTS = *j.ReceivedAt
	}
	normalizedPipeline, normalizedModel := normalizeObservedHints(j.Pipeline, j.ModelID)
	se := SelectionEvent{
		ID:                   stableHash(j.Org, j.RequestID, "ai-batch"),
		Org:                  j.Org,
		SessionKey:           j.RequestID,
		Seq:                  1,
		SelectionTS:          selectionTS.UTC(),
		Trigger:              "ai_batch_completion",
		ObservedURL:          strings.TrimSpace(j.OrchURL),
		ObservedAddress:      "",
		ObservedPipeline:     normalizedPipeline,
		ObservedModelHint:    normalizedModel,
		PipelineHintVerbatim: false,
		AnchorEventTS:        j.CompletedAt.UTC(),
	}
	se.InputHash = stableHash(se.Org, se.SessionKey, se.ObservedURL, se.ObservedPipeline, se.ObservedModelHint, selectionTS.UTC().Format(time.RFC3339Nano))
	return se
}

// toSelectionEvent converts a BYOCJobRecord into a SelectionEvent suitable
// for the standard attribution resolver.
//
// PipelineHintVerbatim is true: BYOC capability strings (e.g.
// "openai-chat-completions") bypass the canonical pipeline allow-list and are
// matched verbatim against the capability interval's Pipeline field.
//
// ObservedPipeline carries the raw capability string from the job. BYOC model
// evidence comes from worker_lifecycle snapshots and is injected through
// toSelectionEventWithModelHint so compatibility selection can disambiguate
// same-capability candidates before the final BYOC row is materialized.
func (j BYOCJobRecord) toSelectionEvent() SelectionEvent {
	return j.toSelectionEventWithModelHint("")
}

// toSelectionEventWithModelHint converts a BYOC job into a SelectionEvent and
// optionally carries a worker-lifecycle model hint into compatibility
// selection. When modelHint is empty, BYOC matching falls back to capability +
// identity only.
func (j BYOCJobRecord) toSelectionEventWithModelHint(modelHint string) SelectionEvent {
	se := SelectionEvent{
		ID:                   stableHash(j.Org, j.EventID, "byoc"),
		Org:                  j.Org,
		SessionKey:           j.EventID,
		Seq:                  1,
		SelectionTS:          j.CompletedAt.UTC(),
		Trigger:              "byoc_completion",
		ObservedURL:          strings.TrimSpace(j.OrchURL),
		ObservedAddress:      j.OrchAddress,
		ObservedPipeline:     j.Capability,
		ObservedModelHint:    strings.TrimSpace(modelHint),
		PipelineHintVerbatim: true,
		AnchorEventTS:        j.CompletedAt.UTC(),
	}
	se.InputHash = stableHash(
		se.Org,
		se.SessionKey,
		se.ObservedURL,
		se.ObservedAddress,
		se.ObservedPipeline,
		se.ObservedModelHint,
		j.CompletedAt.UTC().Format(time.RFC3339Nano),
	)
	return se
}

// workerModel is the resolved model and pricing for a BYOC job, sourced from
// the most recent worker_lifecycle snapshot at or before the job's CompletedAt.
type workerModel struct {
	Model        string
	PricePerUnit float64
}

// resolveWorkerModels returns a map of BYOC event_id to the resolved
// workerModel for that job. Matching prefers the exact worker identity
// `(org, capability, orch_address, worker_url)` and falls back to
// `(org, capability, orch_address)` only when the job or snapshot lacks a
// worker URL. The most recent snapshot where EventTS <= job.CompletedAt wins.
func resolveWorkerModels(jobs []BYOCJobRecord, snapshots []workerLifecycleSnapshot) map[string]workerModel {
	type workerKey struct {
		org, capability, orchAddress, workerURL string
	}
	type fallbackKey struct{ org, capability, orchAddress string }

	// Index snapshots by exact worker identity and by address-only fallback, both
	// sorted newest-first so the first non-future row is the winner.
	exactIndex := make(map[workerKey][]workerLifecycleSnapshot, len(snapshots))
	fallbackIndex := make(map[fallbackKey][]workerLifecycleSnapshot, len(snapshots))
	for _, s := range snapshots {
		exactIndex[workerKey{s.Org, s.Capability, s.OrchAddress, normalizeURL(s.WorkerURL)}] = append(
			exactIndex[workerKey{s.Org, s.Capability, s.OrchAddress, normalizeURL(s.WorkerURL)}],
			s,
		)
		fallbackIndex[fallbackKey{s.Org, s.Capability, s.OrchAddress}] = append(
			fallbackIndex[fallbackKey{s.Org, s.Capability, s.OrchAddress}],
			s,
		)
	}
	sortSnapshots := func(list []workerLifecycleSnapshot) []workerLifecycleSnapshot {
		sort.Slice(list, func(i, j int) bool {
			return list[i].EventTS.After(list[j].EventTS)
		})
		return list
	}
	for k := range exactIndex {
		exactIndex[k] = sortSnapshots(exactIndex[k])
	}
	for k := range fallbackIndex {
		fallbackIndex[k] = sortSnapshots(fallbackIndex[k])
	}

	out := make(map[string]workerModel, len(jobs))
	for _, j := range jobs {
		candidates := []workerLifecycleSnapshot(nil)
		if workerURL := normalizeURL(j.WorkerURL); workerURL != "" {
			candidates = exactIndex[workerKey{j.Org, j.Capability, j.OrchAddress, workerURL}]
		}
		if len(candidates) == 0 {
			candidates = fallbackIndex[fallbackKey{j.Org, j.Capability, j.OrchAddress}]
		}
		for _, s := range candidates {
			if s.WorkerURL != "" && j.WorkerURL != "" && !strings.EqualFold(normalizeURL(s.WorkerURL), normalizeURL(j.WorkerURL)) {
				continue
			}
			if s.EventTS.After(j.CompletedAt) {
				continue
			}
			out[j.EventID] = workerModel{
				Model:        s.Model,
				PricePerUnit: s.PricePerUnit,
			}
			break
		}
	}
	return out
}

// buildAIBatchJobRows converts a slice of AIBatchJobRecords into AIBatchJobRows
// by joining each record against the corresponding SelectionDecision.
// decisionsByID is keyed by the SelectionEvent ID (stableHash(org, requestID, "ai-batch")).
func buildAIBatchJobRows(jobs []AIBatchJobRecord, decisionsByID map[string]SelectionDecision) []AIBatchJobRow {
	out := make([]AIBatchJobRow, 0, len(jobs))
	for _, j := range jobs {
		se := j.toSelectionEvent()
		row := AIBatchJobRow{
			AIBatchJobRecord:      j,
			AttributionStatus:     "unresolved",
			AttributionReason:     "missing_candidate",
			AttributionMethod:     "missing",
			AttributionConfidence: "low",
		}
		if d, ok := decisionsByID[se.ID]; ok {
			row.AttributionStatus = d.Status
			row.AttributionReason = d.Reason
			row.AttributionMethod = d.Method
			row.AttributionConfidence = d.Confidence
			row.AttributedOrchURI = d.AttributedOrchURI
			row.CapabilityVersionID = d.CapabilityVersionID
			row.AttributionSnapshotTS = d.SnapshotTS
			row.GPUID = d.GPUID
			row.GPUModelName = d.GPUModelName
			row.GPUMemoryBytesTotal = d.GPUMemoryBytesTotal
			row.AttributedModel = d.CanonicalModel
		}
		out = append(out, row)
	}
	return out
}

// buildBYOCJobRows converts a slice of BYOCJobRecords into BYOCJobRows by
// joining each record against the corresponding SelectionDecision and the
// resolved worker model.
// decisionsByID is keyed by the SelectionEvent ID (stableHash(org, eventID, "byoc")).
// workerModels is keyed by event_id.
func buildBYOCJobRows(jobs []BYOCJobRecord, decisionsByID map[string]SelectionDecision, workerModels map[string]workerModel) []BYOCJobRow {
	out := make([]BYOCJobRow, 0, len(jobs))
	for _, j := range jobs {
		se := j.toSelectionEvent()
		row := BYOCJobRow{
			BYOCJobRecord:         j,
			AttributionStatus:     "unresolved",
			AttributionReason:     "missing_candidate",
			AttributionMethod:     "missing",
			AttributionConfidence: "low",
		}
		// Worker lifecycle provides model and pricing; takes precedence over
		// capability interval canonical_model.
		if wm, ok := workerModels[j.EventID]; ok {
			row.Model = wm.Model
			row.PricePerUnit = wm.PricePerUnit
		}
		if d, ok := decisionsByID[se.ID]; ok {
			row.AttributionStatus = d.Status
			row.AttributionReason = d.Reason
			row.AttributionMethod = d.Method
			row.AttributionConfidence = d.Confidence
			row.AttributedOrchURI = d.AttributedOrchURI
			row.CapabilityVersionID = d.CapabilityVersionID
			row.AttributionSnapshotTS = d.SnapshotTS
			row.GPUID = d.GPUID
			row.GPUModelName = d.GPUModelName
			row.GPUMemoryBytesTotal = d.GPUMemoryBytesTotal
			// Use CI canonical_model as fallback only when worker_lifecycle had no model.
			if row.Model == "" && d.CanonicalModel != "" {
				row.Model = d.CanonicalModel
			}
		}
		out = append(out, row)
	}
	return out
}
