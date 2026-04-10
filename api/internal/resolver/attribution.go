package resolver

import (
	"slices"
	"sort"
	"strings"
	"time"
)

type matchedInterval struct {
	row    CapabilityInterval
	method string
}

type canonicalSurface struct {
	OrchAddress string
	OrchURI     string
	Pipeline    string
	Model       string
}

type intervalBucket struct {
	ValidFromTS time.Time
	Rows        []CapabilityInterval
}

type indexedIdentityBuckets struct {
	IdentityType string
	Buckets      []intervalBucket
}

func resolveSelectionDecisions(selections []SelectionEvent, intervals []CapabilityInterval) []SelectionDecision {
	if len(selections) == 0 {
		return nil
	}
	byIdentity := indexIntervals(intervals)
	out := make([]SelectionDecision, 0, len(selections))
	for _, selection := range selections {
		out = append(out, resolveSelectionDecision(selection, byIdentity))
	}
	return out
}

func indexIntervals(rows []CapabilityInterval) map[string][]indexedIdentityBuckets {
	grouped := make(map[string]map[string][]CapabilityInterval)
	for _, row := range rows {
		if id := normalizeAddress(row.OrchAddress); id != "" {
			key := row.Org + "\x00" + id
			if grouped[key] == nil {
				grouped[key] = make(map[string][]CapabilityInterval)
			}
			grouped[key]["address"] = append(grouped[key]["address"], row)
		}
		if id := normalizeURL(row.OrchURINorm); id != "" {
			key := row.Org + "\x00" + id
			if grouped[key] == nil {
				grouped[key] = make(map[string][]CapabilityInterval)
			}
			grouped[key]["uri"] = append(grouped[key]["uri"], row)
		}
		if id := normalizeAddress(row.AliasAddress); id != "" {
			key := row.Org + "\x00" + id
			if grouped[key] == nil {
				grouped[key] = make(map[string][]CapabilityInterval)
			}
			grouped[key]["alias"] = append(grouped[key]["alias"], row)
		}
	}
	out := make(map[string][]indexedIdentityBuckets, len(grouped))
	for key, byType := range grouped {
		entries := make([]indexedIdentityBuckets, 0, len(byType))
		for identityType, list := range byType {
			slices.SortFunc(list, func(a, b CapabilityInterval) int {
				switch {
				case a.ValidFromTS.Before(b.ValidFromTS):
					return -1
				case a.ValidFromTS.After(b.ValidFromTS):
					return 1
				case a.IntervalHash < b.IntervalHash:
					return -1
				case a.IntervalHash > b.IntervalHash:
					return 1
				default:
					return 0
				}
			})
			buckets := make([]intervalBucket, 0, len(list))
			for _, row := range list {
				last := len(buckets) - 1
				if last >= 0 && buckets[last].ValidFromTS.Equal(row.ValidFromTS) {
					buckets[last].Rows = append(buckets[last].Rows, row)
					continue
				}
				buckets = append(buckets, intervalBucket{
					ValidFromTS: row.ValidFromTS,
					Rows:        []CapabilityInterval{row},
				})
			}
			entries = append(entries, indexedIdentityBuckets{
				IdentityType: identityType,
				Buckets:      buckets,
			})
		}
		slices.SortFunc(entries, func(a, b indexedIdentityBuckets) int {
			switch {
			case matchIdentityPriority(a.IdentityType+"_interval_exact") < matchIdentityPriority(b.IdentityType+"_interval_exact"):
				return -1
			case matchIdentityPriority(a.IdentityType+"_interval_exact") > matchIdentityPriority(b.IdentityType+"_interval_exact"):
				return 1
			default:
				return 0
			}
		})
		out[key] = entries
	}
	return out
}

func resolveSelectionDecision(selection SelectionEvent, indexed map[string][]indexedIdentityBuckets) SelectionDecision {
	decision := SelectionDecision{
		SelectionEventID: selection.ID,
		Org:              selection.Org,
		SessionKey:       selection.SessionKey,
		SelectionTS:      selection.SelectionTS.UTC(),
		Status:           "unresolved",
		Reason:           "missing_candidate",
		Method:           "missing",
		Confidence:       "low",
		InputHash:        selection.InputHash,
	}

	if id := normalizeURL(selection.ObservedURL); id != "" {
		candidates := collectCandidates(selection, indexed, selection.Org+"\x00"+id)
		if len(candidates) > 0 {
			return resolveMatchedCandidates(selection, candidates, decision)
		}
		decision.Reason = classifyMissingURICandidate(selection, indexed)
		return decision
	}

	keys := make([]string, 0, 2)
	if id := normalizeAddress(selection.ObservedAddress); id != "" {
		keys = append(keys, selection.Org+"\x00"+id)
	}

	candidates := make([]matchedInterval, 0)
	for _, key := range keys {
		candidates = append(candidates, collectCandidates(selection, indexed, key)...)
	}
	return resolveMatchedCandidates(selection, candidates, decision)
}

func classifyMissingURICandidate(selection SelectionEvent, indexed map[string][]indexedIdentityBuckets) string {
	id := normalizeAddress(selection.ObservedAddress)
	if id == "" {
		return "missing_candidate"
	}
	candidates := collectCandidates(selection, indexed, selection.Org+"\x00"+id)
	if len(candidates) == 0 {
		return "missing_candidate"
	}
	hasAddress := false
	hasAlias := false
	for _, candidate := range candidates {
		switch {
		case strings.HasPrefix(candidate.method, "address_"):
			hasAddress = true
		case strings.HasPrefix(candidate.method, "alias_"):
			hasAlias = true
		}
	}
	switch {
	case hasAlias:
		return "missing_uri_snapshot_local_alias_present"
	case hasAddress:
		return "missing_uri_snapshot_address_match_present"
	default:
		return "missing_candidate"
	}
}

func collectCandidates(selection SelectionEvent, indexed map[string][]indexedIdentityBuckets, key string) []matchedInterval {
	candidates := make([]matchedInterval, 0)
	for _, entry := range indexed[key] {
		candidates = append(candidates, matchBuckets(selection, entry.Buckets, entry.IdentityType)...)
	}
	return candidates
}

func resolveMatchedCandidates(selection SelectionEvent, candidates []matchedInterval, decision SelectionDecision) SelectionDecision {
	if len(candidates) == 0 {
		return decision
	}

	var exact []matchedInterval
	var prior []matchedInterval
	var future []matchedInterval
	var stale []matchedInterval
	for _, match := range candidates {
		switch {
		case strings.Contains(match.method, "exact"):
			exact = append(exact, match)
		case strings.Contains(match.method, "prior"):
			prior = append(prior, match)
		case strings.Contains(match.method, "future"):
			future = append(future, match)
		case strings.Contains(match.method, "stale"):
			stale = append(stale, match)
		}
	}

	switch {
	case len(exact) > 0:
		return materializeDecision(selection, exact, "resolved", "matched")
	case len(prior) > 0:
		return materializeDecision(selection, prior, "resolved", "matched_prior")
	case len(future) > 0:
		return materializeDecision(selection, future, "resolved", "future_skew")
	case len(stale) > 0:
		return materializeDecision(selection, stale, "stale", "stale_candidate")
	default:
		return decision
	}
}

func matchBuckets(selection SelectionEvent, buckets []intervalBucket, identityType string) []matchedInterval {
	if len(buckets) == 0 {
		return nil
	}
	selectionTS := selection.SelectionTS.UTC()
	idx := sort.Search(len(buckets), func(i int) bool {
		return !buckets[i].ValidFromTS.Before(selectionTS)
	})
	if idx == len(buckets) {
		idx = len(buckets)
	}

	matches := make([]matchedInterval, 0, 8)
	if idx < len(buckets) && buckets[idx].ValidFromTS.Equal(selectionTS) {
		return classifyBucket(buckets[idx], identityType, "exact")
	}
	if idx > 0 {
		prev := buckets[idx-1]
		age := selectionTS.Sub(prev.ValidFromTS)
		if bucketContains(prev, selectionTS) {
			if age <= 10*time.Minute {
				matches = append(matches, classifyBucket(prev, identityType, "exact")...)
			} else {
				matches = append(matches, classifyBucket(prev, identityType, "stale")...)
			}
		} else {
			if age <= 10*time.Minute {
				matches = append(matches, classifyBucket(prev, identityType, "prior")...)
			} else {
				matches = append(matches, classifyBucket(prev, identityType, "stale")...)
			}
		}
	}
	if idx < len(buckets) && buckets[idx].ValidFromTS.After(selectionTS) && buckets[idx].ValidFromTS.Sub(selectionTS) <= 30*time.Second {
		matches = append(matches, classifyBucket(buckets[idx], identityType, "future")...)
	}
	return matches
}

func classifyBucket(bucket intervalBucket, identityType, matchType string) []matchedInterval {
	out := make([]matchedInterval, 0, len(bucket.Rows))
	for _, row := range bucket.Rows {
		out = append(out, matchedInterval{
			row:    row,
			method: identityType + "_interval_" + matchType,
		})
	}
	return out
}

func bucketContains(bucket intervalBucket, ts time.Time) bool {
	if len(bucket.Rows) == 0 {
		return false
	}
	return intervalContains(bucket.Rows[0], ts)
}

func intervalContains(interval CapabilityInterval, ts time.Time) bool {
	if ts.Before(interval.ValidFromTS) {
		return false
	}
	if interval.ValidToTS == nil {
		return true
	}
	return ts.Before(interval.ValidToTS.UTC())
}

func materializeDecision(selection SelectionEvent, matches []matchedInterval, status, reason string) SelectionDecision {
	filtered := compatibleMatches(selection, matches)
	if len(filtered) == 0 {
		filtered = matches
	}
	slices.SortFunc(filtered, func(a, b matchedInterval) int {
		switch {
		case matchIdentityPriority(a.method) < matchIdentityPriority(b.method):
			return -1
		case matchIdentityPriority(a.method) > matchIdentityPriority(b.method):
			return 1
		case a.row.ValidFromTS.After(b.row.ValidFromTS):
			return -1
		case a.row.ValidFromTS.Before(b.row.ValidFromTS):
			return 1
		case a.row.IntervalHash < b.row.IntervalHash:
			return -1
		case a.row.IntervalHash > b.row.IntervalHash:
			return 1
		default:
			return 0
		}
	})
	bestPriority := matchIdentityPriority(filtered[0].method)
	precedenceFiltered := filtered[:0]
	for _, match := range filtered {
		if matchIdentityPriority(match.method) == bestPriority {
			precedenceFiltered = append(precedenceFiltered, match)
		}
	}
	filtered = precedenceFiltered

	decision := SelectionDecision{
		SelectionEventID: selection.ID,
		Org:              selection.Org,
		SessionKey:       selection.SessionKey,
		SelectionTS:      selection.SelectionTS.UTC(),
		Status:           status,
		Reason:           reason,
		Method:           filtered[0].method,
		Confidence:       "high",
		InputHash:        selection.InputHash,
	}

	if len(uniqueCanonicalSurfaces(filtered)) > 1 {
		decision.Status = "ambiguous"
		decision.Reason = "ambiguous_candidates"
		decision.Method = "ambiguous"
		decision.Confidence = "low"
		return decision
	}

	selected := filtered[0].row
	decision.CapabilityVersionID = selected.VersionID
	decision.SnapshotEventID = selected.SnapshotEventID
	decision.SnapshotTS = ptrTime(selected.SnapshotTS.UTC())
	decision.AttributedOrchAddress = selected.OrchAddress
	decision.AttributedOrchURI = selected.OrchURI
	decision.CanonicalPipeline = selected.Pipeline
	decision.CanonicalModel = selected.Model
	if !canCollapseMultiGPUMatch(filtered) {
		decision.GPUID = selected.GPUID
		decision.GPUModelName = selected.GPUModelName
		decision.GPUMemoryBytesTotal = selected.GPUMemoryTotal
	}
	if !selected.HardwarePresent {
		decision.Status = "hardware_less"
		decision.Reason = "matched_without_hardware"
	}
	if status == "stale" {
		decision.Confidence = "medium"
	}
	return decision
}

func matchIdentityPriority(method string) int {
	switch {
	case strings.HasPrefix(method, "uri_"):
		return 0
	case strings.HasPrefix(method, "address_"):
		return 1
	case strings.HasPrefix(method, "alias_"):
		return 2
	default:
		return 3
	}
}

func compatibleMatches(selection SelectionEvent, matches []matchedInterval) []matchedInterval {
	out := make([]matchedInterval, 0, len(matches))
	for _, match := range matches {
		if isCompatible(selection, match.row) {
			out = append(out, match)
		}
	}
	return out
}

func isCompatible(selection SelectionEvent, interval CapabilityInterval) bool {
	// BYOC path: bypass the canonical pipeline allow-list and match verbatim.
	// The capability string from the job (e.g. "openai-chat-completions") must
	// equal the interval pipeline exactly (case-insensitive).
	if selection.PipelineHintVerbatim {
		if selection.ObservedPipeline == "" || interval.Pipeline == "" {
			return false
		}
		return strings.EqualFold(selection.ObservedPipeline, interval.Pipeline)
	}
	// Standard path: use the canonical pipeline allow-list.
	if pipelineHint := compatiblePipelineHint(selection.ObservedPipeline); pipelineHint != "" {
		if interval.Pipeline == "" || !strings.EqualFold(pipelineHint, interval.Pipeline) {
			return false
		}
	}
	if selection.ObservedModelHint != "" {
		if interval.Model == "" || !strings.EqualFold(selection.ObservedModelHint, interval.Model) {
			return false
		}
	}
	return true
}

func compatiblePipelineHint(hint string) string {
	switch strings.ToLower(strings.TrimSpace(hint)) {
	case "live-video-to-video",
		"text-to-image", "image-to-image", "image-to-video", "text-to-video",
		"audio-to-text", "text-to-speech",
		"upscale", "llm", "image-to-text", "segment-anything-2",
		"noop":
		return strings.ToLower(strings.TrimSpace(hint))
	default:
		return ""
	}
}

func uniqueIntervals(matches []matchedInterval) map[string]struct{} {
	out := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		out[match.row.IntervalHash] = struct{}{}
	}
	return out
}

func uniqueCanonicalSurfaces(matches []matchedInterval) map[canonicalSurface]struct{} {
	out := make(map[canonicalSurface]struct{}, len(matches))
	for _, match := range matches {
		out[canonicalSurface{
			OrchAddress: match.row.OrchAddress,
			OrchURI:     match.row.OrchURINorm,
			Pipeline:    strings.ToLower(strings.TrimSpace(match.row.Pipeline)),
			Model:       strings.ToLower(strings.TrimSpace(match.row.Model)),
		}] = struct{}{}
	}
	return out
}

func canCollapseMultiGPUMatch(matches []matchedInterval) bool {
	if len(matches) == 0 || len(uniqueIntervals(matches)) <= 1 || len(uniqueCanonicalSurfaces(matches)) != 1 {
		return false
	}
	gpus := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if !match.row.HardwarePresent {
			return false
		}
		if strings.TrimSpace(match.row.Pipeline) == "" || strings.TrimSpace(match.row.Model) == "" {
			return false
		}
		if gpu := strings.TrimSpace(match.row.GPUID); gpu != "" {
			gpus[gpu] = struct{}{}
		}
	}
	return len(gpus) > 1
}
