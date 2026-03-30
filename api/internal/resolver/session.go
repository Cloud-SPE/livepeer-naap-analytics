package resolver

import (
	"sort"
	"strings"
	"time"
)

var excusableStartupErrorSubstrings = []string{
	"no orchestrators available",
	"mediamtx ingest disconnected",
	"whip disconnected",
	"missing video",
	"ice connection state failed",
	"user disconnected",
}

func buildSessionCurrentRows(evidence map[string]SessionEvidence, decisions map[string]SelectionDecision) []SessionCurrentRow {
	keys := make([]string, 0, len(evidence))
	for key := range evidence {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]SessionCurrentRow, 0, len(keys))
	for _, key := range keys {
		ev := evidence[key]
		decision := decisions[key]
		lastSeen := maxTime(ev.TraceLastSeen, ev.StatusLastSeen, ev.EventLastSeen)
		if lastSeen.IsZero() {
			continue
		}

		fallbackHint := firstNonEmpty(ev.StatusPipelineHint, ev.TracePipelineHint, ev.EventPipelineHint)
		canonicalPipeline, canonicalModel := normalizeObservedHints(firstNonEmpty(decision.CanonicalPipeline, fallbackHint), decision.CanonicalModel)

		attributionReason := firstNonEmpty(decision.Reason, "missing_candidate")
		selectionOutcome := "unknown"
		if decision.SelectionEventID != "" {
			selectionOutcome = "selected"
		} else if ev.NoOrchCount > 0 {
			selectionOutcome = "no_orch"
		}
		if decision.Reason == "" && selectionOutcome == "no_orch" {
			attributionReason = "no_selection_no_orch_excused"
		}

		row := SessionCurrentRow{
			SessionKey:                key,
			Org:                       ev.Org,
			StreamID:                  firstNonEmpty(ev.StreamID, ""),
			RequestID:                 firstNonEmpty(ev.RequestID, ""),
			Gateway:                   firstNonEmpty(ev.Gateway, ""),
			CanonicalPipeline:         canonicalPipeline,
			CanonicalModel:            canonicalModel,
			GPUID:                     decision.GPUID,
			StartedAt:                 ev.StartedAt,
			LastSeen:                  lastSeen.UTC(),
			StartupLatencyMS:          deriveLatencyMS(ev.StartedAt, ev.FirstProcessedAt),
			E2ELatencyMS:              deriveLatencyMS(ev.FirstIngestAt, ev.RunnerFirstProcessedAt),
			PromptToPlayableLatencyMS: deriveLatencyMS(ev.StatusStartTime, ev.FewProcessedAt),
			RequestedSeen:             boolToUInt8(ev.StartedCount > 0),
			PlayableSeen:              boolToUInt8(ev.PlayableSeenCount > 0),
			SelectionOutcome:          selectionOutcome,
			Completed:                 boolToUInt8(ev.CompletedCount > 0),
			SwapCount:                 ev.SwapCount,
			RestartSeen:               boolToUInt8(ev.RestartSeenCount > 0),
			ErrorSeen:                 boolToUInt8(ev.ErrorSeenCount > 0),
			DegradedInputSeen:         boolToUInt8(ev.DegradedInputSeenCount > 0),
			DegradedInferenceSeen:     boolToUInt8(ev.DegradedInferenceSeenCount > 0),
			StatusSampleCount:         ev.StatusSampleCount,
			StatusErrorSampleCount:    ev.StatusErrorSampleCount,
			StartupErrorCount:         ev.StartupErrorCount,
			ExcusableErrorCount:       ev.ExcusableErrorCount,
			LoadingOnlySession:        boolToUInt8(ev.StatusSampleCount > 0 && ev.RunningStateSamplesCount == 0 && ev.OnlineSeenCount == 0),
			ZeroOutputFPSSession:      boolToUInt8(ev.StatusSampleCount > 0 && ev.PositiveOutputSeenCount == 0),
			StartupOutcome:            "unknown",
			ExcusalReason:             "none",
			AttributionReason:         attributionReason,
			AttributionStatus:         firstNonEmpty(decision.Status, "unresolved"),
			AttributedOrchAddress:     decision.AttributedOrchAddress,
			AttributedOrchURI:         decision.AttributedOrchURI,
			AttributionSnapshotTS:     decision.SnapshotTS,
			HasAmbiguousIdentity:      boolToUInt8(decision.Status == "ambiguous"),
			HasSnapshotMatch:          boolToUInt8(decision.CapabilityVersionID != ""),
			IsHardwareLess:            boolToUInt8(decision.Status == "hardware_less"),
			IsStale:                   boolToUInt8(decision.Status == "stale"),
		}
		if decision.SelectionEventID != "" {
			row.CurrentSelectionEventID = decision.SelectionEventID
			row.CurrentSelectionTS = ptrTime(decision.SelectionTS.UTC())
		}

		row.HealthSignalCount = boolToU64(ev.StatusSampleCount > 0) +
			boolToU64(row.PlayableSeen == 1) +
			boolToU64(ev.StatusSampleCount > 0 && (row.LoadingOnlySession == 0 || row.ZeroOutputFPSSession == 0))
		if row.RequestedSeen == 1 {
			row.HealthExpectedSignalCount = 3
			row.HealthSignalCoverageRatio = float64(row.HealthSignalCount) / 3.0
		}
		switch {
		case row.RequestedSeen == 0:
			row.StartupOutcome = "unknown"
		case row.PlayableSeen == 1:
			row.StartupOutcome = "success"
		default:
			row.StartupOutcome = "failed"
			switch {
			case row.SelectionOutcome == "no_orch":
				row.ExcusalReason = "no_orch"
			case row.StartupErrorCount > 0 && row.StartupErrorCount == row.ExcusableErrorCount:
				row.ExcusalReason = "excusable_error"
			}
		}

		out = append(out, row)
	}
	return out
}

func buildStatusHourRows(evidence []statusHourEvidence, sessions map[string]SessionCurrentRow) []StatusHourRow {
	if len(evidence) == 0 {
		return nil
	}
	sort.Slice(evidence, func(i, j int) bool {
		switch {
		case evidence[i].Org != evidence[j].Org:
			return evidence[i].Org < evidence[j].Org
		case evidence[i].SessionKey != evidence[j].SessionKey:
			return evidence[i].SessionKey < evidence[j].SessionKey
		default:
			return evidence[i].Hour.Before(evidence[j].Hour)
		}
	})

	out := make([]StatusHourRow, 0, len(evidence))
	prevBySession := make(map[string]StatusHourRow)
	for _, ev := range evidence {
		session, ok := sessions[ev.SessionKey]
		if !ok {
			continue
		}
		row := StatusHourRow{
			SessionKey:                ev.SessionKey,
			Org:                       ev.Org,
			Hour:                      ev.Hour.UTC(),
			StreamID:                  firstNonEmpty(ev.StreamID, session.StreamID),
			RequestID:                 firstNonEmpty(ev.RequestID, session.RequestID),
			CanonicalPipeline:         session.CanonicalPipeline,
			CanonicalModel:            session.CanonicalModel,
			OrchAddress:               session.AttributedOrchAddress,
			AttributionStatus:         session.AttributionStatus,
			AttributionReason:         session.AttributionReason,
			StartedAt:                 session.StartedAt,
			SessionLastSeen:           session.LastSeen.UTC(),
			StartupLatencyMS:          session.StartupLatencyMS,
			E2ELatencyMS:              session.E2ELatencyMS,
			PromptToPlayableLatencyMS: session.PromptToPlayableLatencyMS,
			StatusSamples:             ev.StatusSamples,
			FPSPositiveSamples:        ev.FPSPositiveSamples,
			RunningStateSamples:       ev.RunningStateSamples,
			DegradedInputSamples:      ev.DegradedInputSamples,
			DegradedInferenceSamples:  ev.DegradedInferenceSamples,
			ErrorSamples:              ev.ErrorSamples,
			AvgOutputFPS:              average(ev.OutputFPSSum, ev.StatusSamples),
			AvgInputFPS:               average(ev.InputFPSSum, ev.StatusSamples),
		}

		prev := prevBySession[ev.SessionKey]
		if row.StartedAt != nil &&
			row.StartedAt.Before(row.Hour) &&
			!row.SessionLastSeen.Before(row.Hour) &&
			row.SessionLastSeen.Before(row.Hour.Add(time.Hour)) &&
			row.FPSPositiveSamples == 0 &&
			row.RunningStateSamples == 0 &&
			row.StatusSamples < 3 &&
			prev.FPSPositiveSamples == 0 &&
			prev.RunningStateSamples == 0 &&
			prev.StatusSamples < 3 {
			row.IsTerminalTailArtifact = 1
		}

		out = append(out, row)
		prevBySession[ev.SessionKey] = row
	}
	return out
}

func indexSessionRows(rows []SessionCurrentRow) map[string]SessionCurrentRow {
	out := make(map[string]SessionCurrentRow, len(rows))
	for _, row := range rows {
		out[row.SessionKey] = row
	}
	return out
}

func filterOwnedStatusHourRows(rows []StatusHourRow, spec WindowSpec) []StatusHourRow {
	if len(rows) == 0 {
		return nil
	}
	var ownedStart time.Time
	if spec.Start != nil {
		ownedStart = spec.Start.UTC().Truncate(time.Hour)
	}
	var ownedEnd time.Time
	if spec.End != nil {
		ownedEnd = spec.End.UTC().Truncate(time.Hour)
		if !spec.End.UTC().Equal(ownedEnd) {
			ownedEnd = ownedEnd.Add(time.Hour)
		}
	}
	out := make([]StatusHourRow, 0, len(rows))
	for _, row := range rows {
		if spec.Start != nil && row.Hour.Before(ownedStart) {
			continue
		}
		if spec.End != nil && !row.Hour.Before(ownedEnd) {
			continue
		}
		out = append(out, row)
	}
	return out
}

func maxTime(values ...*time.Time) time.Time {
	var out time.Time
	for _, v := range values {
		if v != nil && v.After(out) {
			out = v.UTC()
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func isExcusableStartupError(message string) bool {
	lower := strings.ToLower(strings.TrimSpace(message))
	if lower == "" {
		return false
	}
	for _, fragment := range excusableStartupErrorSubstrings {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func average(sum float64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func deriveLatencyMS(start, end *time.Time) *float64 {
	if start == nil || end == nil || start.IsZero() || end.IsZero() {
		return nil
	}
	delta := end.UTC().Sub(start.UTC())
	if delta < 0 {
		return nil
	}
	ms := float64(delta) / float64(time.Millisecond)
	return &ms
}

func boolToU64(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

func collectWindowSlices(sessionRows []SessionCurrentRow, statusRows []StatusHourRow) []windowSliceRef {
	seen := make(map[string]windowSliceRef)
	for _, row := range sessionRows {
		lastSeenHour := row.LastSeen.UTC().Truncate(time.Hour)
		seen[stableHash(row.Org, lastSeenHour.Format(time.RFC3339))] = windowSliceRef{
			Org:         row.Org,
			WindowStart: lastSeenHour,
		}
		if row.StartedAt != nil {
			startedHour := row.StartedAt.UTC().Truncate(time.Hour)
			seen[stableHash(row.Org, startedHour.Format(time.RFC3339))] = windowSliceRef{
				Org:         row.Org,
				WindowStart: startedHour,
			}
		}
	}
	for _, row := range statusRows {
		hour := row.Hour.UTC()
		seen[stableHash(row.Org, hour.Format(time.RFC3339))] = windowSliceRef{
			Org:         row.Org,
			WindowStart: hour,
		}
	}

	out := make([]windowSliceRef, 0, len(seen))
	for _, slice := range seen {
		out = append(out, slice)
	}
	sort.Slice(out, func(i, j int) bool {
		switch {
		case out[i].Org != out[j].Org:
			return out[i].Org < out[j].Org
		default:
			return out[i].WindowStart.Before(out[j].WindowStart)
		}
	})
	return out
}
