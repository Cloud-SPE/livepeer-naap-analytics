package resolver

import "strings"

func buildSelectionEvents(rows []selectionCandidate) []SelectionEvent {
	if len(rows) == 0 {
		return nil
	}
	events := make([]SelectionEvent, 0, len(rows))

	var currentSession string
	var currentOrg string
	var lastIdentity string
	var seq uint32

	for _, row := range rows {
		if row.SessionKey == "" {
			continue
		}
		if row.Org != currentOrg || row.SessionKey != currentSession {
			currentOrg = row.Org
			currentSession = row.SessionKey
			lastIdentity = ""
			seq = 0
		}

		identity := candidateIdentity(row.OrchAddress, row.OrchURL)
		if identity == "" {
			continue
		}

		trigger := ""
		switch {
		case seq == 0:
			trigger = "initial"
		case row.ExplicitSwap:
			trigger = "swap"
		case identity != lastIdentity:
			trigger = "orch_change_fallback"
		default:
			continue
		}

		normalizedPipeline, normalizedModel := normalizeObservedHints(row.PipelineHint, row.ModelHint)

		seq++
		selection := SelectionEvent{
			ID:                stableHash(row.Org, row.SessionKey, row.EventID),
			Org:               row.Org,
			SessionKey:        row.SessionKey,
			Seq:               seq,
			SelectionTS:       row.EventTS.UTC(),
			Trigger:           trigger,
			ObservedAddress:   strings.ToLower(strings.TrimSpace(row.OrchAddress)),
			ObservedURL:       strings.TrimSpace(row.OrchURL),
			ObservedModelHint: normalizedModel,
			ObservedPipeline:  normalizedPipeline,
			AnchorEventID:     row.EventID,
			AnchorEventType:   row.EventType,
			AnchorEventTS:     row.EventTS.UTC(),
			SourceTopic:       row.SourceTopic,
			SourcePartition:   row.SourcePart,
			SourceOffset:      row.SourceOffset,
		}
		selection.InputHash = stableHash(
			selection.Org,
			selection.SessionKey,
			selection.AnchorEventID,
			selection.Trigger,
			selection.ObservedAddress,
			selection.ObservedURL,
			selection.ObservedPipeline,
			selection.AnchorEventTS.Format(timeLayoutMillis),
		)
		events = append(events, selection)
		lastIdentity = identity
	}

	return events
}

const timeLayoutMillis = "2006-01-02T15:04:05.000Z07:00"
