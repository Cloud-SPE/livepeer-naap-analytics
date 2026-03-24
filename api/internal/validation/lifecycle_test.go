//go:build validation

package validation

// ── RULE-LIFECYCLE-002 ────────────────────────────────────────────────────────
// Trace Subtypes Must Map To Fixed Lifecycle Semantics
// Each contracted trace data.type must drive the correct counter in
// agg_stream_hourly: started, completed, no_orch, orch_swap.
//
// ── RULE-LIFECYCLE-003 ────────────────────────────────────────────────────────
// Startup Outcome Classification Must Use Fixed Precedence
// Tests three canonical scenarios: clean success, excused no-orch failure,
// and unexcused failure (known stream, no playable evidence, no no_orch).
//
// ── RULE-LIFECYCLE-007 ────────────────────────────────────────────────────────
// Session Health Signals Must Be Explicit Additive Facts
// Streams that never received an ai_stream_status are "dark" — they are in the
// denominator (known stream) but have zero health signal coverage.

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ── RULE-LIFECYCLE-002 ───────────────────────────────────────────────────────

// TestRuleLifecycle002_ContractedSubtypesDriveCorrectCounters inserts each
// contracted trace subtype and asserts it contributes to the right
// agg_stream_hourly column.
func TestRuleLifecycle002_ContractedSubtypesDriveCorrectCounters(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		// Drives: started += 1
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
		// Drives: completed += 1
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_ingest_stream_closed"}`, uid("s")), IngestedAt: ts},
		// Drives: no_orch += 1
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_no_orchestrators_available"}`, uid("s")), IngestedAt: ts},
		// Drives: orch_swap += 1
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"orchestrator_swap"}`, uid("s")), IngestedAt: ts},
		// Drives: no counter (informational only, not in agg_stream_hourly MV)
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_send_first_ingest_segment"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_first_processed_segment"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_few_processed_segments"}`, uid("s")), IngestedAt: ts},
	})

	type counters struct {
		started, completed, noOrch, orchSwap uint64
	}

	var got counters
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT sum(started), sum(completed), sum(no_orch), sum(orch_swap)
		 FROM naap.agg_stream_hourly FINAL
		 WHERE org = '%s'`, h.org))
	if err := row.Scan(&got.started, &got.completed, &got.noOrch, &got.orchSwap); err != nil {
		t.Fatalf("RULE-LIFECYCLE-002: scan: %v", err)
	}

	if got.started != 1 {
		t.Errorf("RULE-LIFECYCLE-002: started = %d, want 1", got.started)
	}
	if got.completed != 1 {
		t.Errorf("RULE-LIFECYCLE-002: completed = %d, want 1", got.completed)
	}
	if got.noOrch != 1 {
		t.Errorf("RULE-LIFECYCLE-002: no_orch = %d, want 1", got.noOrch)
	}
	if got.orchSwap != 1 {
		t.Errorf("RULE-LIFECYCLE-002: orch_swap = %d, want 1", got.orchSwap)
	}
}

// ── RULE-LIFECYCLE-003 ───────────────────────────────────────────────────────

// TestRuleLifecycle003_CleanSuccessStream exercises the happy path:
// a stream that reaches gateway_receive_few_processed_segments (startup SUCCESS)
// and is closed cleanly. agg_stream_state must reflect is_closed=1, has_failure=0.
func TestRuleLifecycle003_CleanSuccessStream(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("success")

	milestones := []string{
		"gateway_receive_stream_request",
		"gateway_send_first_ingest_segment",
		"gateway_receive_first_processed_segment",
		"gateway_receive_few_processed_segments", // startup SUCCESS signal
		"gateway_ingest_stream_closed",
	}
	var events []rawEvent
	for i, m := range milestones {
		events = append(events, rawEvent{
			EventID: uid("e"), EventType: "stream_trace",
			EventTs: ts.Add(time.Duration(i) * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":%q}`, streamID, m),
			IngestedAt: ts,
		})
	}
	h.insert(t, events)

	var isClosed, hasFailure uint8
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT is_closed, has_failure FROM naap.agg_stream_state FINAL
		 WHERE stream_id = '%s'`, streamID))
	if err := row.Scan(&isClosed, &hasFailure); err != nil {
		t.Fatalf("RULE-LIFECYCLE-003 clean success: scan: %v", err)
	}

	if isClosed != 1 {
		t.Errorf("RULE-LIFECYCLE-003: clean success — is_closed = %d, want 1", isClosed)
	}
	if hasFailure != 0 {
		t.Errorf("RULE-LIFECYCLE-003: clean success — has_failure = %d, want 0", hasFailure)
	}

	// Also verify in agg_stream_hourly: started=1, completed=1.
	started := h.queryInt(t, `SELECT sum(started) FROM naap.agg_stream_hourly FINAL WHERE org = ?`, h.org)
	completed := h.queryInt(t, `SELECT sum(completed) FROM naap.agg_stream_hourly FINAL WHERE org = ?`, h.org)
	if started != 1 {
		t.Errorf("RULE-LIFECYCLE-003: clean success — hourly started = %d, want 1", started)
	}
	if completed != 1 {
		t.Errorf("RULE-LIFECYCLE-003: clean success — hourly completed = %d, want 1", completed)
	}
}

// TestRuleLifecycle003_NoOrchStartupFailure exercises the excused failure path:
// a stream that received a gateway_no_orchestrators_available signal.
// agg_stream_state must reflect has_failure=1, is_closed=0.
func TestRuleLifecycle003_NoOrchStartupFailure(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("noorch")

	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, streamID), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(time.Second), Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_no_orchestrators_available"}`, streamID), IngestedAt: ts},
	})

	var isClosed, hasFailure uint8
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT is_closed, has_failure FROM naap.agg_stream_state FINAL
		 WHERE stream_id = '%s'`, streamID))
	if err := row.Scan(&isClosed, &hasFailure); err != nil {
		t.Fatalf("RULE-LIFECYCLE-003 no-orch: scan: %v", err)
	}

	if hasFailure != 1 {
		t.Errorf("RULE-LIFECYCLE-003: no-orch — has_failure = %d, want 1", hasFailure)
	}
	if isClosed != 0 {
		t.Errorf("RULE-LIFECYCLE-003: no-orch — is_closed = %d, want 0 (no explicit close)", isClosed)
	}

	// Verify hourly counters.
	noOrch := h.queryInt(t, `SELECT sum(no_orch) FROM naap.agg_stream_hourly FINAL WHERE org = ?`, h.org)
	if noOrch != 1 {
		t.Errorf("RULE-LIFECYCLE-003: no-orch — hourly no_orch = %d, want 1", noOrch)
	}
}

// TestRuleLifecycle003_UnexcusedFailureIsDetectable verifies that a known
// stream with no playable evidence and no no_orch signal is classifiable as
// an unexcused failure using the startup funnel query.
func TestRuleLifecycle003_UnexcusedFailureIsDetectable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("unexcused")

	// Only a start event — no milestones reached, no explicit excusal.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
		Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, streamID),
		IngestedAt: ts,
	}})

	// Startup funnel: known=1, got_playable=0, got_no_orch=0 → unexcused=1
	unexcused := h.queryInt(t, `
		WITH funnel AS (
			SELECT
				JSONExtractString(data, 'stream_id') AS sid,
				countIf(JSONExtractString(data, 'type') = 'gateway_receive_stream_request')     AS got_request,
				countIf(JSONExtractString(data, 'type') = 'gateway_receive_few_processed_segments') AS got_playable,
				countIf(JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available') AS got_no_orch
			FROM naap.events
			WHERE org = ? AND event_type = 'stream_trace'
			  AND JSONExtractString(data, 'stream_id') = ?
			GROUP BY sid
		)
		SELECT countIf(got_request > 0 AND got_playable = 0 AND got_no_orch = 0) AS unexcused
		FROM funnel`, h.org, streamID)

	if unexcused != 1 {
		t.Errorf("RULE-LIFECYCLE-003: expected 1 unexcused failure, got %d", unexcused)
	}
}

// TestRuleLifecycle003_OrphanedCloseDoesNotInflateCompleted verifies that
// a gateway_ingest_stream_closed without a prior start does NOT count as an
// unexcused failure and is detectable as an orphaned close.
func TestRuleLifecycle003_OrphanedCloseDoesNotCountAsKnownStream(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orphanStreamID := uid("orphan_close")

	// Close with no prior start.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
		Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_ingest_stream_closed"}`, orphanStreamID),
		IngestedAt: ts,
	}})

	// Stream is not "known" (no gateway_receive_stream_request).
	knownStreams := h.queryInt(t, `
		SELECT count() FROM (
			SELECT DISTINCT JSONExtractString(data, 'stream_id') AS sid
			FROM naap.events
			WHERE org = ?
			  AND event_type = 'stream_trace'
			  AND JSONExtractString(data, 'type') = 'gateway_receive_stream_request'
		)`, h.org)

	if knownStreams != 0 {
		t.Errorf("RULE-LIFECYCLE-003: orphaned close counted as known stream (got %d known)", knownStreams)
	}

	// But completed counter in agg_stream_hourly is 1 (the MV counts all close events).
	// This is the detectable imbalance: completed > 0 while known = 0.
	completed := h.queryInt(t,
		`SELECT sum(completed) FROM naap.agg_stream_hourly FINAL WHERE org = ?`, h.org)
	if completed != 1 {
		t.Errorf("RULE-LIFECYCLE-003: expected orphaned close to appear in completed count, got %d", completed)
	}

	t.Logf("RULE-LIFECYCLE-003: orphan close detected — known_streams=0, completed=%d (imbalance signals orphaned close)", completed)
}

// ── RULE-LIFECYCLE-007 ───────────────────────────────────────────────────────

// TestRuleLifecycle007_DarkStreamsAreDetectable verifies that streams which
// are in the known-stream denominator (gateway_receive_stream_request seen)
// but never generated an ai_stream_status event can be counted as having
// zero health signal coverage.
func TestRuleLifecycle007_DarkStreamsAreDetectable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	litStreamID := uid("lit")    // has status
	darkStreamID := uid("dark")  // no status

	h.insert(t, []rawEvent{
		// Lit stream: start + status.
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, litStreamID), IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, litStreamID), IngestedAt: ts},

		// Dark stream: start only, no status.
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, darkStreamID), IngestedAt: ts},
	})

	// Count streams that started but never got a status event.
	dark := h.queryInt(t, `
		WITH started AS (
			SELECT DISTINCT JSONExtractString(data, 'stream_id') AS sid
			FROM naap.events
			WHERE org = ? AND event_type = 'stream_trace'
			  AND JSONExtractString(data, 'type') = 'gateway_receive_stream_request'
		),
		has_status AS (
			SELECT DISTINCT JSONExtractString(data, 'stream_id') AS sid
			FROM naap.events
			WHERE org = ? AND event_type = 'ai_stream_status'
		)
		SELECT countIf(s.sid NOT IN (SELECT sid FROM has_status))
		FROM started s`,
		h.org, h.org)

	if dark != 1 {
		t.Errorf("RULE-LIFECYCLE-007: expected 1 dark stream (no health signal), got %d", dark)
	}
}

// TestRuleLifecycle007_HealthSignalCoverageRatio checks that the ratio of
// streams with at least one status event vs total known streams is computable
// and matches the fixture.
func TestRuleLifecycle007_HealthSignalCoverageRatioIsComputable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// 3 started streams, 2 with status → coverage = 2/3.
	for i := 0; i < 3; i++ {
		sid := uid("s")
		h.insert(t, []rawEvent{{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, sid), IngestedAt: ts,
		}})
		if i < 2 { // first 2 get a status event
			h.insert(t, []rawEvent{{
				EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
				Data: fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, sid), IngestedAt: ts,
			}})
		}
	}

	started := h.queryInt(t, `
		SELECT count(DISTINCT JSONExtractString(data, 'stream_id'))
		FROM naap.events
		WHERE org = ? AND event_type = 'stream_trace'
		  AND JSONExtractString(data, 'type') = 'gateway_receive_stream_request'`, h.org)

	withStatus := h.queryInt(t, `
		WITH has_status AS (
			SELECT DISTINCT JSONExtractString(data, 'stream_id') AS sid
			FROM naap.events WHERE org = ? AND event_type = 'ai_stream_status'
		)
		SELECT count(DISTINCT JSONExtractString(data, 'stream_id'))
		FROM naap.events
		WHERE org = ? AND event_type = 'stream_trace'
		  AND JSONExtractString(data, 'type') = 'gateway_receive_stream_request'
		  AND JSONExtractString(data, 'stream_id') IN (SELECT sid FROM has_status)`,
		h.org, h.org)

	if started != 3 {
		t.Errorf("RULE-LIFECYCLE-007: expected 3 started streams, got %d", started)
	}
	if withStatus != 2 {
		t.Errorf("RULE-LIFECYCLE-007: expected 2 streams with health signal, got %d", withStatus)
	}
	t.Logf("RULE-LIFECYCLE-007: health_signal_coverage = %d/%d = %.2f", withStatus, started, float64(withStatus)/float64(started))
}
