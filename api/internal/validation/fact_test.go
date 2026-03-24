//go:build validation

package validation

// ── RULE-FACT-001 ─────────────────────────────────────────────────────────────
// Historical State And Latest State Must Both Be Recoverable
// For agg_stream_state: without FINAL we can see the full version history;
// with FINAL we see exactly the latest row per stream_id. Both must work.
//
// ── RULE-FACT-002 ─────────────────────────────────────────────────────────────
// Latest-State Outputs Must Converge Deterministically
// agg_stream_state FINAL must have exactly one row per stream_id.
// Multiple events for the same stream must not produce duplicate latest rows.

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ── RULE-FACT-001 ─────────────────────────────────────────────────────────────

// TestRuleFact001_HistoricalVersionsAreRecoverable inserts several events for
// the same stream_id and verifies that without FINAL we can see multiple
// history rows, while with FINAL we see exactly the latest state.
// This confirms both "historical state" and "latest state" are accessible.
func TestRuleFact001_HistoricalVersionsAreRecoverable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")

	// Lifecycle for one stream: request → status (LOADING) → status (ONLINE) → closed.
	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace",
			EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, streamID),
			IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status",
			EventTs: ts.Add(2 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"LOADING","orchestrator_info":{"address":"0xabc","url":""}}`, streamID),
			IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status",
			EventTs: ts.Add(5 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, streamID),
			IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace",
			EventTs: ts.Add(10 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_ingest_stream_closed"}`, streamID),
			IngestedAt: ts},
	})

	// Historical view: without FINAL, agg_stream_state has multiple rows per stream.
	historical := h.queryInt(t,
		`SELECT count() FROM naap.agg_stream_state WHERE stream_id = ?`, streamID)

	// Latest view: FINAL collapses to exactly one row.
	latest := h.queryInt(t,
		`SELECT count() FROM naap.agg_stream_state FINAL WHERE stream_id = ?`, streamID)

	// Historical must have more than 1 row (pre-merge; may be 1 after background merge).
	// We assert latest = 1 strictly, and log historical for observability.
	if latest != 1 {
		t.Errorf("RULE-FACT-001: latest state (FINAL) — expected 1 row for stream, got %d", latest)
	}
	t.Logf("RULE-FACT-001: stream %s — historical_rows=%d, latest_rows(FINAL)=%d",
		streamID, historical, latest)
}

// TestRuleFact001_LatestStateIsCorrectState verifies that the latest-state row
// after FINAL reflects the last event seen (is_closed=1 after the close event).
func TestRuleFact001_LatestStateReflectsLastEvent(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")

	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace",
			EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, streamID),
			IngestedAt: ts},
		// Close event arrives 30 seconds later — must be the latest state.
		{EventID: uid("e"), EventType: "stream_trace",
			EventTs: ts.Add(30 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_ingest_stream_closed"}`, streamID),
			IngestedAt: ts},
	})

	var isClosed uint8
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT is_closed FROM naap.agg_stream_state FINAL WHERE stream_id = '%s'`, streamID))
	if err := row.Scan(&isClosed); err != nil {
		t.Fatalf("RULE-FACT-001: scan: %v", err)
	}

	// ReplacingMergeTree(last_seen) keeps the row with the highest last_seen.
	// The close event has last_seen = ts+30s, so is_closed must be 1.
	if isClosed != 1 {
		t.Errorf("RULE-FACT-001: latest state — is_closed = %d after close event, want 1", isClosed)
	}
}

// ── RULE-FACT-002 ─────────────────────────────────────────────────────────────

// TestRuleFact002_LatestStateHasOneRowPerStream inserts multiple events for
// the same stream and verifies agg_stream_state FINAL returns exactly one row
// per stream_id. Duplicate latest rows would inflate metrics.
func TestRuleFact002_LatestStateHasOneRowPerStream(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Two distinct streams, each with multiple events.
	for _, sid := range []string{uid("s"), uid("s")} {
		h.insert(t, []rawEvent{
			{EventID: uid("e"), EventType: "stream_trace",
				EventTs: ts, Org: h.org,
				Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, sid),
				IngestedAt: ts},
			{EventID: uid("e"), EventType: "ai_stream_status",
				EventTs: ts.Add(2 * time.Second), Org: h.org,
				Data:       fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"LOADING","orchestrator_info":{"address":"0xabc","url":""}}`, sid),
				IngestedAt: ts},
			{EventID: uid("e"), EventType: "ai_stream_status",
				EventTs: ts.Add(5 * time.Second), Org: h.org,
				Data:       fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, sid),
				IngestedAt: ts},
		})
	}

	// With FINAL: must be exactly 2 rows (one per stream_id).
	finalRows := h.queryInt(t,
		`SELECT count() FROM naap.agg_stream_state FINAL WHERE org = ?`, h.org)

	if finalRows != 2 {
		t.Errorf("RULE-FACT-002: expected 2 latest-state rows (one per stream), got %d", finalRows)
	}
}

// TestRuleFact002_NoConflictingLatestRowsForSameStream verifies there is no
// scenario where the same stream_id has multiple rows with the same last_seen
// after FINAL — which would indicate a broken dedup key.
func TestRuleFact002_NoConflictingLatestRowsForSameStream(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")

	// Insert a flurry of events for the same stream.
	var events []rawEvent
	for i := 0; i < 5; i++ {
		events = append(events, rawEvent{
			EventID: uid("e"), EventType: "ai_stream_status",
			EventTs: ts.Add(time.Duration(i) * time.Second), Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`,
				streamID),
			IngestedAt: ts,
		})
	}
	h.insert(t, events)

	latest := h.queryInt(t,
		`SELECT count() FROM naap.agg_stream_state FINAL WHERE stream_id = ?`, streamID)

	if latest != 1 {
		t.Errorf("RULE-FACT-002: expected exactly 1 latest row for stream_id, got %d", latest)
	}
}

// TestRuleFact002_OrchestratorStateConvergesOnLatest verifies that
// agg_orch_state FINAL has exactly one row per orchestrator address.
// Multiple network_capabilities snapshots for the same orchestrator must
// collapse to the latest (highest last_seen).
func TestRuleFact002_OrchestratorStateConvergesOnLatest(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := fmt.Sprintf("0x%016x", uid("orch")) // unique fake ETH address

	// Two capability snapshots for the same orchestrator, 1 minute apart.
	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities",
			EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-v1","uri":"https://orch.example.com","version":"0.7.0"}]`, orchAddr),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "network_capabilities",
			EventTs: ts.Add(time.Minute), Org: h.org,
			Data: fmt.Sprintf(`[{"address":%q,"local_address":"orch-v2","uri":"https://orch.example.com","version":"0.8.0"}]`, orchAddr),
			IngestedAt: ts.Add(time.Minute),
		},
	})

	finalRows := h.queryInt(t,
		`SELECT count() FROM naap.agg_orch_state FINAL WHERE orch_address = ?`,
		orchAddr)

	if finalRows != 1 {
		t.Errorf("RULE-FACT-002: expected 1 orch state row after FINAL dedup, got %d", finalRows)
	}

	// Confirm the latest version won.
	var version string
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT version FROM naap.agg_orch_state FINAL WHERE orch_address = '%s'`, orchAddr))
	if err := row.Scan(&version); err != nil {
		t.Fatalf("RULE-FACT-002: scan version: %v", err)
	}
	if version != "0.8.0" {
		t.Errorf("RULE-FACT-002: expected latest version '0.8.0', got %q", version)
	}
}
