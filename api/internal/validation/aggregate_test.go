//go:build validation

package validation

// ── RULE-AGGREGATE-004 (partial) ─────────────────────────────────────────────
// GPU Metrics Must Remain Hardware-Backed; Null KPIs Must Not Become Zero
// The current implementation partially enforces this via the WHERE fps > 0
// filter in mv_fps_hourly: zero-FPS status samples (stream not yet producing
// output) are excluded from agg_fps_hourly. This test verifies that filter
// holds — a zero-FPS sample must not appear in the aggregate.
//
// ── RULE-TYPED_RAW-002 ───────────────────────────────────────────────────────
// Capability Payloads Must Expand Into Canonical Snapshot Rows Without Losing
// Orchestrator Identity
// A network_capabilities event carrying an array of N orchestrators must
// produce exactly N rows in agg_orch_state (one per orchestrator).
// Orchestrators with blank address must be filtered out.

import (
	"context"
	"fmt"
	"testing"
)

// ── RULE-AGGREGATE-004 (partial) ─────────────────────────────────────────────

// TestRuleAggregate004_ZeroFpsSamplesExcludedFromAggregate verifies that
// ai_stream_status events with inference_status.fps = 0 do not appear in
// agg_fps_hourly. The MV has WHERE fps > 0 to exclude non-producing samples.
func TestRuleAggregate004_ZeroFpsSamplesExcludedFromAggregate(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := fmt.Sprintf("0x%s", uid("orch"))
	streamID := uid("s")

	h.insert(t, []rawEvent{
		// Zero FPS — stream not yet producing output. Must NOT appear in agg_fps_hourly.
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"streamdiffusion","state":"LOADING","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":0.0,"restart_count":0,"last_error":""}}`,
				streamID, orchAddr),
			IngestedAt: ts},
	})

	// agg_fps_hourly MV has WHERE fps > 0 — zero-FPS sample must produce no row.
	rows := h.queryInt(t, `
		SELECT count()
		FROM naap.agg_fps_hourly
		WHERE org = ? AND orch_address = ?`, h.org, fmt.Sprintf("%s", orchAddr))

	if rows != 0 {
		t.Errorf("RULE-AGGREGATE-004: zero-FPS sample produced %d row(s) in agg_fps_hourly (expected 0)", rows)
	}
}

// TestRuleAggregate004_PositiveFpsSamplesAreAggregated is the complement:
// a positive-FPS sample must appear in agg_fps_hourly with correct sum and count.
func TestRuleAggregate004_PositiveFpsSamplesAreAggregated(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := fmt.Sprintf("0x%s", uid("orch"))
	streamID := uid("s")

	// Two samples at fps=15 and fps=25. Expected: sum=40, count=2.
	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"streamdiffusion","state":"ONLINE","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":15.0,"restart_count":0,"last_error":""}}`,
				streamID, orchAddr),
			IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"streamdiffusion","state":"ONLINE","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":25.0,"restart_count":0,"last_error":""}}`,
				streamID, orchAddr),
			IngestedAt: ts},
	})

	var fpsSumRaw float64
	var sampleCount uint64
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(`
		SELECT sum(inference_fps_sum), sum(sample_count)
		FROM naap.agg_fps_hourly
		WHERE org = '%s' AND orch_address = '%s'`, h.org, orchAddr))
	if err := row.Scan(&fpsSumRaw, &sampleCount); err != nil {
		t.Fatalf("RULE-AGGREGATE-004: scan: %v", err)
	}

	if sampleCount != 2 {
		t.Errorf("RULE-AGGREGATE-004: sample_count = %d, want 2", sampleCount)
	}
	// fps_sum should be 15+25=40 (or close — float64 arithmetic).
	if fpsSumRaw < 39.9 || fpsSumRaw > 40.1 {
		t.Errorf("RULE-AGGREGATE-004: inference_fps_sum = %.2f, want ~40.0", fpsSumRaw)
	}
}

// TestRuleAggregate004_ZeroAndPositiveSamplesAreSeparated inserts both zero
// and positive FPS samples for the same orchestrator and verifies that only
// the positive ones accumulate in the aggregate.
func TestRuleAggregate004_ZeroAndPositiveSamplesAreSeparated(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := fmt.Sprintf("0x%s", uid("orch"))
	streamID := uid("s")

	h.insert(t, []rawEvent{
		// Zero FPS — excluded by MV filter.
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"p","state":"LOADING","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":0.0}}`,
				streamID, orchAddr),
			IngestedAt: ts},
		// Zero FPS — excluded.
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"p","state":"LOADING","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":0.0}}`,
				streamID, orchAddr),
			IngestedAt: ts},
		// Positive FPS — included.
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Data: fmt.Sprintf(
				`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":%q,"url":""},"inference_status":{"fps":20.0}}`,
				streamID, orchAddr),
			IngestedAt: ts},
	})

	var sampleCount uint64
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(`
		SELECT sum(sample_count)
		FROM naap.agg_fps_hourly
		WHERE org = '%s' AND orch_address = '%s'`, h.org, orchAddr))
	if err := row.Scan(&sampleCount); err != nil {
		t.Fatalf("RULE-AGGREGATE-004: scan: %v", err)
	}

	// Only the 1 positive sample should be counted.
	if sampleCount != 1 {
		t.Errorf("RULE-AGGREGATE-004: sample_count = %d after filtering zero-FPS, want 1", sampleCount)
	}
}

// ── RULE-TYPED_RAW-002 ───────────────────────────────────────────────────────

// TestRuleTypedRaw002_CapabilityArrayFansOutToOneRowPerOrch verifies that a
// single network_capabilities event carrying an array of N orchestrators
// produces exactly N rows in agg_orch_state (via arrayJoin in the MV).
func TestRuleTypedRaw002_CapabilityArrayFansOutToOneRowPerOrch(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Two orchestrators in one network_capabilities payload.
	orch1 := fmt.Sprintf("0x%016x_a", uid("o"))
	orch2 := fmt.Sprintf("0x%016x_b", uid("o"))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[
			{"address":%q,"local_address":"node-a","uri":"https://a.example.com","version":"0.7.0"},
			{"address":%q,"local_address":"node-b","uri":"https://b.example.com","version":"0.7.0"}
		]`, orch1, orch2),
		IngestedAt: ts,
	}})

	orchRows := h.queryInt(t,
		`SELECT count() FROM naap.agg_orch_state FINAL
		 WHERE orch_address IN (?, ?)`, orch1, orch2)

	if orchRows != 2 {
		t.Errorf("RULE-TYPED_RAW-002: expected 2 orch rows from array fanout, got %d", orchRows)
	}
}

// TestRuleTypedRaw002_BlankAddressOrchIsFilteredOut verifies that orchestrator
// entries in the capabilities array with a blank address are excluded from
// agg_orch_state (the MV has WHERE address != '').
func TestRuleTypedRaw002_BlankAddressOrchIsFilteredOut(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	validOrch := fmt.Sprintf("0x%s", uid("o"))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[
			{"address":%q,"local_address":"valid","uri":"https://valid.example.com","version":"0.7.0"},
			{"address":"","local_address":"blank-addr","uri":"https://blank.example.com","version":"0.7.0"}
		]`, validOrch),
		IngestedAt: ts,
	}})

	// Only the valid orch should appear in agg_orch_state.
	rows := h.queryInt(t,
		`SELECT count() FROM naap.agg_orch_state FINAL WHERE orch_address = ?`,
		fmt.Sprintf("%s", validOrch))

	if rows != 1 {
		t.Errorf("RULE-TYPED_RAW-002: expected 1 valid orch row, got %d", rows)
	}

	// Blank address must not appear.
	blankRows := h.queryInt(t,
		`SELECT count() FROM naap.agg_orch_state FINAL WHERE orch_address = ''`)
	if blankRows != 0 {
		t.Errorf("RULE-TYPED_RAW-002: blank-address orch leaked into agg_orch_state (%d rows)", blankRows)
	}
}

// TestRuleTypedRaw002_EmptyOrNullCapabilityDataProducesNoOrchRows verifies
// that malformed network_capabilities events (empty array, "null", or empty
// string data) do not produce any orch rows — guarded by the MV WHERE clause.
func TestRuleTypedRaw002_EmptyCapabilityDataProducesNoOrchRows(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// All three malformed shapes from the known-risk comment in migration 005.
	h.insert(t, []rawEvent{
		{EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: "[]", IngestedAt: ts},
		{EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: "null", IngestedAt: ts},
		{EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data: "", IngestedAt: ts},
	})

	orchRows := h.queryInt(t,
		`SELECT count() FROM naap.agg_orch_state WHERE org = ?`, h.org)

	if orchRows != 0 {
		t.Errorf("RULE-TYPED_RAW-002: empty/null capability data produced %d orch rows (expected 0)", orchRows)
	}
}

// TestRuleTypedRaw002_CapabilityLocalAddressFallback verifies the name field
// preference: local_address is used when present, falls back to address when blank.
func TestRuleTypedRaw002_CapabilityNameFallback(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	orchWithName := fmt.Sprintf("0x%s", uid("o"))
	orchNoName := fmt.Sprintf("0x%s", uid("o"))

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[
			{"address":%q,"local_address":"friendly-name","uri":"https://a.example.com","version":"0.7.0"},
			{"address":%q,"local_address":"","uri":"https://b.example.com","version":"0.7.0"}
		]`, orchWithName, orchNoName),
		IngestedAt: ts,
	}})

	var nameA, nameB string
	rowA := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT name FROM naap.agg_orch_state FINAL WHERE orch_address = '%s'`, orchWithName))
	if err := rowA.Scan(&nameA); err != nil {
		t.Fatalf("RULE-TYPED_RAW-002: scan nameA: %v", err)
	}

	rowB := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT name FROM naap.agg_orch_state FINAL WHERE orch_address = '%s'`, orchNoName))
	if err := rowB.Scan(&nameB); err != nil {
		t.Fatalf("RULE-TYPED_RAW-002: scan nameB: %v", err)
	}

	if nameA != "friendly-name" {
		t.Errorf("RULE-TYPED_RAW-002: expected name='friendly-name' (local_address), got %q", nameA)
	}
	// When local_address is blank, name falls back to address.
	if nameB != orchNoName {
		t.Errorf("RULE-TYPED_RAW-002: expected name=%q (address fallback), got %q", orchNoName, nameB)
	}
}
