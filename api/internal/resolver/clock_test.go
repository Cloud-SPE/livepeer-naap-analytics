package resolver

import (
	"testing"
	"time"
)

// TestRunRequestEffectiveNowZeroFallsThroughToWallClock verifies that a
// production daemon (Now unset) still works: EffectiveNow returns a
// current UTC timestamp within a small tolerance.
func TestRunRequestEffectiveNowZeroFallsThroughToWallClock(t *testing.T) {
	req := RunRequest{}

	before := time.Now().UTC()
	got := req.EffectiveNow()
	after := time.Now().UTC()

	if got.Location() != time.UTC {
		t.Fatalf("EffectiveNow() location = %v, want UTC", got.Location())
	}
	if got.Before(before) || got.After(after) {
		t.Fatalf("EffectiveNow() = %v, expected in [%v, %v]", got, before, after)
	}
}

// TestRunRequestEffectiveNowPinnedIsDeterministic verifies the core
// determinism contract: a non-zero RunRequest.Now always returns the same
// UTC value, regardless of wall clock at the call site.
func TestRunRequestEffectiveNowPinnedIsDeterministic(t *testing.T) {
	pinned := time.Date(2026, 4, 17, 12, 34, 56, 123456789, time.UTC)
	req := RunRequest{Now: pinned}

	for i := 0; i < 10; i++ {
		got := req.EffectiveNow()
		if !got.Equal(pinned) {
			t.Fatalf("call %d: EffectiveNow() = %v, want %v", i, got, pinned)
		}
		if got.Location() != time.UTC {
			t.Fatalf("call %d: location = %v, want UTC", i, got.Location())
		}
	}
}

// TestRunRequestEffectiveNowNormalisesToUTC ensures that callers passing a
// non-UTC timezone still get a UTC result. This matters because the whole
// pipeline stores DateTime64(3, 'UTC') columns; leaking a local TZ into a
// row would make two runs in different timezones disagree.
func TestRunRequestEffectiveNowNormalisesToUTC(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("skipping TZ test: %v", err)
	}
	local := time.Date(2026, 4, 17, 8, 0, 0, 0, ny)
	req := RunRequest{Now: local}

	got := req.EffectiveNow()

	if got.Location() != time.UTC {
		t.Fatalf("EffectiveNow() location = %v, want UTC", got.Location())
	}
	if !got.Equal(local) {
		// Equal() compares instant-in-time, not the zone; the two should
		// point at the same moment even after normalisation.
		t.Fatalf("EffectiveNow() = %v, want same instant as %v", got, local)
	}
}

// TestDecisionIDIsDeterministicUnderFrozenNow proves the serving-layer-v2
// determinism invariant: with a pinned RunRequest.Now, the stableHash used
// to derive decision_id (canonical_selection_attribution_decisions primary
// key) collapses to the same value on every replay. Before the clock
// freeze, this hash included time.Now().UTC() and so drifted every run.
func TestDecisionIDIsDeterministicUnderFrozenNow(t *testing.T) {
	now := time.Date(2026, 4, 17, 12, 34, 56, 123456789, time.UTC)
	row := SelectionDecision{
		SelectionEventID: "sel-001",
		Status:           "resolved",
		Reason:           "snapshot_match",
		Method:           "capability_interval",
		InputHash:        "abc123",
	}

	// This is the exact shape of decisionID derivation in
	// repo.insertDecisionRows — if that shape changes, this test must
	// change in lockstep.
	id1 := stableHash(row.SelectionEventID, row.Status, row.Reason, row.Method, row.InputHash, now.Format(time.RFC3339Nano))
	id2 := stableHash(row.SelectionEventID, row.Status, row.Reason, row.Method, row.InputHash, now.Format(time.RFC3339Nano))
	if id1 != id2 {
		t.Fatalf("decision_id not deterministic: %q != %q", id1, id2)
	}

	// Confirm that a different `now` produces a different id — the hash is
	// not ignoring its time argument.
	laterNow := now.Add(time.Minute)
	id3 := stableHash(row.SelectionEventID, row.Status, row.Reason, row.Method, row.InputHash, laterNow.Format(time.RFC3339Nano))
	if id1 == id3 {
		t.Fatalf("decision_id should differ across different `now`: both %q", id1)
	}
}
