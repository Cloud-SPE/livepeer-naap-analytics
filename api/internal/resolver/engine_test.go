package resolver

import (
	"testing"
	"time"
)

func TestRunRequestMutates(t *testing.T) {
	req := RunRequest{Mode: ModeBackfill}
	if !req.Mutates() {
		t.Fatal("backfill should mutate by default")
	}
	req.DryRun = true
	if req.Mutates() {
		t.Fatal("dry-run backfill should not mutate")
	}
	req = RunRequest{Mode: ModeVerify}
	if req.Mutates() {
		t.Fatal("verify should not mutate")
	}
}

func TestParseModeIncludesBootstrap(t *testing.T) {
	if got := parseMode("bootstrap"); got != ModeBootstrap {
		t.Fatalf("parseMode(bootstrap) = %q, want %q", got, ModeBootstrap)
	}
}

func TestParseModeIncludesAuto(t *testing.T) {
	if got := parseMode("auto"); got != ModeAuto {
		t.Fatalf("parseMode(auto) = %q, want %q", got, ModeAuto)
	}
}

func TestDeriveBootstrapWindowUsesClosedHistoricalDays(t *testing.T) {
	minRaw := time.Date(2026, 3, 23, 6, 25, 10, 0, time.UTC)
	maxRaw := time.Date(2026, 3, 24, 3, 0, 0, 0, time.UTC)
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)

	start, end, caughtUp := deriveBootstrapWindow(minRaw, maxRaw, now, 10*time.Minute, nil, nil)
	if !start.Equal(minRaw) {
		t.Fatalf("start = %s, want %s", start, minRaw)
	}
	wantEnd := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	if !end.Equal(wantEnd) {
		t.Fatalf("end = %s, want %s", end, wantEnd)
	}
	if caughtUp {
		t.Fatalf("caughtUp = true, want false")
	}
}

func TestDeriveBootstrapWindowCapsAtRequestedEnd(t *testing.T) {
	minRaw := time.Date(2026, 3, 23, 6, 25, 10, 0, time.UTC)
	maxRaw := time.Date(2026, 3, 30, 11, 55, 0, 0, time.UTC)
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	requestedEnd := time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)

	_, end, caughtUp := deriveBootstrapWindow(minRaw, maxRaw, now, 10*time.Minute, nil, &requestedEnd)
	if !end.Equal(requestedEnd) {
		t.Fatalf("end = %s, want %s", end, requestedEnd)
	}
	if !caughtUp {
		t.Fatalf("caughtUp = false, want true")
	}
}

func TestFilterPendingPartitionsDropsSuccessfulDays(t *testing.T) {
	day := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	parts := []backfillPartition{
		{Org: "daydream", EventDate: day},
		{Org: "cloudspe", EventDate: day},
	}
	successes := map[string]struct{}{
		"daydream|2026-03-23": {},
	}
	got := filterPendingPartitions(parts, successes)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].Org != "cloudspe" {
		t.Fatalf("remaining org = %s, want cloudspe", got[0].Org)
	}
}

func TestPickWinningWindowClaimPrefersEarliestClaim(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	claims := []windowClaim{
		{ClaimKey: "b", OwnerID: "resolver-b", CreatedAt: now.Add(2 * time.Second)},
		{ClaimKey: "a", OwnerID: "resolver-a", CreatedAt: now},
	}
	got := pickWinningWindowClaim(claims)
	if got.ClaimKey != "a" {
		t.Fatalf("winner = %q, want %q", got.ClaimKey, "a")
	}
}

func TestPickWinningWindowClaimBreaksTiesByOwnerThenKey(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	claims := []windowClaim{
		{ClaimKey: "b", OwnerID: "resolver-b", CreatedAt: now},
		{ClaimKey: "a", OwnerID: "resolver-a", CreatedAt: now},
	}
	got := pickWinningWindowClaim(claims)
	if got.ClaimKey != "a" {
		t.Fatalf("winner = %q, want %q", got.ClaimKey, "a")
	}
}

func TestChooseAutoPhasePriority(t *testing.T) {
	if got := chooseAutoPhase(true, true, false); got != "bootstrap_backlog" {
		t.Fatalf("backlog priority = %q, want bootstrap_backlog", got)
	}
	if got := chooseAutoPhase(false, true, false); got != "historical_repair" {
		t.Fatalf("dirty priority = %q, want historical_repair", got)
	}
	if got := chooseAutoPhase(true, true, true); got != "tail" {
		t.Fatalf("tail due priority = %q, want tail", got)
	}
	if got := chooseAutoPhase(false, false, false); got != "idle" {
		t.Fatalf("idle phase = %q, want idle", got)
	}
}

func TestDirtyScanStateKeySortsExcludedPrefixes(t *testing.T) {
	left := dirtyScanStateKey("daydream", []string{"ztest_", "atest_"})
	right := dirtyScanStateKey("daydream", []string{"atest_", "ztest_"})
	if left != right {
		t.Fatalf("dirty scan state keys differ for same prefixes: %q vs %q", left, right)
	}
}
