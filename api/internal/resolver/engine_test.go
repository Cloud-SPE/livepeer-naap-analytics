package resolver

import (
	"context"
	"errors"
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
	if got := chooseAutoPhase(true, true, true, true, true); got != "tail" {
		t.Fatalf("tail priority = %q, want tail", got)
	}
	if got := chooseAutoPhase(true, true, true, true, false); got != "same_day_repair" {
		t.Fatalf("same day priority = %q, want same_day_repair", got)
	}
	if got := chooseAutoPhase(true, true, true, false, false); got != "repair_request" {
		t.Fatalf("repair request priority = %q, want repair_request", got)
	}
	if got := chooseAutoPhase(true, true, false, false, false); got != "historical_repair" {
		t.Fatalf("historical priority = %q, want historical_repair", got)
	}
	if got := chooseAutoPhase(true, false, false, false, false); got != "bootstrap_backlog" {
		t.Fatalf("backlog priority = %q, want bootstrap_backlog", got)
	}
	if got := chooseAutoPhase(false, false, false, false, false); got != "idle" {
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

func TestDirtyPartitionReadyHonorsQuietPeriod(t *testing.T) {
	now := time.Date(2026, 3, 30, 16, 45, 0, 0, time.UTC)
	part := dirtyPartition{LastDirtyAt: now.Add(-90 * time.Second)}
	if dirtyPartitionReady(part, now, 2*time.Minute) {
		t.Fatal("dirty partition should not be ready before quiet period elapses")
	}
	part.LastDirtyAt = now.Add(-3 * time.Minute)
	if !dirtyPartitionReady(part, now, 2*time.Minute) {
		t.Fatal("dirty partition should be ready after quiet period elapses")
	}
	if !dirtyPartitionReady(part, now, 0) {
		t.Fatal("dirty partition should be ready immediately when quiet period is disabled")
	}
}

func TestDirtyWindowReadyHonorsLatenessAndQuietPeriod(t *testing.T) {
	now := time.Date(2026, 4, 13, 16, 45, 0, 0, time.UTC)
	window := dirtyWindow{
		WindowStart: time.Date(2026, 4, 13, 15, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 4, 13, 16, 0, 0, 0, time.UTC),
		LastDirtyAt: now.Add(-90 * time.Second),
	}
	if dirtyWindowReady(window, now, 10*time.Minute, 2*time.Minute) {
		t.Fatal("dirty window should wait for quiet period")
	}
	window.LastDirtyAt = now.Add(-3 * time.Minute)
	if !dirtyWindowReady(window, now, 10*time.Minute, 2*time.Minute) {
		t.Fatal("dirty window should be ready once closed and quiet")
	}
	window.WindowEnd = now.Add(-5 * time.Minute)
	if dirtyWindowReady(window, now, 10*time.Minute, 2*time.Minute) {
		t.Fatal("dirty window should not be ready inside lateness window")
	}
}

func TestPendingRepairRequestStateClearsClaimAndTimingFields(t *testing.T) {
	now := time.Date(2026, 4, 13, 16, 45, 0, 0, time.UTC)
	lease := now.Add(2 * time.Minute)
	started := now.Add(-time.Minute)
	finished := now.Add(-30 * time.Second)
	state := pendingRepairRequestState(repairRequest{
		RequestID:        "req-1",
		Status:           "claimed",
		ClaimOwner:       "resolver-1",
		LeaseExpiresAt:   &lease,
		StartedAt:        &started,
		FinishedAt:       &finished,
		LastErrorSummary: "claimed elsewhere",
	}, now)
	if state.Status != "pending" {
		t.Fatalf("status = %q, want pending", state.Status)
	}
	if state.ClaimOwner != "" || state.LeaseExpiresAt != nil {
		t.Fatal("pending repair request should not retain claim ownership")
	}
	if state.StartedAt != nil || state.FinishedAt != nil {
		t.Fatal("pending repair request should clear timing fields")
	}
	if state.LastErrorSummary != "" {
		t.Fatalf("last_error_summary = %q, want empty", state.LastErrorSummary)
	}
}

func TestNextDirtyPartitionStateCoalescesClaimedPartition(t *testing.T) {
	now := time.Date(2026, 3, 30, 16, 45, 0, 0, time.UTC)
	lease := now.Add(2 * time.Minute)
	current := &dirtyPartition{
		Org:            "daydream",
		EventDate:      time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC),
		Status:         "claimed",
		ClaimOwner:     "resolver-1234",
		LeaseExpiresAt: &lease,
		AttemptCount:   7,
		FirstDirtyAt:   now.Add(-10 * time.Minute),
		LastDirtyAt:    now.Add(-time.Minute),
		UpdatedAt:      now.Add(-time.Minute),
	}
	part := backfillPartition{Org: "daydream", EventDate: current.EventDate}
	state := nextDirtyPartitionState(current, part, now)
	if state.Status != "claimed" {
		t.Fatalf("status = %q, want claimed", state.Status)
	}
	if state.ClaimOwner != current.ClaimOwner {
		t.Fatalf("claim owner = %q, want %q", state.ClaimOwner, current.ClaimOwner)
	}
	if state.LeaseExpiresAt == nil || !state.LeaseExpiresAt.Equal(lease) {
		t.Fatalf("lease expiry = %v, want %v", state.LeaseExpiresAt, lease)
	}
	if state.AttemptCount != current.AttemptCount {
		t.Fatalf("attempt_count = %d, want %d", state.AttemptCount, current.AttemptCount)
	}
	if !state.LastDirtyAt.Equal(now) {
		t.Fatalf("last_dirty_at = %s, want %s", state.LastDirtyAt, now)
	}
}

func TestNextDirtyPartitionStateRequeuesSuccessfulPartitionAsPending(t *testing.T) {
	now := time.Date(2026, 3, 30, 16, 45, 0, 0, time.UTC)
	current := &dirtyPartition{
		Org:          "daydream",
		EventDate:    time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC),
		Status:       "success",
		AttemptCount: 3,
		FirstDirtyAt: now.Add(-10 * time.Minute),
		LastDirtyAt:  now.Add(-5 * time.Minute),
		UpdatedAt:    now.Add(-5 * time.Minute),
	}
	part := backfillPartition{Org: "daydream", EventDate: current.EventDate}
	state := nextDirtyPartitionState(current, part, now)
	if state.Status != "pending" {
		t.Fatalf("status = %q, want pending", state.Status)
	}
	if state.AttemptCount != current.AttemptCount {
		t.Fatalf("attempt_count = %d, want %d", state.AttemptCount, current.AttemptCount)
	}
	if state.ClaimOwner != "" || state.LeaseExpiresAt != nil {
		t.Fatalf("pending state should not retain claim ownership")
	}
}

func TestNextDirtyWindowStateCoalescesClaimedWindow(t *testing.T) {
	now := time.Date(2026, 4, 13, 16, 45, 0, 0, time.UTC)
	lease := now.Add(2 * time.Minute)
	current := &dirtyWindow{
		Org:            "daydream",
		WindowStart:    time.Date(2026, 4, 13, 15, 0, 0, 0, time.UTC),
		WindowEnd:      time.Date(2026, 4, 13, 16, 0, 0, 0, time.UTC),
		Status:         "claimed",
		ClaimOwner:     "resolver-1234",
		LeaseExpiresAt: &lease,
		AttemptCount:   2,
		FirstDirtyAt:   now.Add(-10 * time.Minute),
		LastDirtyAt:    now.Add(-time.Minute),
		UpdatedAt:      now.Add(-time.Minute),
	}
	window := dirtyWindow{Org: "daydream", WindowStart: current.WindowStart, WindowEnd: current.WindowEnd}
	state := nextDirtyWindowState(current, window, now)
	if state.Status != "claimed" {
		t.Fatalf("status = %q, want claimed", state.Status)
	}
	if state.ClaimOwner != current.ClaimOwner {
		t.Fatalf("claim owner = %q, want %q", state.ClaimOwner, current.ClaimOwner)
	}
	if state.LeaseExpiresAt == nil || !state.LeaseExpiresAt.Equal(lease) {
		t.Fatalf("lease expiry = %v, want %v", state.LeaseExpiresAt, lease)
	}
}

func TestRequeueDirtyPartitionStateExpiresClaim(t *testing.T) {
	now := time.Date(2026, 4, 14, 13, 0, 0, 0, time.UTC)
	lease := now.Add(-time.Minute)
	current := dirtyPartition{
		Org:            "daydream",
		EventDate:      time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC),
		Status:         "claimed",
		ClaimOwner:     "resolver-a",
		LeaseExpiresAt: &lease,
		AttemptCount:   2,
	}
	state, ok := requeueDirtyPartitionState(current, now)
	if !ok {
		t.Fatal("expected expired dirty partition claim to be requeued")
	}
	if state.Status != "pending" {
		t.Fatalf("status = %q, want pending", state.Status)
	}
	if state.ClaimOwner != "" || state.LeaseExpiresAt != nil {
		t.Fatal("requeued dirty partition should clear claim ownership")
	}
}

func TestRequeueDirtyWindowStateExpiresClaim(t *testing.T) {
	now := time.Date(2026, 4, 14, 13, 0, 0, 0, time.UTC)
	lease := now.Add(-time.Minute)
	current := dirtyWindow{
		Org:            "daydream",
		WindowStart:    time.Date(2026, 4, 14, 11, 0, 0, 0, time.UTC),
		WindowEnd:      time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC),
		Status:         "claimed",
		ClaimOwner:     "resolver-a",
		LeaseExpiresAt: &lease,
		AttemptCount:   2,
	}
	state, ok := requeueDirtyWindowState(current, now)
	if !ok {
		t.Fatal("expected expired dirty window claim to be requeued")
	}
	if state.Status != "pending" {
		t.Fatalf("status = %q, want pending", state.Status)
	}
	if state.ClaimOwner != "" || state.LeaseExpiresAt != nil {
		t.Fatal("requeued dirty window should clear claim ownership")
	}
}

func TestRequeueRepairRequestStateExpiresClaim(t *testing.T) {
	now := time.Date(2026, 4, 14, 13, 0, 0, 0, time.UTC)
	lease := now.Add(-time.Minute)
	started := now.Add(-3 * time.Minute)
	current := repairRequest{
		RequestID:      "req-1",
		Org:            "daydream",
		WindowStart:    time.Date(2026, 4, 14, 11, 0, 0, 0, time.UTC),
		WindowEnd:      time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC),
		Status:         "claimed",
		ClaimOwner:     "resolver-a",
		LeaseExpiresAt: &lease,
		StartedAt:      &started,
		AttemptCount:   1,
	}
	state, ok := requeueRepairRequestState(current, now)
	if !ok {
		t.Fatal("expected expired repair request claim to be requeued")
	}
	if state.Status != "pending" {
		t.Fatalf("status = %q, want pending", state.Status)
	}
	if state.ClaimOwner != "" || state.LeaseExpiresAt != nil {
		t.Fatal("requeued repair request should clear claim ownership")
	}
	if state.StartedAt != nil || state.FinishedAt != nil {
		t.Fatal("requeued repair request should clear active timestamps")
	}
}

func TestRepairRequestExecutionContextUsesFiveMinuteTimeout(t *testing.T) {
	ctx, cancel := repairRequestExecutionContext(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline on repair request context")
	}
	remaining := time.Until(deadline)
	if remaining < 4*time.Minute || remaining > 5*time.Minute+10*time.Second {
		t.Fatalf("repair request deadline remaining = %s, want about 5m", remaining)
	}
}

func TestNormalizeRepairRequestErrorRewritesDeadlineExceeded(t *testing.T) {
	err := normalizeRepairRequestError(context.DeadlineExceeded)
	if err == nil || err.Error() != "repair request exceeded 5m execution timeout; narrow the window or use resolver-repair-window/backfill" {
		t.Fatalf("unexpected normalized error: %v", err)
	}
}

func TestNormalizeRepairRequestErrorLeavesOtherErrorsUntouched(t *testing.T) {
	original := errors.New("boom")
	err := normalizeRepairRequestError(original)
	if !errors.Is(err, original) {
		t.Fatalf("expected original error, got %v", err)
	}
}
