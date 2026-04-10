package resolver

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
)

type Engine struct {
	cfg     *config.Config
	log     *zap.Logger
	repo    *repo
	ownerID string
	httpSrv *http.Server

	mu     sync.RWMutex
	health schedulerHealthState
}

func New(cfg *config.Config, log *zap.Logger) (*Engine, error) {
	ownerID := fmt.Sprintf("resolver-%08x", rand.Uint32())
	repo, err := newRepo(cfg, log, ownerID)
	if err != nil {
		return nil, err
	}
	engine := &Engine{
		cfg:     cfg,
		log:     log,
		repo:    repo,
		ownerID: ownerID,
		health: schedulerHealthState{
			Status: "ok",
			Mode:   parseMode(cfg.ResolverMode).String(),
			Phase:  "idle",
		},
	}
	engine.httpSrv = newMetricsServer(cfg.ResolverPort, engine.healthSnapshot)
	setResolverSchedulerPhase("idle")
	return engine, nil
}

func (m Mode) String() string {
	if m == "" {
		return string(ModeTail)
	}
	return string(m)
}

func (e *Engine) healthSnapshot() schedulerHealthState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snapshot := e.health
	if e.health.AcceptedRawScanWatermark != nil {
		copyWatermark := *e.health.AcceptedRawScanWatermark
		snapshot.AcceptedRawScanWatermark = &copyWatermark
	}
	if e.health.TailWatermark != nil {
		copyWatermark := e.health.TailWatermark.UTC()
		snapshot.TailWatermark = &copyWatermark
	}
	if e.health.ActiveClaim != nil {
		copyClaim := *e.health.ActiveClaim
		snapshot.ActiveClaim = &copyClaim
	}
	if snapshot.Status == "" {
		snapshot.Status = "ok"
	}
	return snapshot
}

func (e *Engine) setPhase(mode Mode, phase string) {
	e.mu.Lock()
	e.health.Status = "ok"
	e.health.Mode = mode.String()
	e.health.Phase = phase
	e.mu.Unlock()
	setResolverSchedulerPhase(phase)
}

func (e *Engine) setDirtyQueueDepth(depth uint64) {
	e.mu.Lock()
	e.health.DirtyQueueDepth = depth
	e.mu.Unlock()
	resolverDirtyQueueDepth.WithLabelValues("historical_repair").Set(float64(depth))
}

func (e *Engine) setBootstrapBacklogDepth(depth uint64) {
	resolverDirtyQueueDepth.WithLabelValues("bootstrap_backlog").Set(float64(depth))
}

func (e *Engine) setAcceptedRawScanWatermark(mark *dirtyScanWatermark) {
	e.mu.Lock()
	if mark == nil {
		e.health.AcceptedRawScanWatermark = nil
		e.mu.Unlock()
		resolverDirtyScanWatermarkUnix.Set(0)
		return
	}
	copyMark := *mark
	copyMark.IngestedAt = copyMark.IngestedAt.UTC()
	e.health.AcceptedRawScanWatermark = &copyMark
	e.mu.Unlock()
	resolverDirtyScanWatermarkUnix.Set(float64(copyMark.IngestedAt.Unix()))
}

func (e *Engine) setTailWatermark(ts *time.Time) {
	e.mu.Lock()
	if ts == nil {
		e.health.TailWatermark = nil
		e.mu.Unlock()
		return
	}
	copyTS := ts.UTC()
	e.health.TailWatermark = &copyTS
	e.mu.Unlock()
}

func (e *Engine) setActiveClaim(claim *windowClaim) {
	e.mu.Lock()
	if claim == nil {
		e.health.ActiveClaim = nil
		e.mu.Unlock()
		return
	}
	e.health.ActiveClaim = &schedulerActiveClaim{
		Mode:        claim.Mode,
		Org:         claim.Org,
		WindowStart: claim.WindowStart.UTC(),
		WindowEnd:   claim.WindowEnd.UTC(),
		OwnerID:     claim.OwnerID,
	}
	e.mu.Unlock()
}

func (e *Engine) Run(ctx context.Context, req RunRequest) {
	if !e.cfg.ResolverEnabled {
		e.log.Info("resolver disabled")
		return
	}
	if req.Mode == "" {
		req.Mode = parseMode(e.cfg.ResolverMode)
	}
	mode := req.Mode
	e.setPhase(mode, "idle")
	if req.DryRun {
		stats, err := e.Execute(ctx, req)
		if err != nil {
			e.log.Error("resolver dry-run failed", zap.Error(err))
		} else {
			e.log.Info("resolver dry-run completed",
				zap.String("mode", string(req.Mode)),
				zap.Int("selection_events", stats.SelectionEvents),
				zap.Int("capability_versions", stats.CapabilityVersions),
				zap.Int("capability_intervals", stats.CapabilityIntervals),
				zap.Int("decisions", stats.Decisions),
				zap.Int("session_rows", stats.SessionRows),
				zap.Int("status_hour_rows", stats.StatusHourRows),
				zap.Int("ai_batch_job_rows", stats.AIBatchJobRows),
				zap.Int("byoc_job_rows", stats.BYOCJobRows),
			)
		}
		_ = e.Close(context.Background())
		return
	}
	go func() {
		e.log.Info("resolver metrics server started", zap.String("addr", e.httpSrv.Addr))
		if err := e.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			e.log.Warn("resolver metrics server stopped", zap.Error(err))
		}
	}()
	if mode != ModeTail && mode != ModeBootstrap && mode != ModeAuto {
		if _, err := e.Execute(ctx, req); err != nil {
			e.log.Error("resolver execution failed", zap.Error(err))
		}
		_ = e.Close(context.Background())
		return
	}

	if mode == ModeBootstrap {
		if _, err := e.Execute(ctx, req); err != nil {
			e.log.Error("resolver bootstrap failed", zap.Error(err))
			_ = e.Close(context.Background())
			return
		}
		if ctx.Err() != nil {
			_ = e.Close(context.Background())
			e.log.Info("resolver stopped")
			return
		}
		e.log.Info("resolver bootstrap complete, entering tail mode")
	}

	if mode == ModeAuto {
		e.runAutoLoop(ctx, req)
		return
	}

	tailReq := req
	tailReq.Mode = ModeTail
	tailReq.Start = nil
	tailReq.End = nil
	e.runTailLoop(ctx, tailReq)
}

func (e *Engine) runTailLoop(ctx context.Context, req RunRequest) {
	ticker := time.NewTicker(e.cfg.ResolverInterval)
	defer ticker.Stop()
	for {
		e.setPhase(ModeTail, "tail")
		if _, err := e.Execute(ctx, req); err != nil {
			e.log.Warn("resolver tail run failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			_ = e.Close(context.Background())
			e.log.Info("resolver stopped")
			return
		case <-ticker.C:
		}
	}
}

func (e *Engine) runAutoLoop(ctx context.Context, req RunRequest) {
	nextTailAt := time.Now().UTC().Add(e.cfg.ResolverInterval)
	for {
		_, next, err := e.executeAutoCycle(ctx, req, nextTailAt)
		if err != nil {
			e.log.Warn("resolver auto cycle failed", zap.Error(err))
		}
		nextTailAt = next
		select {
		case <-ctx.Done():
			_ = e.Close(context.Background())
			e.log.Info("resolver stopped")
			return
		default:
		}
	}
}

func (e *Engine) executeAutoOnce(ctx context.Context, req RunRequest) (RunStats, error) {
	stats, _, err := e.executeAutoCycle(ctx, req, time.Now().UTC().Add(e.cfg.ResolverInterval))
	return stats, err
}

func (e *Engine) executeAutoCycle(ctx context.Context, req RunRequest, nextTailAt time.Time) (RunStats, time.Time, error) {
	req = e.boundedWindowRequest(req)
	watermark, dirtyDepth, err := e.scanAndEnqueueDirtyPartitions(ctx, req)
	if err != nil {
		return RunStats{}, nextTailAt, err
	}
	e.setAcceptedRawScanWatermark(watermark)
	e.setDirtyQueueDepth(dirtyDepth)

	now := time.Now().UTC()
	tailDue := !now.Before(nextTailAt)

	plan, err := e.bootstrapPlan(ctx, req)
	if err != nil {
		return RunStats{}, nextTailAt, err
	}
	e.setBootstrapBacklogDepth(uint64(len(plan.PendingPartitions)))
	// Keep one explicit scheduler order: visible backlog, then closed dirty-day
	// repair, then live tail. Tail preempts the historical lanes once due.
	phase := chooseAutoPhase(len(plan.PendingPartitions) > 0, dirtyDepth > 0, tailDue)
	if phase == "bootstrap_backlog" {
		e.setPhase(ModeAuto, phase)
		stats, _, err := e.executeHistoricalPartition(ctx, req, plan.PendingPartitions[0], "bootstrap_backlog")
		if err != nil {
			return stats, nextTailAt, err
		}
		if time.Now().UTC().Before(nextTailAt) {
			return stats, nextTailAt, nil
		}
		tailDue = true
	}

	if chooseAutoPhase(false, dirtyDepth > 0, tailDue) == "historical_repair" {
		part, ok, listErr := e.repo.nextPendingDirtyPartition(ctx, req.Org, req.ExcludedOrgPrefixes)
		if listErr != nil {
			return RunStats{}, nextTailAt, listErr
		}
		if ok && !dirtyPartitionReady(part, time.Now().UTC(), e.cfg.ResolverDirtyQuietPeriod) {
			e.setPhase(ModeAuto, "historical_repair_wait")
			ok = false
		}
		if ok {
			phase = "historical_repair"
			e.setPhase(ModeAuto, phase)
			stats, err := e.executeDirtyHistoricalRepair(ctx, req, part)
			if err != nil {
				return stats, nextTailAt, err
			}
			if time.Now().UTC().Before(nextTailAt) {
				return stats, nextTailAt, nil
			}
			tailDue = true
		}
	}

	if tailDue {
		phase = "tail"
		e.setPhase(ModeAuto, phase)
		tailReq := req
		tailReq.Mode = ModeTail
		tailReq.Start = nil
		tailReq.End = nil
		stats, _, err := e.executeWindow(ctx, e.boundedWindowRequest(tailReq))
		if err != nil {
			return stats, nextTailAt, err
		}
		nextTailAt = time.Now().UTC().Add(e.cfg.ResolverInterval)
		return stats, nextTailAt, nil
	}

	e.setPhase(ModeAuto, phase)
	if req.DryRun {
		return RunStats{}, nextTailAt, nil
	}
	timer := time.NewTimer(e.cfg.ResolverInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return RunStats{}, nextTailAt, ctx.Err()
	case <-timer.C:
	}
	return RunStats{}, time.Now().UTC(), nil
}

func dirtyPartitionReady(part dirtyPartition, now time.Time, quietPeriod time.Duration) bool {
	if quietPeriod <= 0 {
		return true
	}
	return !part.LastDirtyAt.UTC().After(now.UTC().Add(-quietPeriod))
}

func (e *Engine) Close(ctx context.Context) error {
	var errs []error
	if e.httpSrv != nil {
		if err := e.httpSrv.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
			errs = append(errs, err)
		}
	}
	if e.repo != nil {
		if err := e.repo.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func (e *Engine) Execute(ctx context.Context, req RunRequest) (RunStats, error) {
	if req.Mode == "" {
		req.Mode = parseMode(e.cfg.ResolverMode)
	}
	switch req.Mode {
	case ModeAuto:
		return e.executeAutoOnce(ctx, req)
	case ModeBootstrap:
		return e.executeBootstrap(ctx, req)
	case ModeBackfill:
		return e.executeBackfill(ctx, req)
	default:
		stats, _, err := e.executeWindow(ctx, e.boundedWindowRequest(req))
		return stats, err
	}
}

func (e *Engine) boundedWindowRequest(req RunRequest) RunRequest {
	if req.Mode == ModeTail && (req.Start == nil || req.End == nil) {
		now := time.Now().UTC()
		start := now.Add(-e.cfg.ResolverLatenessWindow)
		end := now
		req.Start = &start
		req.End = &end
	}
	if req.Step == 0 {
		req.Step = 24 * time.Hour
	}
	return req
}

func (e *Engine) executeBackfill(ctx context.Context, req RunRequest) (RunStats, error) {
	req = e.boundedWindowRequest(req)
	partitions, err := e.repo.listBackfillPartitions(ctx, req)
	if err != nil {
		return RunStats{}, err
	}
	return e.executePartitions(ctx, req, partitions, ModeBackfill)
}

func (e *Engine) executeBootstrap(ctx context.Context, req RunRequest) (RunStats, error) {
	if req.Step == 0 {
		req.Step = 24 * time.Hour
	}
	var total RunStats
	for {
		plan, err := e.bootstrapPlan(ctx, req)
		if err != nil {
			return total, err
		}
		if plan.MaxRaw == nil {
			e.log.Info("resolver bootstrap waiting for accepted raw events")
			if err := e.waitBootstrapInterval(ctx); err != nil {
				return total, err
			}
			continue
		}
		if len(plan.PendingPartitions) == 0 {
			if plan.CaughtUp {
				return total, nil
			}
			e.log.Info("resolver bootstrap waiting for more closed partitions",
				zap.Time("replay_frontier", plan.MaxRaw.UTC()),
				zap.Time("historical_end", plan.HistoricalEnd.UTC()),
			)
			if err := e.waitBootstrapInterval(ctx); err != nil {
				return total, err
			}
			continue
		}
		e.log.Info("resolver bootstrap processing partitions",
			zap.Int("partitions", len(plan.PendingPartitions)),
			zap.Time("historical_start", plan.PendingPartitions[0].Start.UTC()),
			zap.Time("historical_end", plan.HistoricalEnd.UTC()),
		)
		e.setBootstrapBacklogDepth(uint64(len(plan.PendingPartitions)))
		stats, err := e.executePartitions(ctx, req, plan.PendingPartitions, ModeBootstrap)
		if err != nil {
			return total, err
		}
		total.SelectionEvents += stats.SelectionEvents
		total.CapabilityVersions += stats.CapabilityVersions
		total.CapabilityIntervals += stats.CapabilityIntervals
		total.Decisions += stats.Decisions
		total.SessionRows += stats.SessionRows
		total.StatusHourRows += stats.StatusHourRows
		total.AIBatchJobRows += stats.AIBatchJobRows
		total.BYOCJobRows += stats.BYOCJobRows
	}
}

type bootstrapPlan struct {
	MaxRaw            *time.Time
	HistoricalEnd     time.Time
	CaughtUp          bool
	PendingPartitions []backfillPartition
}

func (e *Engine) scanAndEnqueueDirtyPartitions(ctx context.Context, req RunRequest) (*dirtyScanWatermark, uint64, error) {
	cutoff := truncateUTCDate(time.Now().UTC().Add(-e.cfg.ResolverLatenessWindow))
	stateKey := dirtyScanStateKey(req.Org, req.ExcludedOrgPrefixes)
	currentWatermark, err := e.repo.dirtyScanWatermark(ctx, stateKey)
	if err != nil {
		return nil, 0, err
	}
	partitions, nextWatermark, err := e.repo.scanDirtyAcceptedRawPartitions(ctx, req.Org, req.ExcludedOrgPrefixes, cutoff, currentWatermark)
	if err != nil {
		return currentWatermark, 0, err
	}
	// Persist a tuple watermark, not just a timestamp, so accepted raw rows with
	// identical ingested_at values cannot be skipped across scheduler loops.
	if nextWatermark != nil && !req.DryRun {
		if err := e.repo.upsertDirtyScanWatermark(ctx, stateKey, *nextWatermark); err != nil {
			return currentWatermark, 0, err
		}
	}
	if nextWatermark != nil {
		currentWatermark = nextWatermark
	}
	if len(partitions) > 0 && !req.DryRun {
		enqueued, enqueueErr := e.repo.enqueueDirtyPartitions(ctx, partitions, time.Now().UTC())
		if enqueueErr != nil {
			return currentWatermark, 0, enqueueErr
		}
		resolverDirtyPartitionsEnqueued.Add(float64(enqueued))
	}
	depth, err := e.repo.pendingDirtyPartitionCount(ctx, req.Org, req.ExcludedOrgPrefixes)
	if err != nil {
		return currentWatermark, 0, err
	}
	return currentWatermark, depth, nil
}

func (e *Engine) bootstrapPlan(ctx context.Context, req RunRequest) (bootstrapPlan, error) {
	minRaw, maxRaw, err := e.repo.acceptedRawBounds(ctx, req.Org, req.ExcludedOrgPrefixes)
	if err != nil {
		return bootstrapPlan{}, err
	}
	if maxRaw == nil || minRaw == nil {
		return bootstrapPlan{}, nil
	}
	now, err := e.repo.currentUTC(ctx)
	if err != nil {
		return bootstrapPlan{}, err
	}
	start, historicalEnd, caughtUp := deriveBootstrapWindow(*minRaw, *maxRaw, now, e.cfg.ResolverLatenessWindow, req.Start, req.End)
	plan := bootstrapPlan{
		MaxRaw:        maxRaw,
		HistoricalEnd: historicalEnd,
		CaughtUp:      caughtUp,
	}
	if !historicalEnd.After(start) {
		return plan, nil
	}
	partitions, err := e.repo.listBackfillPartitions(ctx, RunRequest{
		Mode:                ModeBackfill,
		Org:                 req.Org,
		ExcludedOrgPrefixes: req.ExcludedOrgPrefixes,
		Start:               &start,
		End:                 &historicalEnd,
		Step:                req.Step,
	})
	if err != nil {
		return bootstrapPlan{}, err
	}
	successes, err := e.repo.successfulBackfillPartitions(ctx, req.Org, req.ExcludedOrgPrefixes)
	if err != nil {
		return bootstrapPlan{}, err
	}
	plan.PendingPartitions = filterPendingPartitions(partitions, successes)
	return plan, nil
}

func (e *Engine) waitBootstrapInterval(ctx context.Context) error {
	timer := time.NewTimer(e.cfg.ResolverInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func deriveBootstrapWindow(minRaw, maxRaw, now time.Time, lateness time.Duration, requestedStart, requestedEnd *time.Time) (time.Time, time.Time, bool) {
	start := minRaw.UTC()
	if requestedStart != nil && requestedStart.UTC().After(start) {
		start = requestedStart.UTC()
	}
	cutoff := now.UTC().Add(-lateness)
	replayFrontier := maxRaw.UTC()
	if replayFrontier.After(cutoff) {
		replayFrontier = cutoff
	}
	historicalEnd := truncateUTCDate(replayFrontier)
	caughtUp := !maxRaw.UTC().Before(cutoff)
	if requestedEnd != nil {
		targetEnd := requestedEnd.UTC()
		if historicalEnd.After(targetEnd) {
			historicalEnd = targetEnd
		}
		if !historicalEnd.Before(targetEnd) {
			caughtUp = true
		}
	}
	return start, historicalEnd, caughtUp
}

func truncateUTCDate(ts time.Time) time.Time {
	return time.Date(ts.UTC().Year(), ts.UTC().Month(), ts.UTC().Day(), 0, 0, 0, 0, time.UTC)
}

func dirtyScanStateKey(org string, excludedPrefixes []string) string {
	prefixes := append([]string{}, excludedPrefixes...)
	sort.Strings(prefixes)
	return stableHash("dirty_scan_watermark", org, strings.Join(prefixes, ","))
}

func chooseAutoPhase(backlogPending, dirtyPending, tailDue bool) string {
	switch {
	case backlogPending && !tailDue:
		return "bootstrap_backlog"
	case dirtyPending && !tailDue:
		return "historical_repair"
	case tailDue:
		return "tail"
	default:
		return "idle"
	}
}

func filterPendingPartitions(parts []backfillPartition, successes map[string]struct{}) []backfillPartition {
	out := make([]backfillPartition, 0, len(parts))
	for _, part := range parts {
		key := part.Org + "|" + part.EventDate.UTC().Format("2006-01-02")
		if _, ok := successes[key]; ok {
			continue
		}
		out = append(out, part)
	}
	return out
}

func (e *Engine) executePartitions(ctx context.Context, req RunRequest, partitions []backfillPartition, mode Mode) (RunStats, error) {
	var total RunStats
	for _, part := range partitions {
		stats, runStatus, err := e.executeHistoricalPartition(ctx, req.withMode(mode), part, "bootstrap_backlog")
		if err != nil {
			return total, err
		}
		if runStatus == "skipped_claimed" {
			continue
		}
		total.SelectionEvents += stats.SelectionEvents
		total.CapabilityVersions += stats.CapabilityVersions
		total.CapabilityIntervals += stats.CapabilityIntervals
		total.Decisions += stats.Decisions
		total.SessionRows += stats.SessionRows
		total.StatusHourRows += stats.StatusHourRows
		total.AIBatchJobRows += stats.AIBatchJobRows
		total.BYOCJobRows += stats.BYOCJobRows
	}
	return total, nil
}

func (e *Engine) executeHistoricalPartition(ctx context.Context, req RunRequest, part backfillPartition, phase string) (RunStats, string, error) {
	partReq := RunRequest{
		Mode:                req.Mode,
		Org:                 part.Org,
		ExcludedOrgPrefixes: req.ExcludedOrgPrefixes,
		Start:               &part.Start,
		End:                 &part.End,
		Step:                req.Step,
		DryRun:              req.DryRun,
	}
	if phase != "" {
		e.setPhase(req.Mode, phase)
	}
	startedAt := time.Now().UTC()
	stats, runStatus, err := e.executeWindow(ctx, partReq)
	finishedAt := time.Now().UTC()
	errSummary := ""
	if err != nil {
		runStatus = "failed"
		errSummary = err.Error()
	}
	if !req.DryRun {
		_ = e.repo.recordBackfillPartition(ctx, stableHash(part.Org, part.EventDate.Format("2006-01-02"), phase, startedAt.Format(time.RFC3339Nano)), part, runStatus, uint64(stats.SelectionEvents+stats.Decisions+stats.SessionRows+stats.StatusHourRows), 0, errSummary, startedAt, finishedAt)
	}
	return stats, runStatus, err
}

func (r RunRequest) withMode(mode Mode) RunRequest {
	r.Mode = mode
	return r
}

func (e *Engine) executeDirtyHistoricalRepair(ctx context.Context, req RunRequest, part dirtyPartition) (RunStats, error) {
	claimed, err := e.repo.claimDirtyPartition(ctx, part.Org, part.EventDate, e.ownerID, e.cfg.ResolverClaimTTL)
	if err != nil {
		return RunStats{}, err
	}
	if !claimed {
		return RunStats{}, nil
	}
	eventDate := truncateUTCDate(part.EventDate)
	window := backfillPartition{
		Org:       part.Org,
		EventDate: eventDate,
		Start:     eventDate,
		End:       eventDate.Add(24 * time.Hour),
	}
	stats, runStatus, repairErr := e.executeHistoricalPartition(ctx, req.withMode(ModeAuto), window, "historical_repair")
	if repairErr != nil {
		_ = e.repo.failDirtyPartition(context.Background(), part.Org, eventDate, e.ownerID, repairErr.Error())
		return stats, repairErr
	}
	if runStatus == "skipped_claimed" {
		_ = e.repo.releaseDirtyPartition(context.Background(), part.Org, eventDate, e.ownerID)
		return RunStats{}, nil
	}
	if err := e.repo.completeDirtyPartition(ctx, part.Org, eventDate, e.ownerID); err != nil {
		return stats, err
	}
	resolverDirtyPartitionsRepaired.Inc()
	return stats, nil
}

func (e *Engine) executeWindow(ctx context.Context, req RunRequest) (stats RunStats, runStatus string, err error) {
	runStatus = "success"
	startedAt := time.Now().UTC()
	runID := stableHash(string(req.Mode), req.Org, windowRangeLabel(req.Start, req.End), startedAt.Format(time.RFC3339Nano))
	errSummary := ""
	logStage := func(stage string, stageStarted time.Time, fields ...zap.Field) {
		fields = append(fields,
			zap.String("stage", stage),
			zap.Duration("elapsed", time.Since(stageStarted)),
			zap.String("mode", string(req.Mode)),
			zap.String("org", req.Org),
		)
		e.log.Info("resolver stage completed", fields...)
	}
	if !req.DryRun {
		defer func() {
			finishedAt := time.Now().UTC()
			rowsProcessed := uint64(stats.SelectionEvents + stats.CapabilityVersions + stats.CapabilityIntervals + stats.Decisions + stats.SessionRows + stats.StatusHourRows + stats.AIBatchJobRows + stats.BYOCJobRows)
			if err != nil {
				runStatus = "failed"
				errSummary = err.Error()
			}
			_ = e.repo.recordRun(ctx, runID, req, runStatus, startedAt, finishedAt, rowsProcessed, 0, errSummary)
			resolverRunsTotal.WithLabelValues(string(req.Mode), runStatus).Inc()
			resolverRunDuration.WithLabelValues(string(req.Mode)).Observe(finishedAt.Sub(startedAt).Seconds())
			if req.End != nil {
				resolverWatermarkUnix.Set(float64(req.End.UTC().Unix()))
			}
		}()
	}

	if req.Start == nil || req.End == nil {
		return stats, runStatus, fmt.Errorf("resolver window requires start and end")
	}

	spec := WindowSpec{
		Org:                 req.Org,
		ExcludedOrgPrefixes: req.ExcludedOrgPrefixes,
		Start:               req.Start,
		End:                 req.End,
	}
	if req.Mutates() {
		releaseClaim, acquired, skipSummary, claimErr := e.tryClaimWindow(ctx, req)
		if claimErr != nil {
			return stats, runStatus, claimErr
		}
		if !acquired {
			resolverClaimConflicts.WithLabelValues(string(req.Mode)).Inc()
			runStatus = "skipped_claimed"
			errSummary = skipSummary
			e.log.Info("resolver window skipped due to active overlapping claim",
				zap.String("mode", string(req.Mode)),
				zap.String("org", req.Org),
				zap.Time("window_start", req.Start.UTC()),
				zap.Time("window_end", req.End.UTC()),
				zap.String("reason", skipSummary),
			)
			return stats, runStatus, nil
		}
		defer releaseClaim()
	}
	stageStarted := time.Now()
	includeCapabilityRepair := req.Mode == ModeTail || req.Mode == ModeRepairWindow
	touched, err := e.repo.fetchTouchedSessionKeys(ctx, spec, includeCapabilityRepair)
	if err != nil {
		return stats, runStatus, err
	}
	logStage("fetch_touched_session_keys", stageStarted, zap.Int("sessions", len(touched)))

	stageStarted = time.Now()
	selectionCandidates, err := e.repo.fetchSelectionCandidates(ctx, spec)
	if err != nil {
		return stats, runStatus, err
	}
	logStage("fetch_selection_candidates", stageStarted, zap.Int("selection_candidates", len(selectionCandidates)))

	stageStarted = time.Now()
	selectionEvents := buildSelectionEvents(selectionCandidates)
	stats.SelectionEvents = len(selectionEvents)
	resolverSelectionEventsProcessed.Add(float64(len(selectionEvents)))
	logStage("build_selection_events", stageStarted, zap.Int("selection_events", len(selectionEvents)))

	stageStarted = time.Now()
	identities := collectSelectionIdentities(selectionEvents)
	snapshots, err := e.repo.fetchCapabilitySnapshots(ctx, spec, identities)
	if err != nil {
		return stats, runStatus, err
	}
	logStage("fetch_capability_snapshots", stageStarted,
		zap.Int("identities", len(identities)),
		zap.Int("snapshots", len(snapshots)),
	)

	stageStarted = time.Now()
	versions := buildCapabilityVersions(snapshots)
	intervals := buildCapabilityIntervals(versions)
	stats.CapabilityVersions = len(versions)
	stats.CapabilityIntervals = len(intervals)
	logStage("build_capability_intervals", stageStarted,
		zap.Int("capability_versions", len(versions)),
		zap.Int("capability_intervals", len(intervals)),
	)

	stageStarted = time.Now()
	decisions := resolveSelectionDecisions(selectionEvents, intervals)
	stats.Decisions = len(decisions)
	for _, row := range decisions {
		resolverAttributionDecisions.WithLabelValues(row.Status).Inc()
	}
	logStage("resolve_selection_decisions", stageStarted, zap.Int("decisions", len(decisions)))

	if req.Mutates() {
		stageStarted = time.Now()
		if err := e.repo.insertSelectionEvents(ctx, runID, selectionEvents); err != nil {
			return stats, runStatus, err
		}
		if err := e.repo.insertCapabilityVersions(ctx, runID, versions); err != nil {
			return stats, runStatus, err
		}
		if err := e.repo.insertCapabilityIntervals(ctx, runID, intervals); err != nil {
			return stats, runStatus, err
		}
		prevHashes, hashErr := e.repo.fetchCurrentDecisionHashes(ctx, selectionEventIDs(selectionEvents))
		if hashErr != nil {
			return stats, runStatus, hashErr
		}
		if err := e.repo.insertDecisionRows(ctx, runID, decisions, prevHashes); err != nil {
			return stats, runStatus, err
		}
		logStage("persist_selection_state", stageStarted,
			zap.Int("selection_events", len(selectionEvents)),
			zap.Int("capability_versions", len(versions)),
			zap.Int("capability_intervals", len(intervals)),
			zap.Int("decisions", len(decisions)),
		)
	}

	sessionRefs := sessionKeyRefsFromMap(touched)
	stageStarted = time.Now()
	evidence, err := e.repo.fetchSessionEvidence(ctx, sessionRefs)
	if err != nil {
		return stats, runStatus, err
	}
	latestDecisions := decisionsByLatestSession(selectionEvents, decisions)
	if len(latestDecisions) < len(sessionRefs) {
		existing, fetchErr := e.repo.fetchLatestSessionDecisions(ctx, sessionRefs)
		if fetchErr != nil {
			return stats, runStatus, fetchErr
		}
		for key, row := range existing {
			if _, ok := latestDecisions[key]; !ok {
				latestDecisions[key] = row
			}
		}
	}
	logStage("build_session_inputs", stageStarted,
		zap.Int("session_refs", len(sessionRefs)),
		zap.Int("session_evidence_rows", len(evidence)),
		zap.Int("latest_decisions", len(latestDecisions)),
	)

	stageStarted = time.Now()
	sessionRows := buildSessionCurrentRows(evidence, latestDecisions)
	stats.SessionRows = len(sessionRows)
	statusEvidence, err := e.repo.fetchStatusHourEvidence(ctx, sessionRefs, spec)
	if err != nil {
		return stats, runStatus, err
	}
	statusRows := filterOwnedStatusHourRows(buildStatusHourRows(statusEvidence, indexSessionRows(sessionRows)), spec)
	stats.StatusHourRows = len(statusRows)
	logStage("build_current_rows", stageStarted,
		zap.Int("session_rows", len(sessionRows)),
		zap.Int("status_evidence_rows", len(statusEvidence)),
		zap.Int("status_hour_rows", len(statusRows)),
	)

	// Compute all downstream attribution phases even in verify/dry-run mode.
	// Non-mutating runs must exercise the full resolver graph and skip only the
	// final inserts/publication steps.
	stageStarted = time.Now()
	aiBatchRows, err := e.executeAIBatchAttribution(ctx, spec, req, runID)
	if err != nil {
		return stats, runStatus, err
	}
	stats.AIBatchJobRows = aiBatchRows
	logStage("ai_batch_attribution", stageStarted,
		zap.Int("ai_batch_job_rows", aiBatchRows),
	)

	// BYOC attribution phase — independent from AI batch and live V2V.
	stageStarted = time.Now()
	byocRows, err := e.executeBYOCAttribution(ctx, spec, req, runID)
	if err != nil {
		return stats, runStatus, err
	}
	stats.BYOCJobRows = byocRows
	logStage("byoc_attribution", stageStarted,
		zap.Int("byoc_job_rows", byocRows),
	)

	if !req.Mutates() {
		return stats, runStatus, nil
	}

	stageStarted = time.Now()
	currentSessionHashes, err := e.repo.fetchCurrentSessionRowHashes(ctx, sessionRefs)
	if err != nil {
		return stats, runStatus, err
	}
	currentStatusHourHashes, err := e.repo.fetchCurrentStatusHourRowHashes(ctx, sessionRefs, statusRows)
	if err != nil {
		return stats, runStatus, err
	}
	changedSessionRows := filterChangedSessionRows(sessionRows, currentSessionHashes)
	changedStatusRows := filterChangedStatusHourRows(statusRows, currentStatusHourHashes)
	logStage("filter_changed_current_rows", stageStarted,
		zap.Int("session_rows", len(sessionRows)),
		zap.Int("changed_session_rows", len(changedSessionRows)),
		zap.Int("status_hour_rows", len(statusRows)),
		zap.Int("changed_status_hour_rows", len(changedStatusRows)),
	)

	stageStarted = time.Now()
	if err := e.repo.insertSessionCurrentRows(ctx, runID, changedSessionRows); err != nil {
		return stats, runStatus, err
	}
	if err := e.repo.insertStatusHourRows(ctx, runID, changedStatusRows); err != nil {
		return stats, runStatus, err
	}
	if err := e.repo.insertSessionDemandInputRows(ctx, runID, changedSessionRows); err != nil {
		return stats, runStatus, err
	}
	logStage("persist_current_rows", stageStarted,
		zap.Int("session_rows", len(changedSessionRows)),
		zap.Int("status_hour_rows", len(changedStatusRows)),
	)

	stageStarted = time.Now()
	windowSlices := collectWindowSlices(changedSessionRows, changedStatusRows)
	if err := e.repo.publishServingRollups(ctx, runID, windowSlices); err != nil {
		return stats, runStatus, err
	}
	e.setTailWatermark(req.End)
	logStage("publish_serving_rollups", stageStarted,
		zap.Int("window_slices", len(windowSlices)),
	)

	return stats, runStatus, nil
}

// executeAIBatchAttribution attributes AI batch jobs in the given window.
// It fetches candidates, builds an independent capability snapshot set keyed
// by orch URI (the only identity available in batch events), resolves
// attribution decisions, and writes the results to canonical_ai_batch_job_store.
func (e *Engine) executeAIBatchAttribution(ctx context.Context, spec WindowSpec, req RunRequest, runID string) (int, error) {
	jobs, err := e.repo.fetchAIBatchJobCandidates(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("fetch ai batch job candidates: %w", err)
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	identities := aiBatchIdentities(jobs)
	identityStrings := orchIdentitiesToStrings(identities)
	snapshots, err := e.repo.fetchCapabilitySnapshots(ctx, spec, identityStrings)
	if err != nil {
		return 0, fmt.Errorf("fetch ai batch capability snapshots: %w", err)
	}

	versions := buildCapabilityVersions(snapshots)
	intervals := buildCapabilityIntervals(versions)

	selections := make([]SelectionEvent, 0, len(jobs))
	for _, j := range jobs {
		selections = append(selections, j.toSelectionEvent())
	}
	decisions := resolveSelectionDecisions(selections, intervals)

	decisionsByID := make(map[string]SelectionDecision, len(decisions))
	for _, d := range decisions {
		decisionsByID[d.SelectionEventID] = d
	}

	rows := buildAIBatchJobRows(jobs, decisionsByID)

	if !req.Mutates() {
		return len(rows), nil
	}
	if err := e.repo.insertAIBatchJobRows(ctx, runID, rows); err != nil {
		return 0, fmt.Errorf("insert ai batch job rows: %w", err)
	}
	return len(rows), nil
}

// executeBYOCAttribution attributes BYOC jobs in the given window.
// It fetches job candidates, builds an independent capability snapshot set
// keyed by both orch address and URI, resolves worker lifecycle model data,
// resolves attribution decisions, and writes results to canonical_byoc_job_store.
func (e *Engine) executeBYOCAttribution(ctx context.Context, spec WindowSpec, req RunRequest, runID string) (int, error) {
	jobs, err := e.repo.fetchBYOCJobCandidates(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("fetch byoc job candidates: %w", err)
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	identities := byocIdentities(jobs)
	identityStrings := orchIdentitiesToStrings(identities)
	snapshots, err := e.repo.fetchCapabilitySnapshots(ctx, spec, identityStrings)
	if err != nil {
		return 0, fmt.Errorf("fetch byoc capability snapshots: %w", err)
	}

	versions := buildCapabilityVersions(snapshots)
	intervals := buildCapabilityIntervals(versions)

	// Collect orch addresses for worker lifecycle lookup.
	orchAddresses := make([]string, 0, len(identities))
	for _, id := range identities {
		if id.Address != "" {
			orchAddresses = append(orchAddresses, id.Address)
		}
	}
	workerSnapshots, err := e.repo.fetchWorkerLifecycleSnapshots(ctx, spec, orchAddresses)
	if err != nil {
		return 0, fmt.Errorf("fetch worker lifecycle snapshots: %w", err)
	}
	workerModels := resolveWorkerModels(jobs, workerSnapshots)

	selections := make([]SelectionEvent, 0, len(jobs))
	for _, j := range jobs {
		selections = append(selections, j.toSelectionEventWithModelHint(workerModels[j.EventID].Model))
	}
	decisions := resolveSelectionDecisions(selections, intervals)

	decisionsByID := make(map[string]SelectionDecision, len(decisions))
	for _, d := range decisions {
		decisionsByID[d.SelectionEventID] = d
	}

	rows := buildBYOCJobRows(jobs, decisionsByID, workerModels)

	if !req.Mutates() {
		return len(rows), nil
	}
	if err := e.repo.insertBYOCJobRows(ctx, runID, rows); err != nil {
		return 0, fmt.Errorf("insert byoc job rows: %w", err)
	}
	return len(rows), nil
}

func parseMode(raw string) Mode {
	switch Mode(raw) {
	case ModeAuto, ModeBootstrap, ModeBackfill, ModeRepairWindow, ModeVerify:
		return Mode(raw)
	default:
		return ModeTail
	}
}

func (e *Engine) tryClaimWindow(ctx context.Context, req RunRequest) (func(), bool, string, error) {
	if !req.Mutates() {
		return func() {}, true, "", nil
	}
	now := time.Now().UTC()
	claim := windowClaim{
		ClaimKey:       stableHash("mutating_window", e.ownerID, string(req.Mode), req.Org, req.Start.UTC().Format(time.RFC3339Nano), req.End.UTC().Format(time.RFC3339Nano)),
		ClaimType:      "mutating_window",
		Mode:           string(req.Mode),
		Org:            req.Org,
		OwnerID:        e.ownerID,
		WindowStart:    req.Start.UTC(),
		WindowEnd:      req.End.UTC(),
		LeaseExpiresAt: now.Add(e.cfg.ResolverClaimTTL),
		CreatedAt:      now,
	}
	if err := e.repo.insertWindowClaim(ctx, claim); err != nil {
		return nil, false, "", fmt.Errorf("insert resolver window claim: %w", err)
	}
	activeClaims, err := e.repo.activeOverlappingWindowClaims(ctx, claim)
	if err != nil {
		return nil, false, "", err
	}
	winner := pickWinningWindowClaim(activeClaims)
	if winner.ClaimKey != claim.ClaimKey {
		releasedAt := time.Now().UTC()
		claim.ReleasedAt = &releasedAt
		claim.LeaseExpiresAt = releasedAt
		_ = e.repo.insertWindowClaim(context.Background(), claim)
		return func() {}, false, windowClaimSummary(winner), nil
	}
	e.setActiveClaim(&claim)
	stop := make(chan struct{})
	go e.keepWindowClaimAlive(stop, claim)
	release := func() {
		close(stop)
		e.setActiveClaim(nil)
		releasedAt := time.Now().UTC()
		claim.ReleasedAt = &releasedAt
		claim.LeaseExpiresAt = releasedAt
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.repo.insertWindowClaim(releaseCtx, claim); err != nil {
			e.log.Warn("release resolver window claim failed",
				zap.Error(err),
				zap.String("mode", string(req.Mode)),
				zap.String("org", req.Org),
				zap.Time("window_start", req.Start.UTC()),
				zap.Time("window_end", req.End.UTC()),
			)
		}
	}
	return release, true, "", nil
}

func (e *Engine) keepWindowClaimAlive(stop <-chan struct{}, claim windowClaim) {
	ttl := e.cfg.ResolverClaimTTL
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	interval := ttl / 2
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			claim.LeaseExpiresAt = time.Now().UTC().Add(ttl)
			refreshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := e.repo.insertWindowClaim(refreshCtx, claim)
			cancel()
			if err != nil {
				e.log.Warn("renew resolver window claim failed",
					zap.Error(err),
					zap.String("mode", claim.Mode),
					zap.String("org", claim.Org),
					zap.Time("window_start", claim.WindowStart.UTC()),
					zap.Time("window_end", claim.WindowEnd.UTC()),
				)
			}
		}
	}
}

func pickWinningWindowClaim(claims []windowClaim) windowClaim {
	if len(claims) == 0 {
		return windowClaim{}
	}
	winner := claims[0]
	for _, claim := range claims[1:] {
		if claim.CreatedAt.Before(winner.CreatedAt) {
			winner = claim
			continue
		}
		if claim.CreatedAt.After(winner.CreatedAt) {
			continue
		}
		if claim.OwnerID < winner.OwnerID {
			winner = claim
			continue
		}
		if claim.OwnerID == winner.OwnerID && claim.ClaimKey < winner.ClaimKey {
			winner = claim
		}
	}
	return winner
}

func windowClaimSummary(claim windowClaim) string {
	return fmt.Sprintf(
		"owner=%s mode=%s org=%s window=[%s,%s)",
		claim.OwnerID,
		claim.Mode,
		claim.Org,
		claim.WindowStart.UTC().Format(time.RFC3339),
		claim.WindowEnd.UTC().Format(time.RFC3339),
	)
}

func collectSelectionIdentities(rows []SelectionEvent) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(rows)*2)
	for _, row := range rows {
		if id := normalizeAddress(row.ObservedAddress); id != "" {
			if _, ok := seen[id]; ok {
			} else {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		if id := normalizeURL(row.ObservedURL); id != "" {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func selectionEventIDs(rows []SelectionEvent) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.ID)
	}
	return out
}

func decisionsByLatestSession(selections []SelectionEvent, rows []SelectionDecision) map[string]SelectionDecision {
	selectionByID := make(map[string]SelectionEvent, len(selections))
	identityCountBySession := make(map[string]map[string]struct{}, len(selections))
	for _, row := range selections {
		selectionByID[row.ID] = row
		identity := candidateIdentity(row.ObservedAddress, row.ObservedURL)
		if identity == "" {
			continue
		}
		if _, ok := identityCountBySession[row.SessionKey]; !ok {
			identityCountBySession[row.SessionKey] = make(map[string]struct{})
		}
		identityCountBySession[row.SessionKey][identity] = struct{}{}
	}

	out := make(map[string]SelectionDecision, len(rows))
	for _, row := range rows {
		current, ok := out[row.SessionKey]
		if !ok || row.SelectionTS.After(current.SelectionTS) {
			out[row.SessionKey] = row
		}
	}
	for sessionKey, decision := range out {
		selection := selectionByID[decision.SelectionEventID]
		latestIdentity := candidateIdentity(selection.ObservedAddress, selection.ObservedURL)
		identities := identityCountBySession[sessionKey]
		if decision.Status == "unresolved" &&
			decision.Reason == "missing_candidate" &&
			latestIdentity != "" &&
			len(identities) > 1 {
			decision.Status = "ambiguous"
			decision.Reason = "ambiguous_candidates"
			decision.Method = "ambiguous"
			decision.Confidence = "low"
			out[sessionKey] = decision
		}
	}
	return out
}

func sessionKeyRefsFromMap(values map[string]string) []sessionKeyRef {
	out := make([]sessionKeyRef, 0, len(values))
	for key, org := range values {
		out = append(out, sessionKeyRef{Org: org, SessionKey: key})
	}
	return out
}

func filterChangedSessionRows(rows []SessionCurrentRow, previous map[string]string) []SessionCurrentRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]SessionCurrentRow, 0, len(rows))
	for _, row := range rows {
		if previous[row.SessionKey] == sessionCurrentRowHash(row) {
			continue
		}
		out = append(out, row)
	}
	return out
}

func filterChangedStatusHourRows(rows []StatusHourRow, previous map[string]string) []StatusHourRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]StatusHourRow, 0, len(rows))
	for _, row := range rows {
		key := stableHash(row.SessionKey, row.Hour.UTC().Format(time.RFC3339))
		if previous[key] == statusHourRowHash(row) {
			continue
		}
		out = append(out, row)
	}
	return out
}
