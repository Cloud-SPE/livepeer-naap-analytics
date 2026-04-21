package resolver

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
)

// PublishSLAForWindow runs the Phase 2 benchmark + final-scoring pass against
// whatever rows currently live in canonical_streaming_sla_input_hourly_store
// for the given (org, window_start) slices, using the exact same SQL the
// production resolver emits.
//
// Exists so validation harnesses that seed input rows directly can produce
// scored output in naap.api_hourly_streaming_sla_store without invoking the
// full resolver pipeline. Phase 5 retired the legacy api_base_* view chain,
// so this is the supported replacement for
// `INSERT INTO api_hourly_streaming_sla_store SELECT * FROM api_base_sla_compliance_scored_by_org`.
func PublishSLAForWindow(
	ctx context.Context,
	conn driver.Conn,
	log *zap.Logger,
	resolverVersion string,
	runID string,
	now time.Time,
	orgs []string,
	windowStart time.Time,
) error {
	if resolverVersion == "" {
		resolverVersion = "validation"
	}
	r := &repo{
		cfg:  &config.Config{ResolverVersion: resolverVersion},
		log:  log,
		conn: conn,
	}
	slices := make([]windowSliceRef, 0, len(orgs))
	for _, org := range orgs {
		slices = append(slices, windowSliceRef{Org: org, WindowStart: windowStart.UTC()})
	}
	queryID, err := r.stageWindowSlices(ctx, slices)
	if err != nil {
		return err
	}
	if err := r.insertSLABenchmarkDaily(ctx, runID, now); err != nil {
		return err
	}
	if err := r.insertFinalSLAComplianceRollups(ctx, runID, now, queryID); err != nil {
		return err
	}
	return nil
}
