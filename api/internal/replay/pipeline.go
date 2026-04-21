package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

// Config parameterises a harness run.
type Config struct {
	Database     string
	Layers       []Layer // which layers to execute + checksum
	FixturePath  string  // path to the .ndjson.zst archive
	ManifestPath string  // path to the .manifest.json

	// MVFlushTimeout is how long to wait for cascading materialized views
	// to propagate after the fixture is loaded. MVs complete synchronously
	// within the INSERT's HTTP response, so this is a small belt-and-braces
	// grace period only.
	MVFlushTimeout time.Duration

	// Loader carries the HTTP connection parameters the fixture loader
	// needs. The HTTP interface is used because cascading MV backpressure
	// deadlocks the native-protocol batch API (see loader.go doc comment).
	Loader LoadConfig

	// SkipLoad bypasses the TRUNCATE + fixture insert step and runs the
	// checksum phase against whatever state already exists in ClickHouse.
	// Dev-only; CI always runs a full reload.
	SkipLoad bool

	// SkipResolver bypasses the canonical-layer rebuild — the harness will
	// not truncate resolver_* / canonical_*_store or re-invoke the
	// resolver. Checksum phase still runs. Dev-only; CI always rebuilds.
	SkipResolver bool

	// SkipDBT bypasses the api-layer view refresh. The checksum phase
	// still runs against whatever view definitions already exist in
	// ClickHouse. Dev-only; CI always rebuilds.
	SkipDBT bool

	// DBT configures the docker-compose-based dbt invocation the api
	// phase issues. Only read when the layers list includes LayerAPI.
	DBT DBTConfig

	// PauseIngestion tells the harness to DETACH the ingest MVs before
	// doing any work and re-ATTACH them at the end, so live Kafka traffic
	// cannot contaminate the fixture state during the run. Default true;
	// callers can disable for environments that have no Kafka consumer
	// running (ephemeral CI).
	PauseIngestion bool

	// Resolver configures the canonical-layer phase. Only read when the
	// layers list includes LayerCanonical.
	Resolver ResolverConfig

	// Logger is used by the resolver invocation. If nil, a no-op logger
	// is substituted so the harness stays quiet by default.
	Logger *zap.Logger
}

// Run executes the configured layers in forward order and returns the
// aggregated report. Callers are responsible for persisting the report.
func Run(ctx context.Context, conn ch.Conn, cfg Config) (*Report, error) {
	if cfg.Database == "" {
		cfg.Database = "naap"
	}
	if cfg.MVFlushTimeout == 0 {
		cfg.MVFlushTimeout = 2 * time.Second
	}
	layers := cfg.Layers
	if len(layers) == 0 {
		layers = []Layer{LayerRaw, LayerNormalized}
	}

	manifest, err := LoadManifest(cfg.ManifestPath)
	if err != nil {
		return nil, err
	}

	report := &Report{
		FixtureName: manifest.Name,
		StartedAt:   time.Now().UTC(),
	}

	if cfg.PauseIngestion {
		if err := PauseIngestion(ctx, conn, cfg.Database); err != nil {
			return report, fmt.Errorf("pause ingestion: %w", err)
		}
		defer func() {
			// Use a detached context for resume — the caller's context may
			// be cancelled (Ctrl-C, timeout) and we still want to restore
			// ingestion before exiting.
			resumeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if resErr := ResumeIngestion(resumeCtx, conn, cfg.Database); resErr != nil {
				fmt.Printf("replay: WARNING: resume ingestion failed: %v\n", resErr)
			}
		}()
	}

	needsLoad := !cfg.SkipLoad && (contains(layers, LayerRaw) || contains(layers, LayerNormalized))
	if needsLoad {
		lc := cfg.Loader
		if lc.Database == "" {
			lc.Database = cfg.Database
		}
		inserted, err := LoadFixture(ctx, conn, lc, cfg.FixturePath, manifest)
		if err != nil {
			return report, fmt.Errorf("load fixture: %w", err)
		}
		if inserted == 0 {
			return report, fmt.Errorf("loaded 0 rows from %s", cfg.FixturePath)
		}
		// Give cascading MVs a beat to finalise before we read them.
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case <-time.After(cfg.MVFlushTimeout):
		}
	}

	// Canonical layer needs the resolver to run before we can checksum its
	// tables. Decoupled from SkipLoad so a developer can leave normalized
	// state in place and iterate on just the resolver path.
	if !cfg.SkipResolver && contains(layers, LayerCanonical) {
		if err := runCanonicalPhase(ctx, conn, cfg, manifest); err != nil {
			return report, fmt.Errorf("canonical phase: %w", err)
		}
	}

	// API layer refreshes view definitions via dbt before checksumming.
	// Views are views, so this does not move data — it catches model
	// changes on the branch that have not yet been dbt-run into CH.
	if !cfg.SkipDBT && contains(layers, LayerAPI) {
		log := cfg.Logger
		if log == nil {
			log = zap.NewNop()
		}
		if err := RunDBT(ctx, log, cfg.DBT); err != nil {
			return report, fmt.Errorf("api phase: %w", err)
		}
	}

	for _, layer := range layers {
		lr, err := runLayer(ctx, conn, cfg.Database, layer)
		if err != nil {
			return report, fmt.Errorf("layer %s: %w", layer, err)
		}
		report.Layers = append(report.Layers, lr)
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

// runCanonicalPhase truncates resolver bookkeeping + canonical tables,
// then invokes a resolver backfill pinned to cfg.Resolver.Now. The window
// defaults to the fixture manifest's [window_start, window_end] so the
// resolver processes exactly the raw events the fixture holds.
func runCanonicalPhase(ctx context.Context, conn ch.Conn, cfg Config, manifest *Manifest) error {
	if err := truncateResolverBookkeeping(ctx, conn, cfg.Database); err != nil {
		return fmt.Errorf("truncate resolver state: %w", err)
	}
	if err := truncateLayers(ctx, conn, cfg.Database, LayerCanonical); err != nil {
		return fmt.Errorf("truncate canonical layer: %w", err)
	}
	// api_*_store tables live under LayerAPI but the resolver writes them.
	// Truncate them here so a rerun does not accumulate rows on top of
	// prior resolver output.
	if err := truncateResolverWrittenAPIStores(ctx, conn, cfg.Database); err != nil {
		return fmt.Errorf("truncate resolver-written api stores: %w", err)
	}

	rc := cfg.Resolver
	if rc.WindowStart.IsZero() || rc.WindowEnd.IsZero() {
		winStart, winEnd, err := manifest.parseWindow()
		if err != nil {
			return fmt.Errorf("derive resolver window from manifest: %w", err)
		}
		if rc.WindowStart.IsZero() {
			rc.WindowStart = winStart
		}
		if rc.WindowEnd.IsZero() {
			rc.WindowEnd = winEnd
		}
	}
	if rc.Now.IsZero() {
		// Default: pin Now to the fixture window end. Any row-level
		// timestamp inside the run becomes "the moment the window closed",
		// which is the most semantically defensible choice for a replay.
		rc.Now = rc.WindowEnd
	}

	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	stats, err := RunResolverBackfill(ctx, log, rc)
	if err != nil {
		return err
	}
	log.Info("replay: resolver backfill complete",
		zap.Int("selection_events", stats.SelectionEvents),
		zap.Int("capability_versions", stats.CapabilityVersions),
		zap.Int("capability_intervals", stats.CapabilityIntervals),
		zap.Int("decisions", stats.Decisions),
		zap.Int("session_rows", stats.SessionRows),
		zap.Int("status_hour_rows", stats.StatusHourRows),
		zap.Int("ai_batch_job_rows", stats.AIBatchJobRows),
		zap.Int("byoc_job_rows", stats.BYOCJobRows),
	)
	return nil
}

func truncateResolverBookkeeping(ctx context.Context, conn ch.Conn, database string) error {
	for _, t := range resolverBookkeepingTables {
		stmt := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s", database, t)
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("truncate %s.%s: %w", database, t, err)
		}
	}
	return nil
}

func truncateResolverWrittenAPIStores(ctx context.Context, conn ch.Conn, database string) error {
	for _, t := range resolverWrittenApiStores {
		stmt := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s", database, t)
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("truncate %s.%s: %w", database, t, err)
		}
	}
	return nil
}

func runLayer(ctx context.Context, conn ch.Conn, database string, layer Layer) (LayerReport, error) {
	started := time.Now()
	tables := TablesForLayer(layer)
	sort.Strings(tables)
	lr := LayerReport{
		Layer:  layer,
		Tables: make(map[string]TableRollup, len(tables)),
	}
	for _, t := range tables {
		rollup, err := Checksum(ctx, conn, database, t)
		if err != nil {
			return lr, err
		}
		lr.Tables[t] = rollup
	}
	lr.DurationMS = time.Since(started).Milliseconds()
	return lr, nil
}

func contains(xs []Layer, x Layer) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
