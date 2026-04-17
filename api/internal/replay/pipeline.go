package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
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

	// PauseIngestion tells the harness to DETACH the ingest MVs before
	// doing any work and re-ATTACH them at the end, so live Kafka traffic
	// cannot contaminate the fixture state during the run. Default true;
	// callers can disable for environments that have no Kafka consumer
	// running (ephemeral CI).
	PauseIngestion bool
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
