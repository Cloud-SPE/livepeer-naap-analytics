package replay

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/resolver"
)

// ResolverConfig parameterises the resolver invocation the canonical phase
// issues. Fields follow the resolver's own CLI vocabulary so the harness
// reproduces the same pipeline shape a production backfill run uses, with
// one exception: Now is pinned, not wall clock.
type ResolverConfig struct {
	// Now is the frozen wall clock the resolver uses for every persistent
	// row-level timestamp. Determinism hinges on this.
	Now time.Time

	// WindowStart / WindowEnd bound the backfill window. Defaults to the
	// fixture manifest's [window_start, window_end] if both are zero.
	WindowStart time.Time
	WindowEnd   time.Time

	// Step controls the backfill partition size. Default 24h.
	Step time.Duration

	// ClickHouse connection overrides. The resolver builds its own
	// connection via config.Load(); we pre-export these env vars so that
	// connection matches the harness's own ClickHouse creds. Empty fields
	// fall through to whatever .env / shell environment already provides.
	ClickHouseAddr     string // native-protocol host:port
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
}

// RunResolverBackfill builds a fresh resolver Engine from env-loaded config
// and runs a backfill over the configured window with a pinned Now. The
// harness truncates resolver_* bookkeeping + canonical_*_store before this
// call, so the Engine sees a clean slate.
//
// The resolver has its own ClickHouse connection (separate from the
// harness's conn, both read .env). Keep admin credentials in .env so the
// resolver can INSERT into canonical tables.
func RunResolverBackfill(ctx context.Context, log *zap.Logger, rc ResolverConfig) (resolver.RunStats, error) {
	// Align the resolver's config with the harness's connection. config.Load()
	// reads real env vars via envconfig, so overriding here is sufficient —
	// no code change in the config package needed.
	for key, val := range map[string]string{
		"CLICKHOUSE_ADDR":            rc.ClickHouseAddr,
		"CLICKHOUSE_DB":              rc.ClickHouseDB,
		"CLICKHOUSE_USER":            rc.ClickHouseUser,
		"CLICKHOUSE_PASSWORD":        rc.ClickHousePassword,
		"CLICKHOUSE_WRITER_USER":     rc.ClickHouseUser,
		"CLICKHOUSE_WRITER_PASSWORD": rc.ClickHousePassword,
	} {
		if val != "" {
			_ = os.Setenv(key, val)
		}
	}
	cfg, err := config.Load()
	if err != nil {
		return resolver.RunStats{}, fmt.Errorf("config.Load: %w", err)
	}
	// The resolver's own daemon flags are irrelevant for a one-shot
	// backfill; what matters is ResolverEnabled so the wiring can
	// construct the repo, and the ClickHouse connection.
	cfg.ResolverEnabled = true
	cfg.ResolverMode = string(resolver.ModeBackfill)

	if log == nil {
		log = zap.NewNop()
	}
	engine, err := resolver.New(cfg, log)
	if err != nil {
		return resolver.RunStats{}, fmt.Errorf("resolver.New: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = engine.Close(closeCtx)
	}()

	start := rc.WindowStart
	end := rc.WindowEnd
	step := rc.Step
	if step == 0 {
		step = 24 * time.Hour
	}
	req := resolver.RunRequest{
		Mode:  resolver.ModeBackfill,
		Start: &start,
		End:   &end,
		Step:  step,
		Now:   rc.Now,
	}

	stats, err := engine.Execute(ctx, req)
	if err != nil {
		return stats, fmt.Errorf("resolver backfill: %w", err)
	}
	return stats, nil
}
