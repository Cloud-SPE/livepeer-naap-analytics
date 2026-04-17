// Command replay drives the medallion pipeline over a pinned raw-event
// fixture and writes a per-layer artifact_checksum report. A second run
// over the same fixture must produce byte-identical rollups; any
// divergence points at the first layer that broke determinism.
//
// PR 1 scope: raw -> normalized. Later PRs extend to canonical and api.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/replay"
)

func main() {
	var (
		fixturePath = flag.String("fixture",
			"tests/fixtures/raw_events_golden.ndjson.zst",
			"path to the .ndjson.zst fixture archive")
		manifestPath = flag.String("manifest",
			"tests/fixtures/raw_events_golden.manifest.json",
			"path to the fixture manifest JSON")
		layerList = flag.String("layers",
			"raw,normalized",
			"comma-separated layers to execute (raw,normalized,canonical,api)")
		outputDir = flag.String("output",
			"target/replay",
			"directory for the per-run report JSON")
		compareTo = flag.String("compare-to", "",
			"optional path to a prior report JSON to diff against (fails on any divergence)")
		addr = flag.String("clickhouse-addr",
			envOr("CLICKHOUSE_ADDR", "localhost:9000"),
			"ClickHouse native-protocol address host:port (truncate + checksum queries)")
		httpAddr = flag.String("clickhouse-http-addr",
			envOr("CLICKHOUSE_HTTP_ADDR", "localhost:8123"),
			"ClickHouse HTTP address host:port (fixture INSERT)")
		database = flag.String("database",
			envOr("CLICKHOUSE_DB", "naap"),
			"ClickHouse database")
		user = flag.String("user",
			envOr("CLICKHOUSE_ADMIN_USER", "naap_admin"),
			"ClickHouse user (must have TRUNCATE + INSERT on the targeted tables)")
		password = flag.String("password",
			envOr("CLICKHOUSE_ADMIN_PASSWORD", "changeme"),
			"ClickHouse password")
		mvFlush = flag.Duration("mv-flush-timeout",
			2*time.Second,
			"time to wait for downstream MVs to settle after fixture load")
		skipLoad = flag.Bool("skip-load", false,
			"skip fixture load; checksum whatever state is already in ClickHouse (dev-only)")
		skipResolver = flag.Bool("skip-resolver", false,
			"skip canonical-layer rebuild; checksum existing canonical_* state (dev-only)")
		skipDBT = flag.Bool("skip-dbt", false,
			"skip api-layer dbt run; checksum existing api_* views (dev-only)")
		dbtSelector = flag.String("dbt-selector", "+api +api_base",
			"dbt --select expression used during the api phase")
		pauseIngestion = flag.Bool("pause-ingestion", true,
			"detach Kafka ingest MVs while the harness runs; turn off on environments with no Kafka producer")
		resolverNow = flag.String("resolver-now", "",
			"RFC3339 timestamp used as the frozen wall clock for the canonical phase; empty => fixture window_end")
		resolverStep = flag.Duration("resolver-step", 24*time.Hour,
			"backfill partition step the resolver processes in one executeWindow call")
	)
	flag.Parse()

	layers, err := parseLayers(*layerList)
	if err != nil {
		fatal("invalid --layers: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	conn, err := ch.Open(&ch.Options{
		Addr: []string{*addr},
		Auth: ch.Auth{
			Database: *database,
			Username: *user,
			Password: *password,
		},
		Settings: ch.Settings{
			// 12 GiB is well below the 31 GiB host but comfortably above
			// what a FINAL merge on accepted_raw_events (~5M rows, ~1 KB
			// avg) needs. Fails loud rather than silently spilling to disk.
			"max_memory_usage": 12 * 1024 * 1024 * 1024,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fatal("clickhouse open: %v", err)
	}
	defer conn.Close()

	var now time.Time
	if *resolverNow != "" {
		parsed, parseErr := time.Parse(time.RFC3339, *resolverNow)
		if parseErr != nil {
			fatal("invalid --resolver-now %q: %v", *resolverNow, parseErr)
		}
		now = parsed.UTC()
	}

	log, logErr := zap.NewDevelopment()
	if logErr != nil {
		log = zap.NewNop()
	}
	defer func() { _ = log.Sync() }()

	report, err := replay.Run(ctx, conn, replay.Config{
		Database:       *database,
		Layers:         layers,
		FixturePath:    *fixturePath,
		ManifestPath:   *manifestPath,
		MVFlushTimeout: *mvFlush,
		SkipLoad:       *skipLoad,
		SkipResolver:   *skipResolver,
		SkipDBT:        *skipDBT,
		PauseIngestion: *pauseIngestion,
		Loader: replay.LoadConfig{
			Database: *database,
			HTTPAddr: *httpAddr,
			User:     *user,
			Password: *password,
		},
		Resolver: replay.ResolverConfig{
			Now:                now,
			Step:               *resolverStep,
			ClickHouseAddr:     *addr,
			ClickHouseDB:       *database,
			ClickHouseUser:     *user,
			ClickHousePassword: *password,
		},
		DBT: replay.DBTConfig{
			Selector: *dbtSelector,
		},
		Logger: log,
	})
	if err != nil {
		fatal("replay: %v", err)
	}

	reportPath, err := writeReport(*outputDir, report)
	if err != nil {
		fatal("write report: %v", err)
	}
	fmt.Printf("wrote %s\n", reportPath)
	printSummary(report)

	if *compareTo != "" {
		expected, err := loadReport(*compareTo)
		if err != nil {
			fatal("load --compare-to: %v", err)
		}
		divs := replay.Diff(expected, report)
		fmt.Println(replay.FormatDivergences(divs))
		if len(divs) > 0 {
			os.Exit(1)
		}
	}
}

func parseLayers(s string) ([]replay.Layer, error) {
	if s == "" || s == "all" {
		return replay.AllLayers(), nil
	}
	valid := map[string]replay.Layer{
		"raw":        replay.LayerRaw,
		"normalized": replay.LayerNormalized,
		"canonical":  replay.LayerCanonical,
		"api":        replay.LayerAPI,
	}
	var out []replay.Layer
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(strings.ToLower(tok))
		if tok == "" {
			continue
		}
		v, ok := valid[tok]
		if !ok {
			return nil, fmt.Errorf("unknown layer %q", tok)
		}
		out = append(out, v)
	}
	return out, nil
}

func writeReport(outputDir string, report *replay.Report) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", err
	}
	stamp := report.StartedAt.Format("20060102-150405")
	path := filepath.Join(outputDir, fmt.Sprintf("%s-%s.json", stamp, report.FixtureName))
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", err
	}
	// Also write a stable "latest.json" sibling for convenience.
	latest := filepath.Join(outputDir, "latest.json")
	_ = os.WriteFile(latest, append(data, '\n'), 0o644)
	return path, nil
}

func loadReport(path string) (*replay.Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r replay.Report
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func printSummary(r *replay.Report) {
	fmt.Printf("fixture=%s layers=%d elapsed=%s\n",
		r.FixtureName, len(r.Layers),
		r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond))
	for _, l := range r.Layers {
		fmt.Printf("  %-11s %3d tables  %4d ms\n",
			l.Layer, len(l.Tables), l.DurationMS)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "replay: "+format+"\n", args...)
	os.Exit(1)
}
