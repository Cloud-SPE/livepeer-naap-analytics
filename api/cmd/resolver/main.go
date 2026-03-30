package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/providers"
	"github.com/livepeer/naap-analytics/internal/resolver"
)

func main() {
	var (
		mode               = flag.String("mode", "", "resolver mode: auto, bootstrap, tail, backfill, repair-window, verify")
		org                = flag.String("org", "", "optional org filter")
		excludeOrgPrefixes = flag.String("exclude-org-prefixes", "", "comma-separated org prefixes to exclude from broad runs")
		from               = flag.String("from", "", "window start RFC3339")
		to                 = flag.String("to", "", "window end RFC3339")
		step               = flag.Duration("step", 24*time.Hour, "backfill partition step")
		dryRun             = flag.Bool("dry-run", false, "plan and compute without mutating resolver tables")
	)
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		log.Printf("fatal: load config: %v", err)
		os.Exit(1)
	}
	if *mode != "" {
		cfg.ResolverMode = *mode
	}
	cfg.ResolverEnabled = true

	p, err := providers.New(cfg)
	if err != nil {
		log.Printf("fatal: init providers: %v", err)
		os.Exit(1)
	}
	defer p.Close(context.Background())

	engine, err := resolver.New(cfg, p.Logger)
	if err != nil {
		p.Logger.Sugar().Fatalf("resolver init failed: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	req, err := buildRequest(cfg, *org, *excludeOrgPrefixes, *from, *to, *step, *dryRun)
	if err != nil {
		p.Logger.Sugar().Fatalf("invalid resolver request: %v", err)
	}

	if req.Mode == resolver.ModeTail || req.Mode == resolver.ModeBootstrap || req.Mode == resolver.ModeAuto {
		engine.Run(ctx, req)
		return
	}

	stats, err := engine.Execute(ctx, req)
	if err != nil {
		p.Logger.Fatal("resolver execution failed", zap.Error(err))
	}
	p.Logger.Info("resolver execution completed",
		zap.String("mode", string(req.Mode)),
		zap.Int("selection_events", stats.SelectionEvents),
		zap.Int("capability_versions", stats.CapabilityVersions),
		zap.Int("capability_intervals", stats.CapabilityIntervals),
		zap.Int("decisions", stats.Decisions),
		zap.Int("session_rows", stats.SessionRows),
		zap.Int("status_hour_rows", stats.StatusHourRows),
	)
	_ = engine.Close(context.Background())
}

func buildRequest(cfg *config.Config, org, excludeOrgPrefixes, from, to string, step time.Duration, dryRun bool) (resolver.RunRequest, error) {
	req := resolver.RunRequest{
		Mode:                resolver.Mode(cfg.ResolverMode),
		Org:                 org,
		ExcludedOrgPrefixes: parseCSV(excludeOrgPrefixes),
		Step:                step,
		DryRun:              dryRun,
	}
	var err error
	if from != "" {
		ts, parseErr := time.Parse(time.RFC3339, from)
		if parseErr != nil {
			return req, parseErr
		}
		req.Start = &ts
	}
	if to != "" {
		ts, parseErr := time.Parse(time.RFC3339, to)
		if parseErr != nil {
			return req, parseErr
		}
		req.End = &ts
	}
	if req.Mode == "" {
		req.Mode = resolver.ModeAuto
	}
	return req, err
}

func parseCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
