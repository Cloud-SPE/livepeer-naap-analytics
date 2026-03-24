// Command server is the entry point for the NAAP Analytics API.
// Startup order: config → providers → repo → service → runtime.
package main

import (
	"log"
	"os"

	"github.com/livepeer/naap-analytics/internal/config"
	"github.com/livepeer/naap-analytics/internal/providers"
	chrepo "github.com/livepeer/naap-analytics/internal/repo/clickhouse"
	"github.com/livepeer/naap-analytics/internal/runtime"
	"github.com/livepeer/naap-analytics/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Printf("fatal: load config: %v", err)
		os.Exit(1)
	}

	p, err := providers.New(cfg)
	if err != nil {
		log.Printf("fatal: init providers: %v", err)
		os.Exit(1)
	}

	r, err := chrepo.New(cfg)
	if err != nil {
		log.Printf("fatal: init clickhouse repo: %v", err)
		os.Exit(1)
	}

	svc := service.New(r)
	srv := runtime.New(cfg, p, svc)

	if err := srv.Start(); err != nil {
		p.Logger.Sugar().Fatalf("server exited with error: %v", err)
	}
}
