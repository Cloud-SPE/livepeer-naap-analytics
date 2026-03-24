// Package providers holds cross-cutting concerns injected at the runtime layer.
// Providers are constructed once at startup and passed via interfaces — never imported
// directly by types, config, repo, or service layers.
package providers

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/livepeer/naap-analytics/internal/config"
)

// Providers bundles all cross-cutting dependencies.
type Providers struct {
	Logger *zap.Logger
	// Tracer go.opentelemetry.io/otel/trace.Tracer  — add when OTLP endpoint is configured
}

// New constructs and initialises all providers from configuration.
func New(cfg *config.Config) (*Providers, error) {
	logger, err := newLogger(cfg)
	if err != nil {
		return nil, err
	}
	return &Providers{Logger: logger}, nil
}

// Close flushes and shuts down providers gracefully.
func (p *Providers) Close(ctx context.Context) {
	_ = ctx
	_ = p.Logger.Sync()
}

func newLogger(cfg *config.Config) (*zap.Logger, error) {
	level := zapcore.InfoLevel
	if cfg.IsDevelopment() {
		level = zapcore.DebugLevel
	}

	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(level)
	if cfg.IsDevelopment() {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.Level = zap.NewAtomicLevelAt(level)
	}

	return zapCfg.Build()
}
