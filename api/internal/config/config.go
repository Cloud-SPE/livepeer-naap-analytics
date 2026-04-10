// Package config handles configuration loading from environment variables.
// This is layer 2 — may import types only.
package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all application configuration.
// All fields are loaded from environment variables at startup.
// See .env.example for the common documented env surface used by local and deployed stacks.
type Config struct {
	// Server
	Port     string `envconfig:"PORT" default:"8000"`
	Env      string `envconfig:"ENV" default:"development"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`

	// ClickHouse — native protocol (port 9000)
	ClickHouseAddr     string        `envconfig:"CLICKHOUSE_ADDR" default:"localhost:9000"`
	ClickHouseDB       string        `envconfig:"CLICKHOUSE_DB" default:"naap"`
	ClickHouseUser     string        `envconfig:"CLICKHOUSE_USER" default:"naap_reader"`
	ClickHousePassword string        `envconfig:"CLICKHOUSE_PASSWORD" default:"naap_reader_changeme"`
	ClickHouseTimeout  time.Duration `envconfig:"CLICKHOUSE_TIMEOUT" default:"30s"`

	// ClickHouse writer — used by the enrichment worker for INSERT operations.
	ClickHouseWriterUser     string `envconfig:"CLICKHOUSE_WRITER_USER" default:"naap_writer"`
	ClickHouseWriterPassword string `envconfig:"CLICKHOUSE_WRITER_PASSWORD" default:"naap_writer_changeme"`

	// Enrichment — periodic fetch of orchestrator/gateway metadata from the Livepeer API.
	EnrichmentEnabled  bool          `envconfig:"ENRICHMENT_ENABLED" default:"true"`
	EnrichmentInterval time.Duration `envconfig:"ENRICHMENT_INTERVAL" default:"5m"`
	LivepeerAPIURL     string        `envconfig:"LIVEPEER_API_URL" default:"https://livepeer-api.livepeer.cloud"`

	// Selection-centered resolver runtime
	ResolverEnabled          bool          `envconfig:"RESOLVER_ENABLED" default:"false"`
	ResolverMode             string        `envconfig:"RESOLVER_MODE" default:"auto"`
	ResolverInterval         time.Duration `envconfig:"RESOLVER_INTERVAL" default:"1m"`
	ResolverLatenessWindow   time.Duration `envconfig:"RESOLVER_LATENESS_WINDOW" default:"10m"`
	ResolverDirtyQuietPeriod time.Duration `envconfig:"RESOLVER_DIRTY_QUIET_PERIOD" default:"2m"`
	ResolverClaimTTL         time.Duration `envconfig:"RESOLVER_CLAIM_TTL" default:"2m"`
	ResolverPort             string        `envconfig:"RESOLVER_PORT" default:"9102"`
	ResolverVersion          string        `envconfig:"RESOLVER_VERSION" default:"selection-centered-v1"`
	ResolverBatchSize        int           `envconfig:"RESOLVER_BATCH_SIZE" default:"10000"`
}

// IsDevelopment returns true when running in development mode.
func (c *Config) IsDevelopment() bool {
	return c.Env == "development"
}

// Load reads configuration from environment variables.
// Returns an error if required variables are missing or malformed.
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &cfg, nil
}
