// Package config handles configuration loading from environment variables.
// This is layer 2 — may import types only.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all application configuration.
// All fields are loaded from environment variables at startup.
// See .env.example for the full list of supported variables.
type Config struct {
	// Server
	Port     string `envconfig:"PORT" default:"8000"`
	Env      string `envconfig:"ENV" default:"development"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`

	// Kafka (retained for pipeline compatibility; not used by Go API in Phase 3+)
	KafkaBrokers      string        `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	KafkaDialTimeout  time.Duration `envconfig:"KAFKA_DIAL_TIMEOUT" default:"10s"`
	KafkaReadTimeout  time.Duration `envconfig:"KAFKA_READ_TIMEOUT" default:"10s"`
	KafkaWriteTimeout time.Duration `envconfig:"KAFKA_WRITE_TIMEOUT" default:"10s"`

	// ClickHouse — native protocol (port 9000)
	ClickHouseAddr     string        `envconfig:"CLICKHOUSE_ADDR" default:"localhost:9000"`
	ClickHouseDB       string        `envconfig:"CLICKHOUSE_DB" default:"naap"`
	ClickHouseUser     string        `envconfig:"CLICKHOUSE_USER" default:"naap_reader"`
	ClickHousePassword string        `envconfig:"CLICKHOUSE_PASSWORD" default:"naap_reader_changeme"`
	ClickHouseTimeout  time.Duration `envconfig:"CLICKHOUSE_TIMEOUT" default:"30s"`

	// Telemetry
	OTLPEndpoint string `envconfig:"OTLP_ENDPOINT" default:""`
}

// Brokers parses the KAFKA_BROKERS env var (comma-separated) into a slice.
func (c *Config) Brokers() []string {
	return strings.Split(c.KafkaBrokers, ",")
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
