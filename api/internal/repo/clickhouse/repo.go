// Package clickhouse implements repo.AnalyticsRepo backed by ClickHouse.
//
// Query routing rules:
//   - API handlers read `naap.api_*` read models.
//   - Canonical derivation and future downstream logic must not treat `api_*`
//     as a source-of-truth layer; that role belongs to `canonical_*`.
//
// Uses the clickhouse-go/v2 native protocol (port 9000) for full type support,
// including UInt64 columns used for WEI payment amounts.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/livepeer/naap-analytics/internal/config"
)

// Repo is the ClickHouse implementation of repo.AnalyticsRepo.
// Safe for concurrent use; the underlying driver.Conn manages connection pooling.
type Repo struct {
	conn driver.Conn
}

// New opens a ClickHouse connection and verifies it with a ping.
func New(cfg *config.Config) (*Repo, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: ch.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Settings: ch.Settings{
			"max_execution_time": int(cfg.ClickHouseTimeout.Seconds()),
		},
		DialTimeout:      5 * time.Second,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: ch.ConnOpenInOrder,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &Repo{conn: conn}, nil
}

// Close releases the connection pool.
func (r *Repo) Close() error {
	return r.conn.Close()
}

// Ping verifies the ClickHouse connection is alive.
func (r *Repo) Ping(ctx context.Context) error {
	return r.conn.Ping(ctx)
}
