package enrichment

import (
	"context"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
)

// Worker polls the Livepeer API on a fixed interval and upserts results into ClickHouse.
type Worker struct {
	client   *Client
	conn     driver.Conn
	interval time.Duration
	log      *zap.Logger
}

// New creates a Worker and opens a ClickHouse writer connection.
func New(cfg *config.Config, log *zap.Logger) (*Worker, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: ch.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseWriterUser,
			Password: cfg.ClickHouseWriterPassword,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    3,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return &Worker{
		client:   newClient(cfg.LivepeerAPIURL),
		conn:     conn,
		interval: cfg.EnrichmentInterval,
		log:      log,
	}, nil
}

// Run starts the enrichment loop. It runs once immediately, then on each tick.
// Returns when ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("enrichment worker started", zap.Duration("interval", w.interval))
	w.sync(ctx)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.log.Info("enrichment worker stopped")
			_ = w.conn.Close()
			return
		case <-ticker.C:
			w.sync(ctx)
		}
	}
}

func (w *Worker) sync(ctx context.Context) {
	if err := w.syncOrchestrators(ctx); err != nil {
		w.log.Warn("enrichment: orchestrator sync failed", zap.Error(err))
	}
	if err := w.syncGateways(ctx); err != nil {
		w.log.Warn("enrichment: gateway sync failed", zap.Error(err))
	}
}

func (w *Worker) syncOrchestrators(ctx context.Context) error {
	records, err := w.client.FetchOrchestrators(ctx)
	if err != nil {
		return err
	}

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO naap.orch_metadata")
	if err != nil {
		return err
	}

	now := time.Now()
	for _, r := range records {
		var active uint8
		if r.ActivationStatus {
			active = 1
		}
		if err := batch.Append(
			strings.ToLower(r.EthAddress),
			r.Name,
			r.ServiceURI,
			r.Avatar,
			r.TotalStake,
			r.RewardCut,
			r.FeeCut,
			active,
			now,
		); err != nil {
			return err
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}
	w.log.Info("enrichment: orchestrators synced", zap.Int("count", len(records)))
	return nil
}

func (w *Worker) syncGateways(ctx context.Context) error {
	records, err := w.client.FetchGateways(ctx)
	if err != nil {
		return err
	}

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO naap.gateway_metadata")
	if err != nil {
		return err
	}

	now := time.Now()
	for _, r := range records {
		if err := batch.Append(
			strings.ToLower(r.EthAddress),
			r.Name,
			r.Avatar,
			r.Deposit,
			r.Reserve,
			now,
		); err != nil {
			return err
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}
	w.log.Info("enrichment: gateways synced", zap.Int("count", len(records)))
	return nil
}
