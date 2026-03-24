// Package repo defines data access interfaces for the NAAP Analytics API.
// This is layer 3 — implementations depend on types and config.
// Concrete implementations (e.g., Postgres, ClickHouse) live in sub-packages.
package repo

import (
	"context"

	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsRepo is the read interface for aggregated analytics data.
// All methods accept a context for cancellation and deadline propagation.
type AnalyticsRepo interface {
	GetWindows(ctx context.Context, params types.QueryParams) ([]types.AggregatedWindow, error)
	GetAlerts(ctx context.Context, params types.QueryParams) ([]types.Alert, error)
}

// NoopAnalyticsRepo is a no-op implementation for use in tests and early development.
// Replace with a real implementation once storage is chosen.
type NoopAnalyticsRepo struct{}

func (n *NoopAnalyticsRepo) GetWindows(_ context.Context, _ types.QueryParams) ([]types.AggregatedWindow, error) {
	return []types.AggregatedWindow{}, nil
}

func (n *NoopAnalyticsRepo) GetAlerts(_ context.Context, _ types.QueryParams) ([]types.Alert, error) {
	return []types.Alert{}, nil
}
