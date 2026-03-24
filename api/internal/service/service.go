// Package service implements business logic for the NAAP Analytics API.
// This is layer 4 — depends on types, config, and repo interfaces.
// Service methods must not leak storage concerns to callers.
package service

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsService provides analytics query operations.
type AnalyticsService interface {
	QueryWindows(ctx context.Context, params types.QueryParams) ([]types.AggregatedWindow, error)
	QueryAlerts(ctx context.Context, params types.QueryParams) ([]types.Alert, error)
}

type analyticsService struct {
	repo repo.AnalyticsRepo
}

// New constructs an AnalyticsService backed by the given repo.
func New(r repo.AnalyticsRepo) AnalyticsService {
	return &analyticsService{repo: r}
}

func (s *analyticsService) QueryWindows(ctx context.Context, params types.QueryParams) ([]types.AggregatedWindow, error) {
	windows, err := s.repo.GetWindows(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("query windows: %w", err)
	}
	return windows, nil
}

func (s *analyticsService) QueryAlerts(ctx context.Context, params types.QueryParams) ([]types.Alert, error) {
	alerts, err := s.repo.GetAlerts(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}
	return alerts, nil
}
