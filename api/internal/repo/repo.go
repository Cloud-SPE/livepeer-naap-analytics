// Package repo defines data access interfaces for the NAAP Analytics API v2.
// This is layer 3 — implementations depend on types and config.
// Concrete implementations live in sub-packages (e.g., repo/clickhouse).
package repo

import (
	"context"

	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsRepo is the read interface for all NAAP analytics data.
// Implementations must be safe for concurrent use.
type AnalyticsRepo interface {
	// Dashboard
	GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error)
	GetDashboardPipelines(ctx context.Context, limit int, windowHours int) ([]types.DashboardPipelineUsage, error)
	GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error)
	GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error)
	GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error)
	GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error)
	GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error)

	// Dashboard — request-job overview (used by KPI and pipelines combined endpoints)
	GetDashboardJobsOverview(ctx context.Context, windowHours int) (*types.DashboardJobsOverview, error)
	GetDashboardJobsByPipeline(ctx context.Context, windowHours int) ([]types.DashboardJobsByPipelineRow, error)
	GetDashboardJobsByCapability(ctx context.Context, windowHours int) ([]types.DashboardJobsByCapabilityRow, error)

	// Streaming
	GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error)

	// Requests
	GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error)

	// Discover
	DiscoverOrchestrators(ctx context.Context, p types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error)

	// Health
	Ping(ctx context.Context) error
}

// NoopAnalyticsRepo is a no-op implementation for use in tests.
type NoopAnalyticsRepo struct{}

func (n *NoopAnalyticsRepo) GetDashboardKPI(_ context.Context, _ int, _, _ string) (*types.DashboardKPI, error) {
	return &types.DashboardKPI{HourlySessions: []types.HourlyBucket{}, HourlyUsage: []types.HourlyBucket{}}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardPipelines(_ context.Context, _ int, _ int) ([]types.DashboardPipelineUsage, error) {
	return []types.DashboardPipelineUsage{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardOrchestrators(_ context.Context, _ int) ([]types.DashboardOrchestrator, error) {
	return []types.DashboardOrchestrator{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardGPUCapacity(_ context.Context) (*types.DashboardGPUCapacity, error) {
	return &types.DashboardGPUCapacity{Models: []types.DashboardGPUModelCapacity{}, PipelineGPUs: []types.DashboardGPUCapacityPipeline{}}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardPipelineCatalog(_ context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	return []types.DashboardPipelineCatalogEntry{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardPricing(_ context.Context) ([]types.DashboardPipelinePricing, error) {
	return []types.DashboardPipelinePricing{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardJobFeed(_ context.Context, _ int) ([]types.DashboardJobFeedItem, error) {
	return []types.DashboardJobFeedItem{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardJobsOverview(_ context.Context, _ int) (*types.DashboardJobsOverview, error) {
	return &types.DashboardJobsOverview{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardJobsByPipeline(_ context.Context, _ int) ([]types.DashboardJobsByPipelineRow, error) {
	return []types.DashboardJobsByPipelineRow{}, nil
}
func (n *NoopAnalyticsRepo) GetDashboardJobsByCapability(_ context.Context, _ int) ([]types.DashboardJobsByCapabilityRow, error) {
	return []types.DashboardJobsByCapabilityRow{}, nil
}
func (n *NoopAnalyticsRepo) GetStreamingModels(_ context.Context) ([]types.StreamingModel, error) {
	return []types.StreamingModel{}, nil
}
func (n *NoopAnalyticsRepo) GetRequestsModels(_ context.Context) ([]types.RequestsModel, error) {
	return []types.RequestsModel{}, nil
}
func (n *NoopAnalyticsRepo) DiscoverOrchestrators(_ context.Context, _ types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	return []types.DiscoverOrchestratorRow{}, nil
}
func (n *NoopAnalyticsRepo) Ping(_ context.Context) error { return nil }
