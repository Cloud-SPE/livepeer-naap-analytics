// Package repo defines data access interfaces for the NAAP Analytics API v2.
// This is layer 3 — implementations depend on types and config.
// Concrete implementations live in sub-packages (e.g., repo/clickhouse).
package repo

import (
	"context"

	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsRepo is the read interface for all NAAP analytics data.
// 21 query methods + 1 healthcheck. Implementations must be safe for concurrent use.
type AnalyticsRepo interface {
	// Dashboard (7 endpoints)
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

	// Streaming (5 endpoints)
	GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error)
	GetStreamingOrchestrators(ctx context.Context) ([]types.StreamingOrchestrator, error)
	ListStreamingSLA(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, types.CursorPageInfo, error)
	ListStreamingDemand(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, types.CursorPageInfo, error)
	ListStreamingGPUMetrics(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, types.CursorPageInfo, error)

	// Requests (9 endpoints)
	GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error)
	GetRequestsOrchestrators(ctx context.Context) ([]types.RequestsOrchestrator, error)
	GetAIBatchSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchSummaryRow, error)
	ListAIBatchJobs(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error)
	GetAIBatchLLMSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error)
	GetBYOCSummary(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCSummaryRow, error)
	ListBYOCJobs(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error)
	GetBYOCWorkers(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCWorkerRow, error)
	GetBYOCAuth(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCAuthRow, error)

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
func (n *NoopAnalyticsRepo) GetStreamingOrchestrators(_ context.Context) ([]types.StreamingOrchestrator, error) {
	return []types.StreamingOrchestrator{}, nil
}
func (n *NoopAnalyticsRepo) ListStreamingSLA(_ context.Context, _ types.TimeWindowParams) ([]types.StreamingSLARow, types.CursorPageInfo, error) {
	return []types.StreamingSLARow{}, types.CursorPageInfo{}, nil
}
func (n *NoopAnalyticsRepo) ListStreamingDemand(_ context.Context, _ types.TimeWindowParams) ([]types.StreamingDemandRow, types.CursorPageInfo, error) {
	return []types.StreamingDemandRow{}, types.CursorPageInfo{}, nil
}
func (n *NoopAnalyticsRepo) ListStreamingGPUMetrics(_ context.Context, _ types.TimeWindowParams) ([]types.StreamingGPUMetricRow, types.CursorPageInfo, error) {
	return []types.StreamingGPUMetricRow{}, types.CursorPageInfo{}, nil
}
func (n *NoopAnalyticsRepo) GetRequestsModels(_ context.Context) ([]types.RequestsModel, error) {
	return []types.RequestsModel{}, nil
}
func (n *NoopAnalyticsRepo) GetRequestsOrchestrators(_ context.Context) ([]types.RequestsOrchestrator, error) {
	return []types.RequestsOrchestrator{}, nil
}
func (n *NoopAnalyticsRepo) GetAIBatchSummary(_ context.Context, _ types.TimeWindowParams) ([]types.AIBatchSummaryRow, error) {
	return []types.AIBatchSummaryRow{}, nil
}
func (n *NoopAnalyticsRepo) ListAIBatchJobs(_ context.Context, _ types.TimeWindowParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	return []types.AIBatchJobRecord{}, types.CursorPageInfo{}, nil
}
func (n *NoopAnalyticsRepo) GetAIBatchLLMSummary(_ context.Context, _ types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error) {
	return []types.AIBatchLLMSummaryRow{}, nil
}
func (n *NoopAnalyticsRepo) GetBYOCSummary(_ context.Context, _ types.TimeWindowParams) ([]types.BYOCSummaryRow, error) {
	return []types.BYOCSummaryRow{}, nil
}
func (n *NoopAnalyticsRepo) ListBYOCJobs(_ context.Context, _ types.TimeWindowParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error) {
	return []types.BYOCJobRecord{}, types.CursorPageInfo{}, nil
}
func (n *NoopAnalyticsRepo) GetBYOCWorkers(_ context.Context, _ types.TimeWindowParams) ([]types.BYOCWorkerRow, error) {
	return []types.BYOCWorkerRow{}, nil
}
func (n *NoopAnalyticsRepo) GetBYOCAuth(_ context.Context, _ types.TimeWindowParams) ([]types.BYOCAuthRow, error) {
	return []types.BYOCAuthRow{}, nil
}
func (n *NoopAnalyticsRepo) DiscoverOrchestrators(_ context.Context, _ types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	return []types.DiscoverOrchestratorRow{}, nil
}
func (n *NoopAnalyticsRepo) Ping(_ context.Context) error { return nil }
