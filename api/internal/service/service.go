// Package service implements business logic for the NAAP Analytics API v2.
// This is layer 4 — depends on types, config, and repo interfaces.
// Thin delegation to repo; business logic added as needed.
package service

import (
	"context"
	"fmt"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsService provides analytics query operations.
type AnalyticsService interface {
	// Dashboard (7 + 3 combined)
	GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error)
	GetDashboardPipelines(ctx context.Context, limit int, windowHours int) ([]types.DashboardPipelineUsage, error)
	GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error)
	GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error)
	GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error)
	GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error)
	GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error)
	GetDashboardJobsOverview(ctx context.Context, windowHours int) (*types.DashboardJobsOverview, error)
	GetDashboardJobsByPipeline(ctx context.Context, windowHours int) ([]types.DashboardJobsByPipelineRow, error)
	GetDashboardJobsByCapability(ctx context.Context, windowHours int) ([]types.DashboardJobsByCapabilityRow, error)

	// Streaming
	GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error)
	GetStreamingOrchestrators(ctx context.Context) ([]types.StreamingOrchestrator, error)
	ListStreamingSLA(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, string, error)
	ListStreamingDemand(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, string, error)
	ListStreamingGPUMetrics(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, string, error)

	// Requests
	GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error)
	GetRequestsOrchestrators(ctx context.Context) ([]types.RequestsOrchestrator, error)
	GetAIBatchSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchSummaryRow, error)
	ListAIBatchJobs(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, string, error)
	GetAIBatchLLMSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error)
	GetBYOCSummary(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCSummaryRow, error)
	ListBYOCJobs(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, string, error)
	GetBYOCWorkers(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCWorkerRow, error)
	GetBYOCAuth(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCAuthRow, error)

	// Discover
	DiscoverOrchestrators(ctx context.Context, p types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error)

	// Health
	Ping(ctx context.Context) error
}

type analyticsService struct {
	repo repo.AnalyticsRepo
}

func New(r repo.AnalyticsRepo) AnalyticsService {
	return &analyticsService{repo: r}
}

// --- Dashboard ---

func (s *analyticsService) GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error) {
	v, err := s.repo.GetDashboardKPI(ctx, windowHours, pipeline, modelID)
	if err != nil {
		return nil, fmt.Errorf("get dashboard kpi: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardPipelines(ctx context.Context, limit int, windowHours int) ([]types.DashboardPipelineUsage, error) {
	v, err := s.repo.GetDashboardPipelines(ctx, limit, windowHours)
	if err != nil {
		return nil, fmt.Errorf("get dashboard pipelines: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error) {
	v, err := s.repo.GetDashboardOrchestrators(ctx, windowHours)
	if err != nil {
		return nil, fmt.Errorf("get dashboard orchestrators: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error) {
	v, err := s.repo.GetDashboardGPUCapacity(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dashboard gpu capacity: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error) {
	v, err := s.repo.GetDashboardPipelineCatalog(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dashboard pipeline catalog: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error) {
	v, err := s.repo.GetDashboardPricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dashboard pricing: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error) {
	v, err := s.repo.GetDashboardJobFeed(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("get dashboard job feed: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardJobsOverview(ctx context.Context, windowHours int) (*types.DashboardJobsOverview, error) {
	v, err := s.repo.GetDashboardJobsOverview(ctx, windowHours)
	if err != nil {
		return nil, fmt.Errorf("get dashboard jobs overview: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardJobsByPipeline(ctx context.Context, windowHours int) ([]types.DashboardJobsByPipelineRow, error) {
	v, err := s.repo.GetDashboardJobsByPipeline(ctx, windowHours)
	if err != nil {
		return nil, fmt.Errorf("get dashboard jobs by pipeline: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardJobsByCapability(ctx context.Context, windowHours int) ([]types.DashboardJobsByCapabilityRow, error) {
	v, err := s.repo.GetDashboardJobsByCapability(ctx, windowHours)
	if err != nil {
		return nil, fmt.Errorf("get dashboard jobs by capability: %w", err)
	}
	return v, nil
}

// --- Streaming ---

func (s *analyticsService) GetStreamingModels(ctx context.Context) ([]types.StreamingModel, error) {
	v, err := s.repo.GetStreamingModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("get streaming models: %w", err)
	}
	return v, nil
}

// --- Requests ---

func (s *analyticsService) GetRequestsModels(ctx context.Context) ([]types.RequestsModel, error) {
	v, err := s.repo.GetRequestsModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("get requests models: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetStreamingOrchestrators(ctx context.Context) ([]types.StreamingOrchestrator, error) {
	v, err := s.repo.GetStreamingOrchestrators(ctx)
	if err != nil {
		return nil, fmt.Errorf("get streaming orchestrators: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListStreamingSLA(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingSLARow, string, error) {
	v, c, err := s.repo.ListStreamingSLA(ctx, p)
	if err != nil {
		return nil, "", fmt.Errorf("list streaming sla: %w", err)
	}
	return v, c, nil
}

func (s *analyticsService) ListStreamingDemand(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingDemandRow, string, error) {
	v, c, err := s.repo.ListStreamingDemand(ctx, p)
	if err != nil {
		return nil, "", fmt.Errorf("list streaming demand: %w", err)
	}
	return v, c, nil
}

func (s *analyticsService) ListStreamingGPUMetrics(ctx context.Context, p types.TimeWindowParams) ([]types.StreamingGPUMetricRow, string, error) {
	v, c, err := s.repo.ListStreamingGPUMetrics(ctx, p)
	if err != nil {
		return nil, "", fmt.Errorf("list streaming gpu metrics: %w", err)
	}
	return v, c, nil
}

func (s *analyticsService) GetRequestsOrchestrators(ctx context.Context) ([]types.RequestsOrchestrator, error) {
	v, err := s.repo.GetRequestsOrchestrators(ctx)
	if err != nil {
		return nil, fmt.Errorf("get requests orchestrators: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetAIBatchSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchSummaryRow, error) {
	v, err := s.repo.GetAIBatchSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get ai-batch summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListAIBatchJobs(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchJobRecord, string, error) {
	v, c, err := s.repo.ListAIBatchJobs(ctx, p)
	if err != nil {
		return nil, "", fmt.Errorf("list ai-batch jobs: %w", err)
	}
	return v, c, nil
}

func (s *analyticsService) GetAIBatchLLMSummary(ctx context.Context, p types.TimeWindowParams) ([]types.AIBatchLLMSummaryRow, error) {
	v, err := s.repo.GetAIBatchLLMSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get ai-batch llm summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetBYOCSummary(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCSummaryRow, error) {
	v, err := s.repo.GetBYOCSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get byoc summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListBYOCJobs(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCJobRecord, string, error) {
	v, c, err := s.repo.ListBYOCJobs(ctx, p)
	if err != nil {
		return nil, "", fmt.Errorf("list byoc jobs: %w", err)
	}
	return v, c, nil
}

func (s *analyticsService) GetBYOCWorkers(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCWorkerRow, error) {
	v, err := s.repo.GetBYOCWorkers(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get byoc workers: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetBYOCAuth(ctx context.Context, p types.TimeWindowParams) ([]types.BYOCAuthRow, error) {
	v, err := s.repo.GetBYOCAuth(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get byoc auth: %w", err)
	}
	return v, nil
}

// --- Discover ---

func (s *analyticsService) DiscoverOrchestrators(ctx context.Context, p types.DiscoverOrchestratorsParams) ([]types.DiscoverOrchestratorRow, error) {
	v, err := s.repo.DiscoverOrchestrators(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("discover orchestrators: %w", err)
	}
	return v, nil
}

// --- Health ---

func (s *analyticsService) Ping(ctx context.Context) error {
	return s.repo.Ping(ctx)
}
