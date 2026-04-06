// Package service implements business logic for the NAAP Analytics API.
// This is layer 4 — depends on types, config, and repo interfaces.
// Service methods must not leak storage concerns to callers.
package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsService provides analytics query operations.
// Phase 3: thin delegation to repo. Business logic added in Phase 4+.
type AnalyticsService interface {
	// Network state (R1)
	GetNetworkSummary(ctx context.Context, p types.QueryParams) (*types.NetworkSummary, error)
	ListOrchestrators(ctx context.Context, p types.QueryParams) ([]types.Orchestrator, error)
	GetGPUSummary(ctx context.Context, p types.QueryParams) (*types.GPUSummary, error)
	ListModels(ctx context.Context, p types.QueryParams) ([]types.ModelAvailability, error)

	// Stream activity (R2)
	GetActiveStreams(ctx context.Context, p types.QueryParams) (*types.ActiveStreamsSummary, error)
	GetStreamSummary(ctx context.Context, p types.QueryParams) (*types.StreamSummary, error)
	ListStreamHistory(ctx context.Context, p types.QueryParams) ([]types.StreamBucket, error)

	// Performance (R3)
	GetFPSSummary(ctx context.Context, p types.QueryParams) (*types.FPSSummary, error)
	ListFPSHistory(ctx context.Context, p types.QueryParams) ([]types.FPSBucket, error)
	GetLatencySummary(ctx context.Context, p types.QueryParams) (*types.LatencySummary, error)
	GetWebRTCQuality(ctx context.Context, p types.QueryParams) (*types.WebRTCQuality, error)

	// Payments (R4)
	GetPaymentSummary(ctx context.Context, p types.QueryParams) (*types.PaymentSummary, error)
	ListPaymentHistory(ctx context.Context, p types.QueryParams) ([]types.PaymentBucket, error)
	ListPaymentsByPipeline(ctx context.Context, p types.QueryParams) ([]types.PipelinePayment, error)
	ListPaymentsByOrch(ctx context.Context, p types.QueryParams) ([]types.OrchPayment, error)

	// Reliability (R5)
	GetReliabilitySummary(ctx context.Context, p types.QueryParams) (*types.ReliabilitySummary, error)
	ListReliabilityHistory(ctx context.Context, p types.QueryParams) ([]types.ReliabilityBucket, error)
	ListOrchReliability(ctx context.Context, p types.QueryParams) ([]types.OrchReliability, error)
	ListFailures(ctx context.Context, p types.QueryParams) ([]types.FailureEvent, error)

	// Leaderboard (R6)
	GetLeaderboard(ctx context.Context, p types.QueryParams) ([]types.LeaderboardEntry, error)
	GetOrchProfile(ctx context.Context, address string) (*types.OrchProfile, error)

	// Gateways (R7)
	ListGateways(ctx context.Context, p types.QueryParams) ([]types.Gateway, error)
	GetGatewayProfile(ctx context.Context, address string) (*types.GatewayProfile, error)
	ListGatewayOrchestrators(ctx context.Context, address string, p types.QueryParams) ([]types.GatewayOrch, error)

	// Stream extensions (R8)
	ListStreamSamples(ctx context.Context, p types.QueryParams) ([]types.StreamStatusSample, error)
	GetStreamDetail(ctx context.Context, streamID string) (*types.StreamDetail, error)
	GetAttributionSummary(ctx context.Context, p types.QueryParams) (*types.AttributionSummary, error)

	// E2E Latency (R9)
	GetE2ELatencySummary(ctx context.Context, p types.QueryParams) (*types.E2ELatencySummary, error)
	ListE2ELatencyHistory(ctx context.Context, p types.QueryParams) ([]types.E2ELatencyBucket, error)

	// Pipelines (R10)
	ListPipelines(ctx context.Context, p types.QueryParams) ([]types.PipelineSummary, error)
	GetPipelineDetail(ctx context.Context, pipeline string, p types.QueryParams) (*types.PipelineDetail, error)

	// Pricing (R11)
	ListPricing(ctx context.Context, p types.QueryParams) ([]types.OrchPricingEntry, error)
	GetOrchPricingProfile(ctx context.Context, address string) (*types.OrchPricingProfile, error)

	// Model performance (R12)
	ListModelPerformance(ctx context.Context, p types.QueryParams) ([]types.ModelPerformance, error)
	GetModelDetail(ctx context.Context, modelID string, p types.QueryParams) (*types.ModelDetail, error)

	// Extended payments (R13)
	ListPaymentsByGateway(ctx context.Context, p types.QueryParams) ([]types.GatewayPayment, error)
	ListPaymentsByStream(ctx context.Context, p types.QueryParams) ([]types.StreamPayment, error)

	// Failure analysis (R14)
	ListFailuresByPipeline(ctx context.Context, p types.QueryParams) ([]types.FailuresByPipeline, error)
	ListFailuresByOrch(ctx context.Context, p types.QueryParams) ([]types.FailuresByOrch, error)

	// Capacity (R15)
	GetCapacitySummary(ctx context.Context, p types.QueryParams) (*types.CapacitySummary, error)

	// SLA / GPU / Network Demand (ported from leaderboard-serverless)
	ListSLACompliance(ctx context.Context, p types.SLAComplianceParams) ([]types.SLAComplianceRow, int, error)
	ListNetworkDemand(ctx context.Context, p types.NetworkDemandParams) ([]types.NetworkDemandRow, int, error)
	ListGPUNetworkDemand(ctx context.Context, p types.GPUNetworkDemandParams) ([]types.GPUNetworkDemandRow, int, error)
	ListGPUMetrics(ctx context.Context, p types.GPUMetricsParams) ([]types.GPUMetric, int, error)

	// Dashboard — pre-aggregated UI endpoints (R16)
	GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error)
	GetDashboardPipelines(ctx context.Context, limit int) ([]types.DashboardPipelineUsage, error)
	GetDashboardOrchestrators(ctx context.Context, windowHours int) ([]types.DashboardOrchestrator, error)
	GetDashboardGPUCapacity(ctx context.Context) (*types.DashboardGPUCapacity, error)
	GetDashboardPipelineCatalog(ctx context.Context) ([]types.DashboardPipelineCatalogEntry, error)
	GetDashboardPricing(ctx context.Context) ([]types.DashboardPipelinePricing, error)
	GetDashboardJobFeed(ctx context.Context, limit int) ([]types.DashboardJobFeedItem, error)

	// Health
	Ping(ctx context.Context) error
}

type analyticsService struct {
	repo repo.AnalyticsRepo
}

// New constructs an AnalyticsService backed by the given repo.
func New(r repo.AnalyticsRepo) AnalyticsService {
	return &analyticsService{repo: r}
}

func (s *analyticsService) GetNetworkSummary(ctx context.Context, p types.QueryParams) (*types.NetworkSummary, error) {
	v, err := s.repo.GetNetworkSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get network summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListOrchestrators(ctx context.Context, p types.QueryParams) ([]types.Orchestrator, error) {
	v, err := s.repo.ListOrchestrators(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list orchestrators: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetGPUSummary(ctx context.Context, p types.QueryParams) (*types.GPUSummary, error) {
	v, err := s.repo.GetGPUSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get gpu summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListModels(ctx context.Context, p types.QueryParams) ([]types.ModelAvailability, error) {
	v, err := s.repo.ListModels(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list models: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetActiveStreams(ctx context.Context, p types.QueryParams) (*types.ActiveStreamsSummary, error) {
	v, err := s.repo.GetActiveStreams(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get active streams: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetStreamSummary(ctx context.Context, p types.QueryParams) (*types.StreamSummary, error) {
	v, err := s.repo.GetStreamSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get stream summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListStreamHistory(ctx context.Context, p types.QueryParams) ([]types.StreamBucket, error) {
	v, err := s.repo.ListStreamHistory(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list stream history: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetFPSSummary(ctx context.Context, p types.QueryParams) (*types.FPSSummary, error) {
	v, err := s.repo.GetFPSSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get fps summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListFPSHistory(ctx context.Context, p types.QueryParams) ([]types.FPSBucket, error) {
	v, err := s.repo.ListFPSHistory(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list fps history: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetLatencySummary(ctx context.Context, p types.QueryParams) (*types.LatencySummary, error) {
	v, err := s.repo.GetLatencySummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get latency summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetWebRTCQuality(ctx context.Context, p types.QueryParams) (*types.WebRTCQuality, error) {
	v, err := s.repo.GetWebRTCQuality(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get webrtc quality: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetPaymentSummary(ctx context.Context, p types.QueryParams) (*types.PaymentSummary, error) {
	v, err := s.repo.GetPaymentSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get payment summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPaymentHistory(ctx context.Context, p types.QueryParams) ([]types.PaymentBucket, error) {
	v, err := s.repo.ListPaymentHistory(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list payment history: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPaymentsByPipeline(ctx context.Context, p types.QueryParams) ([]types.PipelinePayment, error) {
	v, err := s.repo.ListPaymentsByPipeline(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list payments by pipeline: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPaymentsByOrch(ctx context.Context, p types.QueryParams) ([]types.OrchPayment, error) {
	v, err := s.repo.ListPaymentsByOrch(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list payments by orch: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetReliabilitySummary(ctx context.Context, p types.QueryParams) (*types.ReliabilitySummary, error) {
	v, err := s.repo.GetReliabilitySummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get reliability summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListReliabilityHistory(ctx context.Context, p types.QueryParams) ([]types.ReliabilityBucket, error) {
	v, err := s.repo.ListReliabilityHistory(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list reliability history: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListOrchReliability(ctx context.Context, p types.QueryParams) ([]types.OrchReliability, error) {
	v, err := s.repo.ListOrchReliability(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list orch reliability: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListFailures(ctx context.Context, p types.QueryParams) ([]types.FailureEvent, error) {
	v, err := s.repo.ListFailures(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list failures: %w", err)
	}
	return v, nil
}

// GetLeaderboard fetches raw entries from the repo and applies composite scoring.
// Score = 0.30*normFPS + 0.30*(1-normDegraded) + 0.20*normVolume + 0.20*(1-normLatency), scaled 0-100.
func (s *analyticsService) GetLeaderboard(ctx context.Context, p types.QueryParams) ([]types.LeaderboardEntry, error) {
	entries, err := s.repo.GetLeaderboard(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get leaderboard: %w", err)
	}
	if len(entries) > 0 {
		scoreLeaderboard(entries)
	}
	return entries, nil
}

// scoreLeaderboard applies min-max normalised composite scoring in-place and
// re-sorts entries by Score DESC.
func scoreLeaderboard(entries []types.LeaderboardEntry) {
	n := len(entries)
	fps := make([]float64, n)
	deg := make([]float64, n)
	vol := make([]float64, n)
	lat := make([]float64, n)
	for i, e := range entries {
		fps[i] = e.AvgInferenceFPS
		deg[i] = e.DegradedRate
		vol[i] = float64(e.StreamsHandled)
		lat[i] = e.AvgLatencyMS
	}

	nFPS := normMinMax(fps)
	nDeg := normMinMax(deg)
	nVol := normMinMax(vol)
	nLat := normMinMax(lat)

	for i := range entries {
		entries[i].Score = (0.30*nFPS[i] + 0.30*(1-nDeg[i]) + 0.20*nVol[i] + 0.20*(1-nLat[i])) * 100
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Score > entries[j].Score
	})
}

// normMinMax returns a slice of values normalised to [0,1] using min-max scaling.
// When all values are equal the normalised value is 1.0.
func normMinMax(vals []float64) []float64 {
	mn, mx := vals[0], vals[0]
	for _, v := range vals[1:] {
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	out := make([]float64, len(vals))
	rng := mx - mn
	for i, v := range vals {
		if rng == 0 {
			out[i] = 1.0
		} else {
			out[i] = (v - mn) / rng
		}
	}
	return out
}

func (s *analyticsService) GetOrchProfile(ctx context.Context, address string) (*types.OrchProfile, error) {
	v, err := s.repo.GetOrchProfile(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("get orch profile: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListGateways(ctx context.Context, p types.QueryParams) ([]types.Gateway, error) {
	v, err := s.repo.ListGateways(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list gateways: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetGatewayProfile(ctx context.Context, address string) (*types.GatewayProfile, error) {
	v, err := s.repo.GetGatewayProfile(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("get gateway profile: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListGatewayOrchestrators(ctx context.Context, address string, p types.QueryParams) ([]types.GatewayOrch, error) {
	v, err := s.repo.ListGatewayOrchestrators(ctx, address, p)
	if err != nil {
		return nil, fmt.Errorf("list gateway orchestrators: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListStreamSamples(ctx context.Context, p types.QueryParams) ([]types.StreamStatusSample, error) {
	v, err := s.repo.ListStreamSamples(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list stream samples: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetStreamDetail(ctx context.Context, streamID string) (*types.StreamDetail, error) {
	v, err := s.repo.GetStreamDetail(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("get stream detail: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetAttributionSummary(ctx context.Context, p types.QueryParams) (*types.AttributionSummary, error) {
	v, err := s.repo.GetAttributionSummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get attribution summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetE2ELatencySummary(ctx context.Context, p types.QueryParams) (*types.E2ELatencySummary, error) {
	v, err := s.repo.GetE2ELatencySummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get e2e latency summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListE2ELatencyHistory(ctx context.Context, p types.QueryParams) ([]types.E2ELatencyBucket, error) {
	v, err := s.repo.ListE2ELatencyHistory(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list e2e latency history: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPipelines(ctx context.Context, p types.QueryParams) ([]types.PipelineSummary, error) {
	v, err := s.repo.ListPipelines(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetPipelineDetail(ctx context.Context, pipeline string, p types.QueryParams) (*types.PipelineDetail, error) {
	v, err := s.repo.GetPipelineDetail(ctx, pipeline, p)
	if err != nil {
		return nil, fmt.Errorf("get pipeline detail: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPricing(ctx context.Context, p types.QueryParams) ([]types.OrchPricingEntry, error) {
	v, err := s.repo.ListPricing(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list pricing: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetOrchPricingProfile(ctx context.Context, address string) (*types.OrchPricingProfile, error) {
	v, err := s.repo.GetOrchPricingProfile(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("get orch pricing profile: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListModelPerformance(ctx context.Context, p types.QueryParams) ([]types.ModelPerformance, error) {
	v, err := s.repo.ListModelPerformance(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list model performance: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetModelDetail(ctx context.Context, modelID string, p types.QueryParams) (*types.ModelDetail, error) {
	v, err := s.repo.GetModelDetail(ctx, modelID, p)
	if err != nil {
		return nil, fmt.Errorf("get model detail: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPaymentsByGateway(ctx context.Context, p types.QueryParams) ([]types.GatewayPayment, error) {
	v, err := s.repo.ListPaymentsByGateway(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list payments by gateway: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListPaymentsByStream(ctx context.Context, p types.QueryParams) ([]types.StreamPayment, error) {
	v, err := s.repo.ListPaymentsByStream(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list payments by stream: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListFailuresByPipeline(ctx context.Context, p types.QueryParams) ([]types.FailuresByPipeline, error) {
	v, err := s.repo.ListFailuresByPipeline(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list failures by pipeline: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListFailuresByOrch(ctx context.Context, p types.QueryParams) ([]types.FailuresByOrch, error) {
	v, err := s.repo.ListFailuresByOrch(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list failures by orch: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetCapacitySummary(ctx context.Context, p types.QueryParams) (*types.CapacitySummary, error) {
	v, err := s.repo.GetCapacitySummary(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("get capacity summary: %w", err)
	}
	return v, nil
}

func (s *analyticsService) ListSLACompliance(ctx context.Context, p types.SLAComplianceParams) ([]types.SLAComplianceRow, int, error) {
	rows, total, err := s.repo.ListSLACompliance(ctx, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list sla compliance: %w", err)
	}
	return rows, total, nil
}

func (s *analyticsService) ListNetworkDemand(ctx context.Context, p types.NetworkDemandParams) ([]types.NetworkDemandRow, int, error) {
	rows, total, err := s.repo.ListNetworkDemand(ctx, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list network demand: %w", err)
	}
	return rows, total, nil
}

func (s *analyticsService) ListGPUNetworkDemand(ctx context.Context, p types.GPUNetworkDemandParams) ([]types.GPUNetworkDemandRow, int, error) {
	rows, total, err := s.repo.ListGPUNetworkDemand(ctx, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list gpu network demand: %w", err)
	}
	return rows, total, nil
}

func (s *analyticsService) ListGPUMetrics(ctx context.Context, p types.GPUMetricsParams) ([]types.GPUMetric, int, error) {
	rows, total, err := s.repo.ListGPUMetrics(ctx, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list gpu metrics: %w", err)
	}
	return rows, total, nil
}

func (s *analyticsService) GetDashboardKPI(ctx context.Context, windowHours int, pipeline, modelID string) (*types.DashboardKPI, error) {
	v, err := s.repo.GetDashboardKPI(ctx, windowHours, pipeline, modelID)
	if err != nil {
		return nil, fmt.Errorf("get dashboard kpi: %w", err)
	}
	return v, nil
}

func (s *analyticsService) GetDashboardPipelines(ctx context.Context, limit int) ([]types.DashboardPipelineUsage, error) {
	v, err := s.repo.GetDashboardPipelines(ctx, limit)
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

func (s *analyticsService) Ping(ctx context.Context) error {
	return s.repo.Ping(ctx)
}
