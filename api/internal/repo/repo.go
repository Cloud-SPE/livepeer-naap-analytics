// Package repo defines data access interfaces for the NAAP Analytics API.
// This is layer 3 — implementations depend on types and config.
// Concrete implementations live in sub-packages (e.g., repo/clickhouse).
package repo

import (
	"context"

	"github.com/livepeer/naap-analytics/internal/types"
)

// AnalyticsRepo is the read interface for all NAAP analytics data.
// Methods map 1:1 to API endpoints. Implementations must be safe for concurrent use.
type AnalyticsRepo interface {
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

	// Healthcheck
	Ping(ctx context.Context) error
}

// NoopAnalyticsRepo is a no-op implementation for use in tests.
type NoopAnalyticsRepo struct{}

func (n *NoopAnalyticsRepo) GetNetworkSummary(_ context.Context, _ types.QueryParams) (*types.NetworkSummary, error) {
	return &types.NetworkSummary{}, nil
}
func (n *NoopAnalyticsRepo) ListOrchestrators(_ context.Context, _ types.QueryParams) ([]types.Orchestrator, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetGPUSummary(_ context.Context, _ types.QueryParams) (*types.GPUSummary, error) {
	return &types.GPUSummary{}, nil
}
func (n *NoopAnalyticsRepo) ListModels(_ context.Context, _ types.QueryParams) ([]types.ModelAvailability, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetActiveStreams(_ context.Context, _ types.QueryParams) (*types.ActiveStreamsSummary, error) {
	return &types.ActiveStreamsSummary{ByOrg: map[string]int64{}, ByPipeline: map[string]int64{}, ByState: map[string]int64{}}, nil
}
func (n *NoopAnalyticsRepo) GetStreamSummary(_ context.Context, _ types.QueryParams) (*types.StreamSummary, error) {
	return &types.StreamSummary{}, nil
}
func (n *NoopAnalyticsRepo) ListStreamHistory(_ context.Context, _ types.QueryParams) ([]types.StreamBucket, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetFPSSummary(_ context.Context, _ types.QueryParams) (*types.FPSSummary, error) {
	return &types.FPSSummary{}, nil
}
func (n *NoopAnalyticsRepo) ListFPSHistory(_ context.Context, _ types.QueryParams) ([]types.FPSBucket, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetLatencySummary(_ context.Context, _ types.QueryParams) (*types.LatencySummary, error) {
	return &types.LatencySummary{}, nil
}
func (n *NoopAnalyticsRepo) GetWebRTCQuality(_ context.Context, _ types.QueryParams) (*types.WebRTCQuality, error) {
	return &types.WebRTCQuality{}, nil
}
func (n *NoopAnalyticsRepo) GetPaymentSummary(_ context.Context, _ types.QueryParams) (*types.PaymentSummary, error) {
	return &types.PaymentSummary{ByOrg: map[string]types.OrgPaymentTotal{}}, nil
}
func (n *NoopAnalyticsRepo) ListPaymentHistory(_ context.Context, _ types.QueryParams) ([]types.PaymentBucket, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPaymentsByPipeline(_ context.Context, _ types.QueryParams) ([]types.PipelinePayment, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPaymentsByOrch(_ context.Context, _ types.QueryParams) ([]types.OrchPayment, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetReliabilitySummary(_ context.Context, _ types.QueryParams) (*types.ReliabilitySummary, error) {
	return &types.ReliabilitySummary{}, nil
}
func (n *NoopAnalyticsRepo) ListReliabilityHistory(_ context.Context, _ types.QueryParams) ([]types.ReliabilityBucket, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListOrchReliability(_ context.Context, _ types.QueryParams) ([]types.OrchReliability, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListFailures(_ context.Context, _ types.QueryParams) ([]types.FailureEvent, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetLeaderboard(_ context.Context, _ types.QueryParams) ([]types.LeaderboardEntry, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetOrchProfile(_ context.Context, _ string) (*types.OrchProfile, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListGateways(_ context.Context, _ types.QueryParams) ([]types.Gateway, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetGatewayProfile(_ context.Context, _ string) (*types.GatewayProfile, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListGatewayOrchestrators(_ context.Context, _ string, _ types.QueryParams) ([]types.GatewayOrch, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListStreamSamples(_ context.Context, _ types.QueryParams) ([]types.StreamStatusSample, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetStreamDetail(_ context.Context, _ string) (*types.StreamDetail, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetAttributionSummary(_ context.Context, _ types.QueryParams) (*types.AttributionSummary, error) {
	return &types.AttributionSummary{}, nil
}
func (n *NoopAnalyticsRepo) GetE2ELatencySummary(_ context.Context, _ types.QueryParams) (*types.E2ELatencySummary, error) {
	return &types.E2ELatencySummary{ByPipeline: []types.PipelineE2ELatency{}, ByOrchestrator: []types.OrchE2ELatency{}}, nil
}
func (n *NoopAnalyticsRepo) ListE2ELatencyHistory(_ context.Context, _ types.QueryParams) ([]types.E2ELatencyBucket, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPipelines(_ context.Context, _ types.QueryParams) ([]types.PipelineSummary, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetPipelineDetail(_ context.Context, _ string, _ types.QueryParams) (*types.PipelineDetail, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPricing(_ context.Context, _ types.QueryParams) ([]types.OrchPricingEntry, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetOrchPricingProfile(_ context.Context, _ string) (*types.OrchPricingProfile, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListModelPerformance(_ context.Context, _ types.QueryParams) ([]types.ModelPerformance, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetModelDetail(_ context.Context, _ string, _ types.QueryParams) (*types.ModelDetail, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPaymentsByGateway(_ context.Context, _ types.QueryParams) ([]types.GatewayPayment, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListPaymentsByStream(_ context.Context, _ types.QueryParams) ([]types.StreamPayment, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListFailuresByPipeline(_ context.Context, _ types.QueryParams) ([]types.FailuresByPipeline, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) ListFailuresByOrch(_ context.Context, _ types.QueryParams) ([]types.FailuresByOrch, error) {
	return nil, nil
}
func (n *NoopAnalyticsRepo) GetCapacitySummary(_ context.Context, _ types.QueryParams) (*types.CapacitySummary, error) {
	return &types.CapacitySummary{Entries: []types.CapacityEntry{}}, nil
}
func (n *NoopAnalyticsRepo) Ping(_ context.Context) error { return nil }
