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
func (n *NoopAnalyticsRepo) Ping(_ context.Context) error { return nil }
