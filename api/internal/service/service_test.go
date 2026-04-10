package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/service"
	"github.com/livepeer/naap-analytics/internal/types"
)

func newNoopSvc() service.AnalyticsService {
	return service.New(&repo.NoopAnalyticsRepo{})
}

func defaultParams() types.QueryParams {
	return types.QueryParams{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Limit:     10,
	}
}

func TestGetNetworkSummary_Noop(t *testing.T) {
	svc := newNoopSvc()
	v, err := svc.GetNetworkSummary(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestListOrchestrators_Noop(t *testing.T) {
	svc := newNoopSvc()
	_, _, err := svc.ListOrchestrators(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetActiveStreams_Noop(t *testing.T) {
	svc := newNoopSvc()
	v, err := svc.GetActiveStreams(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestGetLeaderboard_Noop(t *testing.T) {
	svc := newNoopSvc()
	_, err := svc.GetLeaderboard(context.Background(), defaultParams())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPing_Noop(t *testing.T) {
	svc := newNoopSvc()
	if err := svc.Ping(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
