package service_test

import (
	"context"
	"testing"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/service"
	"github.com/livepeer/naap-analytics/internal/types"
)

func newNoopSvc() service.AnalyticsService {
	return service.New(&repo.NoopAnalyticsRepo{})
}

func TestDashboardMethods_Noop(t *testing.T) {
	svc := newNoopSvc()

	if v, err := svc.GetDashboardKPI(context.Background(), 24, "", ""); err != nil || v == nil {
		t.Fatalf("GetDashboardKPI() err=%v value=%v", err, v)
	}
	if _, err := svc.GetDashboardPipelines(context.Background(), 10, 24); err != nil {
		t.Fatalf("GetDashboardPipelines() err=%v", err)
	}
	if _, err := svc.GetDashboardOrchestrators(context.Background(), 24); err != nil {
		t.Fatalf("GetDashboardOrchestrators() err=%v", err)
	}
	if v, err := svc.GetDashboardGPUCapacity(context.Background()); err != nil || v == nil {
		t.Fatalf("GetDashboardGPUCapacity() err=%v value=%v", err, v)
	}
	if _, err := svc.GetDashboardPipelineCatalog(context.Background()); err != nil {
		t.Fatalf("GetDashboardPipelineCatalog() err=%v", err)
	}
	if _, err := svc.GetDashboardPricing(context.Background()); err != nil {
		t.Fatalf("GetDashboardPricing() err=%v", err)
	}
	if _, err := svc.GetDashboardJobFeed(context.Background(), 10); err != nil {
		t.Fatalf("GetDashboardJobFeed() err=%v", err)
	}
	if v, err := svc.GetDashboardJobsOverview(context.Background(), 24); err != nil || v == nil {
		t.Fatalf("GetDashboardJobsOverview() err=%v value=%v", err, v)
	}
	if _, err := svc.GetDashboardJobsByPipeline(context.Background(), 24); err != nil {
		t.Fatalf("GetDashboardJobsByPipeline() err=%v", err)
	}
	if _, err := svc.GetDashboardJobsByCapability(context.Background(), 24); err != nil {
		t.Fatalf("GetDashboardJobsByCapability() err=%v", err)
	}
}

func TestStreamingMethods_Noop(t *testing.T) {
	svc := newNoopSvc()

	if _, err := svc.GetStreamingModels(context.Background()); err != nil {
		t.Fatalf("GetStreamingModels() err=%v", err)
	}
}

func TestRequestsMethods_Noop(t *testing.T) {
	svc := newNoopSvc()

	if _, err := svc.GetRequestsModels(context.Background()); err != nil {
		t.Fatalf("GetRequestsModels() err=%v", err)
	}
}

func TestDiscoverMethods_Noop(t *testing.T) {
	svc := newNoopSvc()

	if _, err := svc.DiscoverOrchestrators(context.Background(), types.DiscoverOrchestratorsParams{}); err != nil {
		t.Fatalf("DiscoverOrchestrators() err=%v", err)
	}
}

func TestPing_Noop(t *testing.T) {
	svc := newNoopSvc()
	if err := svc.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() err=%v", err)
	}
}
