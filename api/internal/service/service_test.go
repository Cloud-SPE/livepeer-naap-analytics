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

func defaultWindowParams() types.TimeWindowParams {
	return types.TimeWindowParams{
		Start: time.Now().Add(-1 * time.Hour),
		End:   time.Now(),
		Limit: 10,
	}
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
	p := defaultWindowParams()

	if _, err := svc.GetStreamingModels(context.Background()); err != nil {
		t.Fatalf("GetStreamingModels() err=%v", err)
	}
	if _, err := svc.GetStreamingOrchestrators(context.Background()); err != nil {
		t.Fatalf("GetStreamingOrchestrators() err=%v", err)
	}
	if _, _, err := svc.ListStreamingSLA(context.Background(), p); err != nil {
		t.Fatalf("ListStreamingSLA() err=%v", err)
	}
	if _, _, err := svc.ListStreamingDemand(context.Background(), p); err != nil {
		t.Fatalf("ListStreamingDemand() err=%v", err)
	}
	if _, _, err := svc.ListStreamingGPUMetrics(context.Background(), p); err != nil {
		t.Fatalf("ListStreamingGPUMetrics() err=%v", err)
	}
}

func TestRequestsMethods_Noop(t *testing.T) {
	svc := newNoopSvc()
	p := defaultWindowParams()

	if _, err := svc.GetRequestsModels(context.Background()); err != nil {
		t.Fatalf("GetRequestsModels() err=%v", err)
	}
	if _, err := svc.GetRequestsOrchestrators(context.Background()); err != nil {
		t.Fatalf("GetRequestsOrchestrators() err=%v", err)
	}
	if _, err := svc.GetAIBatchSummary(context.Background(), p); err != nil {
		t.Fatalf("GetAIBatchSummary() err=%v", err)
	}
	if _, _, err := svc.ListAIBatchJobs(context.Background(), p); err != nil {
		t.Fatalf("ListAIBatchJobs() err=%v", err)
	}
	if _, err := svc.GetAIBatchLLMSummary(context.Background(), p); err != nil {
		t.Fatalf("GetAIBatchLLMSummary() err=%v", err)
	}
	if _, err := svc.GetBYOCSummary(context.Background(), p); err != nil {
		t.Fatalf("GetBYOCSummary() err=%v", err)
	}
	if _, _, err := svc.ListBYOCJobs(context.Background(), p); err != nil {
		t.Fatalf("ListBYOCJobs() err=%v", err)
	}
	if _, err := svc.GetBYOCWorkers(context.Background(), p); err != nil {
		t.Fatalf("GetBYOCWorkers() err=%v", err)
	}
	if _, err := svc.GetBYOCAuth(context.Background(), p); err != nil {
		t.Fatalf("GetBYOCAuth() err=%v", err)
	}
}

func TestPing_Noop(t *testing.T) {
	svc := newNoopSvc()
	if err := svc.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() err=%v", err)
	}
}
