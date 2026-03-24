package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/service"
	"github.com/livepeer/naap-analytics/internal/types"
)

func TestQueryWindows_Noop(t *testing.T) {
	svc := service.New(&repo.NoopAnalyticsRepo{})
	params := types.QueryParams{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Limit:     10,
	}

	windows, err := svc.QueryWindows(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if windows == nil {
		t.Fatal("expected non-nil slice")
	}
}

func TestQueryAlerts_Noop(t *testing.T) {
	svc := service.New(&repo.NoopAnalyticsRepo{})
	params := types.QueryParams{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Limit:     10,
	}

	alerts, err := svc.QueryAlerts(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if alerts == nil {
		t.Fatal("expected non-nil slice")
	}
}
