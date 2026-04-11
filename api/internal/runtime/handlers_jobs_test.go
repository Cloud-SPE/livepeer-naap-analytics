package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livepeer/naap-analytics/internal/repo"
	"github.com/livepeer/naap-analytics/internal/types"
)

func TestListAIBatchJobs_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/ai-batch/jobs?limit=2", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON object: %v", err)
	}
	assertCursorEnvelope(t, body)
}

func TestListAIBatchJobs_RejectsLegacyPaginationParams(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/ai-batch/jobs?offset=1", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `query parameter "offset" is no longer supported; use limit and cursor`)
}

func TestListAIBatchJobs_InvalidCursor(t *testing.T) {
	srv := newTestServerWithRepo(t, &invalidCursorJobRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/ai-batch/jobs?cursor=bad-token", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, types.ErrInvalidCursor.Error())
}

func TestListBYOCJobs_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/byoc/jobs?limit=2", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON object: %v", err)
	}
	assertCursorEnvelope(t, body)
}

func TestJobAPIs_ExposeSelectionOutcomeFields(t *testing.T) {
	srv := newTestServerWithRepo(t, &jobSelectionOutcomeRepo{})

	tests := []struct {
		path      string
		dataField string
		kind      string
	}{
		{path: "/v1/ai-batch/jobs", dataField: "selection_outcome", kind: "list"},
		{path: "/v1/byoc/jobs", dataField: "selection_outcome", kind: "list"},
		{path: "/v1/ai-batch/summary", dataField: "selected_attribution_worked_rate", kind: "summary"},
		{path: "/v1/byoc/summary", dataField: "selected_attribution_worked_rate", kind: "summary"},
	}

	for _, tc := range tests {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", tc.path, rr.Code)
		}
		var body map[string]any
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("%s: decode body: %v", tc.path, err)
		}
		data, ok := body["data"].([]any)
		if !ok || len(data) == 0 {
			t.Fatalf("%s: expected non-empty data array", tc.path)
		}
		first, ok := data[0].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected first row object, got %T", tc.path, data[0])
		}
		if _, ok := first[tc.dataField]; !ok {
			t.Fatalf("%s: missing field %s", tc.path, tc.dataField)
		}
		if tc.kind == "summary" {
			for _, field := range []string{"selected_jobs", "no_orch_jobs", "unknown_jobs"} {
				if _, ok := first[field]; !ok {
					t.Fatalf("%s: missing summary field %s", tc.path, field)
				}
			}
		}
	}
}

func TestDashboardJobsOverview_ExposesSelectionOutcomeBreakdown(t *testing.T) {
	srv := newTestServerWithRepo(t, &jobSelectionOutcomeRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/jobs/overview", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	data, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object, got %T", body["data"])
	}
	for _, family := range []string{"ai_batch", "byoc"} {
		stats, ok := data[family].(map[string]any)
		if !ok {
			t.Fatalf("expected %s object, got %T", family, data[family])
		}
		for _, field := range []string{"selected_jobs", "no_orch_jobs", "unknown_jobs", "selected_attribution_worked_rate"} {
			if _, ok := stats[field]; !ok {
				t.Fatalf("%s missing field %s", family, field)
			}
		}
	}
}

func TestJobsDemand_HappyPath(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/demand?limit=5", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON object: %v", err)
	}
	assertCursorEnvelope(t, body)
}

func TestJobsDemand_RejectsLegacyPaginationParams(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/demand?page=1", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, `query parameter "page" is no longer supported; use limit and cursor`)
}

func TestJobsSLA_InvalidCursor(t *testing.T) {
	srv := newTestServerWithRepo(t, &invalidCursorJobRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/sla?cursor=bad-token", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	assertProblemDetail(t, rr, types.ErrInvalidCursor.Error())
}

func TestJobsSLA_UsesOrchestratorURIQueryParam(t *testing.T) {
	repoSpy := &jobParamCaptureRepo{}
	srv := newTestServerWithRepo(t, repoSpy)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/sla?orchestrator_uri=HTTPS://ORCH.EXAMPLE.COM:8935", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if repoSpy.jobsSLAParams.OrchestratorURI != "https://orch.example.com:8935" {
		t.Fatalf("orchestrator_uri = %q", repoSpy.jobsSLAParams.OrchestratorURI)
	}
}

type invalidCursorJobRepo struct {
	repo.NoopAnalyticsRepo
}

type jobParamCaptureRepo struct {
	repo.NoopAnalyticsRepo
	jobsSLAParams types.JobsParams
}

type jobSelectionOutcomeRepo struct {
	repo.NoopAnalyticsRepo
}

func (invalidCursorJobRepo) ListAIBatchJobs(_ context.Context, p types.QueryParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.AIBatchJobRecord{}, types.CursorPageInfo{}, nil
}

func (invalidCursorJobRepo) ListBYOCJobs(_ context.Context, p types.QueryParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.BYOCJobRecord{}, types.CursorPageInfo{}, nil
}

func (invalidCursorJobRepo) ListJobsDemand(_ context.Context, p types.JobsParams) ([]types.JobsDemandRow, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.JobsDemandRow{}, types.CursorPageInfo{}, nil
}

func (invalidCursorJobRepo) ListJobsSLA(_ context.Context, p types.JobsParams) ([]types.JobsSLARow, types.CursorPageInfo, error) {
	if p.Cursor != "" {
		return nil, types.CursorPageInfo{}, types.ErrInvalidCursor
	}
	return []types.JobsSLARow{}, types.CursorPageInfo{}, nil
}

func (r *jobParamCaptureRepo) ListJobsSLA(_ context.Context, p types.JobsParams) ([]types.JobsSLARow, types.CursorPageInfo, error) {
	r.jobsSLAParams = p
	return []types.JobsSLARow{}, types.CursorPageInfo{}, nil
}

func (jobSelectionOutcomeRepo) ListAIBatchJobs(_ context.Context, _ types.QueryParams) ([]types.AIBatchJobRecord, types.CursorPageInfo, error) {
	return []types.AIBatchJobRecord{{
		RequestID:         "req-1",
		Org:               "acme",
		Gateway:           "gw",
		Pipeline:          "llm",
		ModelID:           "model-a",
		SelectionOutcome:  "selected",
		CompletedAt:       time.Now().UTC(),
		Tries:             1,
		DurationMs:        1000,
		OrchURL:           "https://orch.example.com",
		LatencyScore:      0.9,
		PricePerUnit:      0.1,
		AttributionStatus: "resolved",
	}}, types.CursorPageInfo{}, nil
}

func (jobSelectionOutcomeRepo) ListBYOCJobs(_ context.Context, _ types.QueryParams) ([]types.BYOCJobRecord, types.CursorPageInfo, error) {
	return []types.BYOCJobRecord{{
		RequestID:         "evt-1",
		Org:               "acme",
		Capability:        "openai-chat-completions",
		SelectionOutcome:  "unknown",
		CompletedAt:       time.Now().UTC(),
		DurationMs:        900,
		HTTPStatus:        500,
		OrchAddress:       "",
		OrchURL:           "",
		WorkerURL:         "",
		Error:             "no worker",
		AttributionStatus: "unresolved",
	}}, types.CursorPageInfo{}, nil
}

func (jobSelectionOutcomeRepo) GetAIBatchSummary(_ context.Context, _ types.QueryParams) ([]types.AIBatchJobSummary, error) {
	return []types.AIBatchJobSummary{{
		Pipeline:                      "llm",
		TotalJobs:                     10,
		SelectedJobs:                  7,
		NoOrchJobs:                    2,
		UnknownJobs:                   1,
		SuccessRate:                   0.8,
		AvgDurationMs:                 1100,
		AvgLatency:                    0.91,
		SelectedAttributionWorkedRate: 0.86,
	}}, nil
}

func (jobSelectionOutcomeRepo) GetBYOCSummary(_ context.Context, _ types.QueryParams) ([]types.BYOCJobSummary, error) {
	return []types.BYOCJobSummary{{
		Capability:                    "openai-chat-completions",
		TotalJobs:                     12,
		SelectedJobs:                  9,
		NoOrchJobs:                    0,
		UnknownJobs:                   3,
		SuccessRate:                   0.75,
		AvgDurationMs:                 950,
		SelectedAttributionWorkedRate: 0.78,
	}}, nil
}

func (jobSelectionOutcomeRepo) GetDashboardJobsOverview(_ context.Context, _ types.QueryParams) (*types.DashboardJobsOverview, error) {
	return &types.DashboardJobsOverview{
		AIBatch: types.DashboardJobsStats{
			TotalJobs:                     10,
			SelectedJobs:                  7,
			NoOrchJobs:                    2,
			UnknownJobs:                   1,
			SuccessRate:                   0.8,
			AvgDurationMs:                 1100,
			P99DurationMs:                 2500,
			SelectedAttributionWorkedRate: 0.86,
		},
		BYOC: types.DashboardJobsStats{
			TotalJobs:                     12,
			SelectedJobs:                  9,
			NoOrchJobs:                    0,
			UnknownJobs:                   3,
			SuccessRate:                   0.75,
			AvgDurationMs:                 950,
			P99DurationMs:                 2000,
			SelectedAttributionWorkedRate: 0.78,
		},
	}, nil
}
