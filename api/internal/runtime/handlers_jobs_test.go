package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
