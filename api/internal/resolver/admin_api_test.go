package resolver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/livepeer/naap-analytics/internal/config"
)

func TestNewRepairRequestFromPayloadValidatesBounds(t *testing.T) {
	_, err := newRepairRequestFromPayload(createRepairRequestPayload{
		From:        "2026-04-13T10:00:00Z",
		To:          "2026-04-13T09:00:00Z",
		RequestedBy: "ops",
	})
	if err == nil || err.Error() != "from must be earlier than to" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRepairRequestFromPayloadRequiresRequestedBy(t *testing.T) {
	_, err := newRepairRequestFromPayload(createRepairRequestPayload{
		From: "2026-04-13T09:00:00Z",
		To:   "2026-04-13T10:00:00Z",
	})
	if err == nil || err.Error() != "requested_by is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRepairRequestFromPayloadRequiresOrgForWindowsOverOneHour(t *testing.T) {
	_, err := newRepairRequestFromPayload(createRepairRequestPayload{
		From:        "2026-04-13T09:00:00Z",
		To:          "2026-04-13T11:00:01Z",
		RequestedBy: "ops",
	})
	if err == nil || err.Error() != "org is required for repair requests over 1h; use resolver-repair-window or backfill for broader all-org recovery" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRepairRequestFromPayloadRejectsWindowsOverSixHours(t *testing.T) {
	_, err := newRepairRequestFromPayload(createRepairRequestPayload{
		Org:         "cloudspe",
		From:        "2026-04-13T09:00:00Z",
		To:          "2026-04-13T15:00:01Z",
		RequestedBy: "ops",
	})
	if err == nil || err.Error() != "repair request window must be 6h or less; use resolver-repair-window or backfill for larger ranges" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRepairRequestFromPayloadAcceptsScopedWindowUpToSixHours(t *testing.T) {
	request, err := newRepairRequestFromPayload(createRepairRequestPayload{
		Org:         "cloudspe",
		From:        "2026-04-13T09:00:00Z",
		To:          "2026-04-13T15:00:00Z",
		RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if request.Org != "cloudspe" {
		t.Fatalf("org = %q, want cloudspe", request.Org)
	}
}

func TestNewRepairRequestFromPayloadSeparatesDryRunAndMutatingIdentity(t *testing.T) {
	base := createRepairRequestPayload{
		Org:         "cloudspe",
		From:        "2026-04-13T09:00:00Z",
		To:          "2026-04-13T10:00:00Z",
		RequestedBy: "ops",
		Reason:      "incident_repair",
	}
	mutating, err := newRepairRequestFromPayload(base)
	if err != nil {
		t.Fatalf("unexpected mutating error: %v", err)
	}
	base.DryRun = true
	dryRun, err := newRepairRequestFromPayload(base)
	if err != nil {
		t.Fatalf("unexpected dry-run error: %v", err)
	}
	if mutating.RequestID == dryRun.RequestID {
		t.Fatal("dry-run and mutating requests should not share request identity")
	}
}

func TestAuthorizeAdminRequestRequiresConfiguredBearerToken(t *testing.T) {
	engine := &Engine{cfg: &config.Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/repair-requests", nil)
	if engine.authorizeAdminRequest(rec, req) {
		t.Fatal("expected auth to fail without configured token")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAuthorizeAdminRequestRejectsWrongBearerToken(t *testing.T) {
	engine := &Engine{cfg: &config.Config{ResolverAdminToken: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/repair-requests", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	if engine.authorizeAdminRequest(rec, req) {
		t.Fatal("expected auth to fail with wrong token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthorizeAdminRequestAcceptsConfiguredBearerToken(t *testing.T) {
	engine := &Engine{cfg: &config.Config{ResolverAdminToken: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/repair-requests", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer secret")
	if !engine.authorizeAdminRequest(rec, req) {
		t.Fatalf("expected auth success, got status %d", rec.Code)
	}
}
