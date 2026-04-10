package runtime

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseSLAComplianceParams_DefaultWindowMatchesDemandPeriod(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/sla/compliance", nil)

	params, err := parseSLAComplianceParams(req)
	if err != nil {
		t.Fatalf("parseSLAComplianceParams: %v", err)
	}
	window := params.End.Sub(params.Start)

	if window < (3*time.Hour-time.Second) || window > (3*time.Hour+time.Second) {
		t.Fatalf("expected ~3h default window, got %s", window)
	}
}
