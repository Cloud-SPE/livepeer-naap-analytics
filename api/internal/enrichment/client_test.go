package enrichment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_FetchOrchestrators(t *testing.T) {
	records := []OrchestratorRecord{
		{
			EthAddress:       "0xABC",
			Name:             "vires-in-numeris.eth",
			ServiceURI:       "https://orch.example.com:8935",
			TotalStake:       3_870_742.08,
			RewardCut:        0.05,
			FeeCut:           0.0,
			ActivationStatus: true,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/orchestrator" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(records)
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	got, err := c.FetchOrchestrators(context.Background())
	if err != nil {
		t.Fatalf("FetchOrchestrators: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	if got[0].EthAddress != "0xABC" {
		t.Errorf("EthAddress: want 0xABC, got %q", got[0].EthAddress)
	}
	if got[0].Name != "vires-in-numeris.eth" {
		t.Errorf("Name: want vires-in-numeris.eth, got %q", got[0].Name)
	}
	if !got[0].ActivationStatus {
		t.Errorf("ActivationStatus: want true, got false")
	}
	if got[0].TotalStake != 3_870_742.08 {
		t.Errorf("TotalStake: want 3870742.08, got %f", got[0].TotalStake)
	}
}

func TestClient_FetchGateways(t *testing.T) {
	records := []GatewayRecord{
		{
			EthAddress: "0xDEF",
			Name:       "Livepeer, Inc (Realtime AI Video)",
			Deposit:    14.48,
			Reserve:    1.0,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/gateways" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(records)
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	got, err := c.FetchGateways(context.Background())
	if err != nil {
		t.Fatalf("FetchGateways: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	if got[0].Name != "Livepeer, Inc (Realtime AI Video)" {
		t.Errorf("Name: want 'Livepeer, Inc (Realtime AI Video)', got %q", got[0].Name)
	}
	if got[0].Deposit != 14.48 {
		t.Errorf("Deposit: want 14.48, got %f", got[0].Deposit)
	}
}

func TestClient_FetchOrchestrators_EmptyList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	got, err := c.FetchOrchestrators(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want 0 records, got %d", len(got))
	}
}

func TestClient_FetchOrchestrators_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	_, err := c.FetchOrchestrators(context.Background())
	if err == nil {
		t.Fatal("expected error for 503 response, got nil")
	}
}

func TestClient_FetchGateways_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	_, err := c.FetchGateways(context.Background())
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

func TestClient_FetchOrchestrators_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	c := newClient(srv.URL)
	_, err := c.FetchOrchestrators(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestClient_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Handler that would return normally — cancelled before it gets there.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	c := newClient(srv.URL)
	_, err := c.FetchOrchestrators(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
