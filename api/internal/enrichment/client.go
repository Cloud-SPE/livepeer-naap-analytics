// Package enrichment periodically fetches orchestrator and gateway metadata
// from the Livepeer API and upserts it into ClickHouse enrichment tables.
package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// OrchestratorRecord is one item from /api/orchestrator.
type OrchestratorRecord struct {
	EthAddress       string  `json:"eth_address"`
	Name             string  `json:"name"`
	ServiceURI       string  `json:"service_uri"`
	Avatar           string  `json:"avatar"`
	TotalStake       float64 `json:"total_stake"`
	RewardCut        float64 `json:"reward_cut"`
	FeeCut           float64 `json:"fee_cut"`
	ActivationStatus bool    `json:"activation_status"`
}

// GatewayRecord is one item from /api/gateways.
type GatewayRecord struct {
	EthAddress string  `json:"eth_address"`
	Name       string  `json:"name"`
	Avatar     string  `json:"avatar"`
	Deposit    float64 `json:"deposit"`
	Reserve    float64 `json:"reserve"`
}

// Client fetches metadata from the Livepeer public API.
type Client struct {
	baseURL string
	http    *http.Client
}

// newClient creates a Client with a sensible timeout.
func newClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

// FetchOrchestrators calls /api/orchestrator and returns all records.
func (c *Client) FetchOrchestrators(ctx context.Context) ([]OrchestratorRecord, error) {
	var out []OrchestratorRecord
	if err := c.get(ctx, "/api/orchestrator", &out); err != nil {
		return nil, fmt.Errorf("fetch orchestrators: %w", err)
	}
	return out, nil
}

// FetchGateways calls /api/gateways and returns all records.
func (c *Client) FetchGateways(ctx context.Context) ([]GatewayRecord, error) {
	var out []GatewayRecord
	if err := c.get(ctx, "/api/gateways", &out); err != nil {
		return nil, fmt.Errorf("fetch gateways: %w", err)
	}
	return out, nil
}

func (c *Client) get(ctx context.Context, path string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}
