package types

import "time"

// Gateway is one entry for GET /v1/net/gateways (GAT-001).
type Gateway struct {
	Address      string
	Name         string
	Avatar       string
	DepositWEI   WEI
	ReserveWEI   WEI
	UpdatedAt    time.Time
	ActiveStreams int64
}

// GatewayProfile is the response for GET /v1/net/gateways/{address} (GAT-002).
type GatewayProfile struct {
	Address          string
	Name             string
	Avatar           string
	DepositWEI       WEI
	ReserveWEI       WEI
	UpdatedAt        time.Time
	StreamsRouted    int64
	ActiveStreams     int64
	TotalPaymentsWEI WEI
	OrchsUsed        []string
}

// GatewayOrch is one orch row for GET /v1/net/gateways/{address}/orchs (GAT-003).
type GatewayOrch struct {
	OrchAddress string
	Name        string
	StreamCount int64
	LastSeen    time.Time
}
