package types

import "time"

// Gateway is one gateway entry (GAT-001).
type Gateway struct {
	Address      string
	Name         string
	Avatar       string
	DepositWEI   WEI
	ReserveWEI   WEI
	UpdatedAt    time.Time
	ActiveStreams int64
}

// GatewayProfile is the gateway detail profile (GAT-002).
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

// GatewayOrch is one orchestrator row associated with a gateway (GAT-003).
type GatewayOrch struct {
	OrchAddress string
	Name        string
	StreamCount int64
	LastSeen    time.Time
}
