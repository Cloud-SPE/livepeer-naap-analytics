package types

// GatewayPayment is one row for GET /v1/payments/by-gateway (GPAY-001).
type GatewayPayment struct {
	GatewayAddress string
	Name           string
	TotalWEI       WEI
	EventCount     int64
	UniqueOrchs    int64
}

// StreamPayment is one row for GET /v1/payments/by-stream (GPAY-002).
type StreamPayment struct {
	StreamID   string
	Org        string
	Pipeline   string
	TotalWEI   WEI
	EventCount int64
}
