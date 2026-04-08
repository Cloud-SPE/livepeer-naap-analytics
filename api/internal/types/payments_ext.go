package types

// GatewayPayment holds payment totals aggregated by gateway address (GPAY-001).
type GatewayPayment struct {
	GatewayAddress string
	Name           string
	TotalWEI       WEI
	EventCount     int64
	UniqueOrchs    int64
}

// StreamPayment holds payment totals per stream session (GPAY-002).
type StreamPayment struct {
	StreamID   string
	Org        string
	Pipeline   string
	TotalWEI   WEI
	EventCount int64
}
