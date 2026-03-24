package types

import "time"

// OrgPaymentTotal breaks down payment totals by org.
type OrgPaymentTotal struct {
	TotalWEI WEI
	Count    int64
}

// PaymentSummary is the response payload for GET /v1/payments/summary (PAY-001).
type PaymentSummary struct {
	StartTime           time.Time
	EndTime             time.Time
	TotalPaymentsWEI    WEI
	PaymentEventCount   int64
	UniqueOrchestrators int64
	ByOrg               map[string]OrgPaymentTotal
}

// PaymentBucket is one time-bucket for GET /v1/payments/history (PAY-002).
type PaymentBucket struct {
	Timestamp   time.Time
	TotalWEI    WEI
	EventCount  int64
	UniqueOrchs int64
}

// PipelinePayment is one pipeline row for GET /v1/payments/by-pipeline (PAY-003).
type PipelinePayment struct {
	Pipeline            string
	TotalWEI            WEI
	EventCount          int64
	AvgPriceWeiPerPixel float64
}

// OrchPayment is one orch row for GET /v1/payments/by-orchestrator (PAY-004).
type OrchPayment struct {
	Address          string
	TotalReceivedWEI WEI
	PaymentCount     int64
}
