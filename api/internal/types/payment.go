package types

import "time"

// OrgPaymentTotal breaks down payment totals by org.
type OrgPaymentTotal struct {
	TotalWEI WEI
	Count    int64
}

// PaymentSummary holds aggregate payment totals for a time window (PAY-001).
type PaymentSummary struct {
	StartTime           time.Time
	EndTime             time.Time
	TotalPaymentsWEI    WEI
	PaymentEventCount   int64
	UniqueOrchestrators int64
	ByOrg               map[string]OrgPaymentTotal
}

// PaymentBucket is one time-bucket in the payment history time-series (PAY-002).
type PaymentBucket struct {
	Timestamp   time.Time
	TotalWEI    WEI
	EventCount  int64
	UniqueOrchs int64
}

// PipelinePayment holds payment totals aggregated by pipeline (PAY-003).
type PipelinePayment struct {
	Pipeline            string
	TotalWEI            WEI
	EventCount          int64
	AvgPriceWeiPerPixel float64
}

// OrchPayment holds payment totals aggregated by orchestrator (PAY-004).
type OrchPayment struct {
	Address          string
	TotalReceivedWEI WEI
	PaymentCount     int64
}
