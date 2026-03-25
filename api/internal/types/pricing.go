package types

// ModelPricing holds pricing for one model offered by an orchestrator.
type ModelPricing struct {
	ModelID       string
	PricePerUnit  int64
	PixelsPerUnit int64
	IsWarm        bool
}

// PipelinePricing groups model pricing under a pipeline.
type PipelinePricing struct {
	Pipeline string
	Models   []ModelPricing
}

// OrchPricingEntry is one flat row for GET /v1/net/pricing (PRICE-001).
type OrchPricingEntry struct {
	OrchAddress   string
	Name          string
	Pipeline      string
	ModelID       string
	PricePerUnit  int64
	PixelsPerUnit int64
	IsWarm        bool
}

// OrchPricingProfile is the response for GET /v1/net/pricing/{address} (PRICE-002).
type OrchPricingProfile struct {
	OrchAddress string
	Name        string
	IsActive    bool
	Pipelines   []PipelinePricing
}
