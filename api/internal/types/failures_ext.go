package types

// FailuresByPipeline is one row for GET /v1/failures/by-pipeline (FAGG-001).
type FailuresByPipeline struct {
	Pipeline              string
	NoOrchCount           int64
	InferenceErrorCount   int64
	OrchSwapCount         int64
	InferenceRestartCount int64
	TotalFailures         int64
	FailureRate           float64 // total failures / requested sessions
}

// FailuresByOrch is one row for GET /v1/failures/by-orch (FAGG-002).
type FailuresByOrch struct {
	OrchAddress       string
	Name              string
	InferenceErrors   int64
	InferenceRestarts int64
	TotalFailures     int64
	StreamsHandled    int64
	FailureRate       float64
}
