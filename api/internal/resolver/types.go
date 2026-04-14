package resolver

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Mode string

const (
	ModeAuto         Mode = "auto"
	ModeBootstrap    Mode = "bootstrap"
	ModeTail         Mode = "tail"
	ModeBackfill     Mode = "backfill"
	ModeRepairWindow Mode = "repair-window"
	ModeVerify       Mode = "verify"
)

type RunRequest struct {
	Mode                Mode
	Org                 string
	ExcludedOrgPrefixes []string
	Start               *time.Time
	End                 *time.Time
	Step                time.Duration
	DryRun              bool
}

type WindowSpec struct {
	Org                 string
	ExcludedOrgPrefixes []string
	Start               *time.Time
	End                 *time.Time
}

func (r RunRequest) Mutates() bool {
	return r.Mode != ModeVerify && !r.DryRun
}

type RunStats struct {
	SelectionEvents     int
	CapabilityVersions  int
	CapabilityIntervals int
	Decisions           int
	SessionRows         int
	StatusHourRows      int
	AIBatchJobRows      int
	BYOCJobRows         int
}

type selectionCandidate struct {
	Org            string
	SessionKey     string
	EventID        string
	EventType      string
	EventTS        time.Time
	SourceTopic    string
	SourcePart     int32
	SourceOffset   int64
	SourcePriority uint8
	OrchAddress    string
	OrchURL        string
	PipelineHint   string
	ModelHint      string
	ExplicitSwap   bool
}

type eventLineage struct {
	SourceTopic  string
	SourcePart   int32
	SourceOffset int64
}

type SelectionEvent struct {
	ID                   string
	Org                  string
	SessionKey           string
	Seq                  uint32
	SelectionTS          time.Time
	Trigger              string
	ObservedAddress      string
	ObservedURL          string
	ObservedModelHint    string
	ObservedPipeline     string
	PipelineHintVerbatim bool
	AnchorEventID        string
	AnchorEventType      string
	AnchorEventTS        time.Time
	SourceTopic          string
	SourcePartition      int32
	SourceOffset         int64
	InputHash            string
}

type capabilitySnapshot struct {
	Org          string
	OrchAddress  string
	OrchURI      string
	OrchURINorm  string
	LocalAddress string
	EventID      string
	EventTS      time.Time
	RawPayload   string
}

type CapabilityVersion struct {
	ID              string
	Org             string
	OrchAddress     string
	OrchURI         string
	OrchURINorm     string
	LocalAddress    string
	SnapshotEventID string
	SnapshotTS      time.Time
	PayloadHash     string
	RawCapabilities string
	IsNoop          bool
	VersionRank     uint32
}

type CapabilityInterval struct {
	VersionID       string
	Org             string
	OrchAddress     string
	AliasAddress    string
	OrchURI         string
	OrchURINorm     string
	ValidFromTS     time.Time
	ValidToTS       *time.Time
	Pipeline        string
	Model           string
	GPUID           string
	GPUModelName    string
	GPUMemoryTotal  *uint64
	HardwarePresent bool
	IntervalHash    string
	SnapshotEventID string
	SnapshotTS      time.Time
}

type SelectionDecision struct {
	ID                    string
	SelectionEventID      string
	Org                   string
	SessionKey            string
	SelectionTS           time.Time
	Status                string
	Reason                string
	Method                string
	Confidence            string
	CapabilityVersionID   string
	SnapshotEventID       string
	SnapshotTS            *time.Time
	AttributedOrchAddress string
	AttributedOrchURI     string
	CanonicalPipeline     string
	CanonicalModel        string
	GPUID                 string
	GPUModelName          string
	GPUMemoryBytesTotal   *uint64
	InputHash             string
}

type SessionEvidence struct {
	Org                        string
	SessionKey                 string
	StreamID                   string
	RequestID                  string
	Gateway                    string
	StartedAt                  *time.Time
	FirstProcessedAt           *time.Time
	FewProcessedAt             *time.Time
	FirstIngestAt              *time.Time
	RunnerFirstProcessedAt     *time.Time
	StatusStartTime            *time.Time
	StartedCount               uint64
	PlayableSeenCount          uint64
	NoOrchCount                uint64
	CompletedCount             uint64
	SwapCount                  uint64
	TraceLastSeen              *time.Time
	TracePipelineHint          string
	RestartSeenCount           uint64
	ErrorSeenCount             uint64
	DegradedInputSeenCount     uint64
	DegradedInferenceSeenCount uint64
	StatusSampleCount          uint64
	StatusErrorSampleCount     uint64
	OnlineSeenCount            uint64
	PositiveOutputSeenCount    uint64
	RunningStateSamplesCount   uint64
	StatusLastSeen             *time.Time
	StatusPipelineHint         string
	EventPipelineHint          string
	EventLastSeen              *time.Time
	StartupErrorCount          uint64
	ExcusableErrorCount        uint64
}

type SessionCurrentRow struct {
	SessionKey                string
	Org                       string
	StreamID                  string
	RequestID                 string
	Gateway                   string
	CurrentSelectionEventID   string
	CurrentSelectionTS        *time.Time
	CanonicalPipeline         string
	CanonicalModel            string
	GPUID                     string
	StartedAt                 *time.Time
	LastSeen                  time.Time
	StartupLatencyMS          *float64
	E2ELatencyMS              *float64
	PromptToPlayableLatencyMS *float64
	RequestedSeen             uint8
	PlayableSeen              uint8
	SelectionOutcome          string
	Completed                 uint8
	SwapCount                 uint64
	RestartSeen               uint8
	ErrorSeen                 uint8
	DegradedInputSeen         uint8
	DegradedInferenceSeen     uint8
	StatusSampleCount         uint64
	StatusErrorSampleCount    uint64
	StartupErrorCount         uint64
	ExcusableErrorCount       uint64
	LoadingOnlySession        uint8
	ZeroOutputFPSSession      uint8
	HealthSignalCount         uint64
	HealthExpectedSignalCount uint64
	HealthSignalCoverageRatio float64
	StartupOutcome            string
	ExcusalReason             string
	HasAmbiguousIdentity      uint8
	HasSnapshotMatch          uint8
	IsHardwareLess            uint8
	IsStale                   uint8
	AttributionReason         string
	AttributionStatus         string
	AttributedOrchAddress     string
	AttributedOrchURI         string
	AttributionSnapshotTS     *time.Time
}

type StatusHourRow struct {
	SessionKey                string
	Org                       string
	Hour                      time.Time
	StreamID                  string
	RequestID                 string
	CanonicalPipeline         string
	CanonicalModel            string
	OrchAddress               string
	AttributionStatus         string
	AttributionReason         string
	StartedAt                 *time.Time
	SessionLastSeen           time.Time
	StartupLatencyMS          *float64
	E2ELatencyMS              *float64
	PromptToPlayableLatencyMS *float64
	StatusSamples             uint64
	FPSPositiveSamples        uint64
	RunningStateSamples       uint64
	DegradedInputSamples      uint64
	DegradedInferenceSamples  uint64
	ErrorSamples              uint64
	AvgOutputFPS              float64
	AvgInputFPS               float64
	IsTerminalTailArtifact    uint8
}

type statusHourEvidence struct {
	Org                      string
	SessionKey               string
	Hour                     time.Time
	StreamID                 string
	RequestID                string
	StatusSamples            uint64
	FPSPositiveSamples       uint64
	RunningStateSamples      uint64
	DegradedInputSamples     uint64
	DegradedInferenceSamples uint64
	ErrorSamples             uint64
	OutputFPSSum             float64
	InputFPSSum              float64
}

type windowClaim struct {
	ClaimKey       string
	ClaimType      string
	Mode           string
	Org            string
	OwnerID        string
	WindowStart    time.Time
	WindowEnd      time.Time
	LeaseExpiresAt time.Time
	CreatedAt      time.Time
	ReleasedAt     *time.Time
}

type dirtyPartition struct {
	Org              string
	EventDate        time.Time
	Status           string
	Reason           string
	FirstDirtyAt     time.Time
	LastDirtyAt      time.Time
	ClaimOwner       string
	LeaseExpiresAt   *time.Time
	AttemptCount     uint32
	LastErrorSummary string
	UpdatedAt        time.Time
}

type dirtyWindow struct {
	Org              string
	WindowStart      time.Time
	WindowEnd        time.Time
	Status           string
	Reason           string
	FirstDirtyAt     time.Time
	LastDirtyAt      time.Time
	ClaimOwner       string
	LeaseExpiresAt   *time.Time
	AttemptCount     uint32
	LastErrorSummary string
	UpdatedAt        time.Time
}

// AIBatchJobRecord holds the raw job data fetched from normalized_ai_batch_job.
// OrchURL is always sourced from the completed_at event; ReceivedAt is from the
// received event and is used as SelectionTS when available.
type AIBatchJobRecord struct {
	RequestID    string
	Org          string
	Gateway      string
	Pipeline     string
	ModelID      string
	OrchURL      string
	OrchURLNorm  string
	ReceivedAt   *time.Time
	CompletedAt  time.Time
	Success      *uint8
	Tries        uint16
	DurationMS   int64
	LatencyScore float64
	PricePerUnit float64
	ErrorType    string
	Error        string
}

// AIBatchJobRow is an AIBatchJobRecord enriched with attribution outputs.
type AIBatchJobRow struct {
	AIBatchJobRecord
	SelectionOutcome      string
	AttributionStatus     string
	AttributionReason     string
	AttributionMethod     string
	AttributionConfidence string
	AttributedOrchURI     string
	CapabilityVersionID   string
	AttributionSnapshotTS *time.Time
	GPUID                 string
	GPUModelName          string
	GPUMemoryBytesTotal   *uint64
	AttributedModel       string
}

// BYOCJobRecord holds the raw job data fetched from normalized_byoc_job.
type BYOCJobRecord struct {
	EventID        string
	Org            string
	Gateway        string
	Capability     string
	OrchAddress    string
	OrchURL        string
	OrchURLNorm    string
	WorkerURL      string
	ChargedCompute uint8
	CompletedAt    time.Time
	Success        *uint8
	DurationMS     int64
	HTTPStatus     uint16
	Error          string
}

// BYOCJobRow is a BYOCJobRecord enriched with attribution and model outputs.
type BYOCJobRow struct {
	BYOCJobRecord
	SelectionOutcome      string
	Model                 string
	PricePerUnit          float64
	AttributionStatus     string
	AttributionReason     string
	AttributionMethod     string
	AttributionConfidence string
	AttributedOrchURI     string
	CapabilityVersionID   string
	AttributionSnapshotTS *time.Time
	GPUID                 string
	GPUModelName          string
	GPUMemoryBytesTotal   *uint64
}

// workerLifecycleSnapshot is a point-in-time view of a BYOC worker's model
// and pricing, used to resolve model identity for BYOC jobs. The most recent
// snapshot where EventTS <= job.CompletedAt takes precedence.
type workerLifecycleSnapshot struct {
	Org          string
	Capability   string
	OrchAddress  string
	EventTS      time.Time
	Model        string
	PricePerUnit float64
	WorkerURL    string
}

type dirtyScanWatermark struct {
	IngestedAt time.Time
	EventID    string
}

type dirtyScanResult struct {
	HistoricalPartitions []backfillPartition
	SameDayWindows       []dirtyWindow
	Watermark            *dirtyScanWatermark
}

type repairRequest struct {
	RequestID        string
	Org              string
	WindowStart      time.Time
	WindowEnd        time.Time
	Status           string
	RequestedBy      string
	Reason           string
	DryRun           bool
	ClaimOwner       string
	LeaseExpiresAt   *time.Time
	AttemptCount     uint32
	StartedAt        *time.Time
	FinishedAt       *time.Time
	LastErrorSummary string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type schedulerHealthState struct {
	Status                    string                  `json:"status"`
	Mode                      string                  `json:"mode,omitempty"`
	Phase                     string                  `json:"phase,omitempty"`
	HistoricalDirtyQueueDepth uint64                  `json:"historical_dirty_queue_depth,omitempty"`
	SameDayDirtyQueueDepth    uint64                  `json:"same_day_dirty_queue_depth,omitempty"`
	RepairRequestQueueDepth   uint64                  `json:"repair_request_queue_depth,omitempty"`
	AcceptedRawScanWatermark  *dirtyScanWatermark     `json:"accepted_raw_scan_watermark,omitempty"`
	TailWatermark             *time.Time              `json:"tail_watermark,omitempty"`
	ActiveClaim               *schedulerActiveClaim   `json:"active_claim,omitempty"`
	ActiveRepairRequest       *schedulerRepairRequest `json:"active_repair_request,omitempty"`
}

type schedulerActiveClaim struct {
	Mode        string    `json:"mode"`
	Org         string    `json:"org,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	OwnerID     string    `json:"owner_id"`
}

type schedulerRepairRequest struct {
	RequestID   string    `json:"request_id"`
	Org         string    `json:"org,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Status      string    `json:"status"`
	DryRun      bool      `json:"dry_run"`
}

func stableHash(parts ...string) string {
	h := md5.Sum([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:])
}

func normalizeURL(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func normalizeAddress(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func isHexAddressIdentity(v string) bool {
	v = normalizeAddress(v)
	if len(v) != 42 || !strings.HasPrefix(v, "0x") {
		return false
	}
	for _, r := range v[2:] {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func candidateIdentity(address, url string) string {
	if id := normalizeAddress(address); id != "" {
		return id
	}
	return normalizeURL(url)
}

func sortSelectionCandidates(rows []selectionCandidate) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		switch {
		case a.Org != b.Org:
			return a.Org < b.Org
		case a.SessionKey != b.SessionKey:
			return a.SessionKey < b.SessionKey
		case !a.EventTS.Equal(b.EventTS):
			return a.EventTS.Before(b.EventTS)
		case a.SourcePriority != b.SourcePriority:
			return a.SourcePriority < b.SourcePriority
		case a.SourcePart != b.SourcePart:
			return a.SourcePart < b.SourcePart
		case a.SourceOffset != b.SourceOffset:
			return a.SourceOffset < b.SourceOffset
		default:
			return a.EventID < b.EventID
		}
	})
}

func sortCapabilitySnapshots(rows []capabilitySnapshot) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		switch {
		case a.Org != b.Org:
			return a.Org < b.Org
		case a.OrchAddress != b.OrchAddress:
			return a.OrchAddress < b.OrchAddress
		case a.OrchURINorm != b.OrchURINorm:
			return a.OrchURINorm < b.OrchURINorm
		case !a.EventTS.Equal(b.EventTS):
			return a.EventTS.Before(b.EventTS)
		default:
			return a.EventID < b.EventID
		}
	})
}

func ptrTime(v time.Time) *time.Time {
	return &v
}

func windowRangeLabel(start, end *time.Time) string {
	var s, e string
	if start != nil {
		s = start.UTC().Format(time.RFC3339)
	}
	if end != nil {
		e = end.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("%s..%s", s, e)
}
