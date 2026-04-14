# Canonical Rule Requirements

This document is the standalone, architecture-independent behavioral contract for systems that ingest this raw event family and are expected to produce the same validated, clean, high-quality outputs as the current production analytics design.

It is intentionally written so a reviewer can start with raw source envelopes, normalize them, derive canonical identities and lifecycle state, and validate the final outputs without reusing the current repository structure, data model, or implementation stack.

This is the document to use when the question is:

- What raw inputs matter?
- What must be accepted, ignored, rejected, deduplicated, or normalized?
- What canonical identities and lifecycle outcomes must be derived?
- What aggregate outputs are required and how are they calculated?
- What parity and quality checks must any equivalent implementation pass?

This file does not require an implementation to use the same tables, jobs, or storage layout. It does require an implementation to preserve the same business semantics, normalization rules, error treatment, attribution behavior, temporal bucketing, and final output meanings.

## Scope And Status Labels

- `active_contracted`: explicitly required by current product and validation surfaces.
- `active_inferred`: required to reproduce current outputs and validations, but extracted from implementation/tests rather than one canonical product sentence.
- `ambiguous_needs_signoff`: current behavior exists, but the product requirement is not yet crisp enough to freeze as implementation-independent truth.
- `excluded_future`: backlog or future semantics, intentionally not part of the active contract.

## How To Use This Document

1. Classify each raw inbound envelope as accepted, ignored, or rejected.
2. Normalize accepted raw events into canonical logical records.
3. Build canonical latest-state and historical-state outputs.
4. Derive session, segment, attribution, and health semantics.
5. Produce contracted aggregate and serving outputs.
6. Validate parity from raw envelopes through final outputs using independent recomputation.

For every rule below, a compliant system must make it possible for a reviewer to answer:

1. What raw fields feed the rule?
2. What normalization or enrichment is required?
3. What output meaning is required?
4. What edge cases are intentional?
5. How can an independent validator prove the rule passed or failed?

## Canonical Raw Input Contract

The current contract is defined over these raw event families and their minimum logical elements.

| Raw family | Minimum logical raw elements | Primary semantic role | Common normalization risk |
|---|---|---|---|
| `stream_trace` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.type`, `data.stream_id`, `data.request_id`, `data.pipeline_id`, optional `data.pipeline`, `data.message`, `data.orchestrator_info{address,url}` | startup lifecycle, close edges, swap edges, denominator membership | some traces are known non-contract noise and must be ignored; raw `pipeline` can be inconsistent |
| `ai_stream_status` | envelope `id`, `timestamp`, `gateway`; payload `data.stream_id`, `data.request_id`, `data.pipeline`, `data.state`, `data.start_time`, `data.orchestrator_info{address,url}`, `data.inference_status{fps,restart_count,last_error,last_error_time,last_params_hash,last_params}` | output health, running-state evidence, output FPS, startup/health coverage | upstream `pipeline` is often model-like, not canonical workflow pipeline |
| `ai_stream_events` | envelope `id`, `timestamp`, `gateway`; payload `data.stream_id`, `data.request_id`, `data.pipeline`, `data.type`, optional `data.message`, optional param payloads, `data.orchestrator_info{address,url}` | errors, parameter updates, generic lifecycle signals | event subtypes have different business meaning; some only affect diagnostics |
| `stream_ingest_metrics` | envelope `id`, `timestamp`, `gateway`; payload `data.stream_id`, `data.request_id`, `data.stats` | ingest quality samples | non-stateful projection only; do not confuse with lifecycle facts |
| `network_capabilities` | envelope `id`, `timestamp`, `gateway`; payload object or array of orchestrators with `address`, `local_address`, `orch_uri`, `capabilities`, `capabilities_prices`, optional `hardware[]` and per-model constraints | canonical orchestrator, pipeline, model, GPU, capability, and price identity | one raw payload can fan out into many logical rows; hardware may be absent even when model identity exists |
| `discovery_results` | envelope `id`, `timestamp`, `gateway`; non-empty array of candidate orchestrators with `address`, `url`, `latency_ms` | discovery visibility and latency evidence | array must be non-empty and normalized safely |
| `create_new_payment` | envelope `id`, `timestamp`, `gateway`; payload `sessionID`, `manifestID`, `requestID`, `sender`, `recipient`, `orchestrator`, `faceValue`, `numTickets`, `price`, `winProb` | payment/fee attribution to session demand | joins must use canonical request/session lineage, not fuzzy time matching |
| `ai_batch_request` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.type` (`ai_batch_request_received`/`ai_batch_request_completed`), `data.request_id`, `data.pipeline`, `data.model_id`, `data.success`, `data.tries`, `data.duration_ms`, `data.orch_url`, `data.latency_score`, `data.price_per_unit`, `data.error_type`, `data.error` | AI batch job lifecycle for fixed pipelines (text-to-image, llm, audio-to-text, etc.) | `data.type` is the full subtype string (e.g. `ai_batch_request_completed`), not a short form; only completed rows carry outcome fields |
| `ai_llm_request` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.type` (`llm_request_completed`/`llm_stream_completed`), `data.request_id`, `data.model`, `data.orch_url`, `data.prompt_tokens`, `data.completion_tokens`, `data.total_tokens`, `data.total_duration_ms`, `data.tokens_per_second`, `data.ttft_ms`, `data.finish_reason`, `data.error` | LLM-specific token/TPS/TTFT metrics paired with `ai_batch_request` via `request_id` | only completed/stream-completed subtypes are accepted; links via `(org, request_id)` |
| `job_gateway` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.type` (`submitted`/`completed`/`discovery_result`/`token_fetch_result`), `data.capability`, `data.request_id`, `data.success`, `data.duration_ms`, `data.orchestrator_info{address,url}` | BYOC job lifecycle as seen by the gateway | `data.type` is the short form; only `completed` rows are used as canonical job records; capabilities are stored verbatim (never hardcoded) |
| `job_orchestrator` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.type` (`completed`), `data.capability`, `data.success`, `data.duration_ms`, `data.http_status`, `data.worker_url`, `data.orchestrator_info{address,url}`, `data.charged_compute`, `data.price_per_unit` | BYOC job outcome as seen by the orchestrator | supplements `job_gateway` completed rows; not joined into canonical_byoc_jobs by default |
| `job_auth` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.capability`, `data.success`, `data.orchestrator_info{address,url}`, `data.error` | BYOC authorization event per capability and orchestrator | used for auth success/failure rate aggregations only |
| `worker_lifecycle` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.capability`, `data.orchestrator_info{address,url}`, `data.worker_url`, `data.price_per_unit`, `data.worker_options[]{model}` | BYOC worker registration snapshot per orchestrator+capability | multiple entries over time for same orch+capability; `argMax` on `event_ts` gives the latest known worker/model/price |
| `job_payment` | envelope `id`, `type`, `timestamp`, `gateway`; payload `data.capability`, `data.request_id`, `data.orchestrator_info{address}`, `data.amount`, `data.currency`; subtype in `data.type` | BYOC payment event — financial record of a completed job charge | stored in `normalized_byoc_payment`; no hardware attribution; kept separate from job lifecycle events |

## Global Normalization Principles

- A compliant system must preserve raw envelope identity, event time, source partition, and payload lineage for every accepted event.
- Address fields must be normalized case-insensitively before identity joins.
- Blank, null, unresolved, ambiguous, and stale are not interchangeable states.
- Derived ratios and scores must be recomputed from additive support fields rather than averaged directly.
- Historical state and latest state are both part of the contract when outputs depend on correction, replay, or late attribution.
- The contract is logical, not physical: a system may use streams, tables, documents, or batch jobs, but it must expose enough evidence to prove each rule.

## Rule Inventory

| Rule ID | Title | Layer | Status |
|---|---|---|---|
| `RULE-INGEST-001` | Supported Envelope And Family Contract | `ingest` | `active_contracted` |
| `RULE-INGEST-002` | Family-Specific Minimum Raw Elements | `ingest` | `active_contracted` |
| `RULE-INGEST-003` | Ignored Raw Families And Subtypes Must Be Explicit | `ingest` | `active_inferred` |
| `RULE-INGEST-004` | Reject, Quarantine, And Drop Accounting Must Explain Loss | `ingest` | `active_contracted` |
| `RULE-LINEAGE-001` | Canonical Accepted-Event Identity Must Be Stable And Replay-Safe | `lineage` | `active_contracted` |
| `RULE-LINEAGE-002` | Derived Outputs Must Preserve Traceable Lineage Coverage | `lineage` | `active_contracted` |
| `RULE-TYPED_RAW-001` | Non-Fanout Families Must Project Losslessly Into Normalized Records | `typed_raw` | `active_contracted` |
| `RULE-TYPED_RAW-002` | Capability Payloads Must Expand Into Canonical Snapshot Rows Without Losing Model Or GPU Identity | `typed_raw` | `active_contracted` |
| `RULE-TYPED_RAW-003` | Capability No-Op And Stale Snapshots Must Not Distort Downstream Coverage | `typed_raw` | `active_inferred` |
| `RULE-FACT-001` | Historical State And Latest State Must Both Be Recoverable | `fact` | `active_contracted` |
| `RULE-FACT-002` | Latest-State Outputs Must Converge Deterministically | `fact` | `active_contracted` |
| `RULE-FACT-003` | Selection Decisions Are Part Of The Canonical Fact Contract | `fact` | `active_contracted` |
| `RULE-LIFECYCLE-001` | Session Identity Must Be Deterministic | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-002` | Trace Subtypes Must Map To Fixed Lifecycle Semantics | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-003` | Startup Outcome Classification Must Use Fixed Precedence | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-004` | Excused Error Taxonomy Must Be Exact And Case-Insensitive | `lifecycle` | `active_inferred` |
| `RULE-LIFECYCLE-005` | Segment Boundaries Must Be Driven By Canonical Orchestrator Change Only | `lifecycle` | `active_inferred` |
| `RULE-LIFECYCLE-006` | Parameter Updates And Other Non-Boundary Changes Must Not Re-Segment Sessions | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-007` | Session Health Signals Must Be Explicit Additive Facts | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-008` | Demand Uses Session-Start-Hour Semantics | `lifecycle` | `active_contracted` |
| `RULE-LIFECYCLE-009` | SLA And GPU Reliability Use Status-Hour Semantics With Tail Filtering | `lifecycle` | `active_contracted` |
| `RULE-ATTRIBUTION-001` | Canonical Orchestrator Identity Must Be Distinguished From Proxy Identity | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-002` | Pipeline And Model Must Be Normalized Across Event Families | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-003` | Capability Selection Must Favor Safe Compatible Evidence | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-004` | Ambiguous, Unresolved, And Stale Attribution Must Fail Safe | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-005` | Lifecycle Facts May Be Enriched From Attribution But Must Not Invent Identity | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-006` | Hardware-Less Attribution Must Preserve Model Identity Without Manufacturing GPU Identity | `attribution` | `active_contracted` |
| `RULE-ATTRIBUTION-007` | Attribution Coverage Must Be Observable And Reviewable | `attribution` | `active_contracted` |
| `RULE-AGGREGATE-001` | Minute Performance Rollups Must Be Latest-Only And Attribution-Aware | `aggregate` | `active_contracted` |
| `RULE-AGGREGATE-002` | Demand Rollups Must Combine Perf Keys And Session Demand Keys Correctly | `aggregate` | `active_contracted` |
| `RULE-AGGREGATE-003` | GPU-Sliced Demand May Use Fallback GPU Attribution But Must Mark Unresolved Hardware Explicitly | `aggregate` | `active_contracted` |
| `RULE-AGGREGATE-004` | GPU Metrics Must Remain Hardware-Backed | `aggregate` | `active_contracted` |
| `RULE-AGGREGATE-005` | SLA Score And Reliability Rates Must Use The Contracted Formula Set | `aggregate` | `active_contracted` |
| `RULE-AGGREGATE-006` | Rollup Algebra, Join Grain, And Null Semantics Must Be Explicit | `aggregate` | `active_contracted` |
| `RULE-SERVING-001` | Public Output Families Must Expose Stable Logical Grains And Required Fields | `serving` | `active_contracted` |
| `RULE-SERVING-002` | Serving Outputs Must Be Derivable From Canonical Facts And Aggregates | `serving` | `active_contracted` |
| `RULE-PARITY-001` | Conformance Must Be Proven From Raw Replay Through Final Outputs | `parity` | `active_contracted` |
| `RULE-PARITY-002` | Parity Must Cover Ingest, Lineage, Attribution, Lifecycle, Aggregate, And Serving Layers | `parity` | `active_contracted` |
| `RULE-QUALITY-001` | Quality Diagnostics Must Expose Coverage, Drift, And Failure Classes Without Redefining Truth | `quality` | `active_contracted` |
| `RULE-QUALITY-002` | Scenario Coverage Must Exercise Core Edge Semantics | `quality` | `active_contracted` |

## Ingest Rules

### RULE-INGEST-001 Supported Envelope And Family Contract

- `layer`: `ingest`
- `domain`: raw inbound envelope
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Freeze the raw event universe that downstream quality and parity logic is allowed to depend on.

**Raw data scope**
- envelope `id`
- envelope `type`
- envelope `timestamp`
- envelope `gateway`
- envelope `data`
- optional envelope version fields

**Requirement**
A compliant system must accept and understand these top-level families:

- `ai_stream_status`
- `stream_ingest_metrics`
- `stream_trace`
- `network_capabilities`
- `ai_stream_events`
- `discovery_results`
- `create_new_payment`

The minimum envelope contract is:

- the root is a JSON object
- `type` is present and textual
- `timestamp` is present and parseable as numeric or numeric string
- `data` is present
- version support is enforced when a supported-version policy is configured

**Output obligations**
- accepted events enter the canonical processing path
- non-supported or malformed events do not silently appear in canonical outputs

**Validator checks**
- accepted-event counts can be explained entirely by the supported-family and envelope policy
- unsupported top-level types do not leak into canonical outputs

### RULE-INGEST-002 Family-Specific Minimum Raw Elements

- `layer`: `ingest`
- `domain`: per-family schema minimums
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Make raw-data conformance review start from the actual fields that drive downstream logic.

**Raw data scope**
- family-specific payload elements listed below

**Requirement**
A compliant system must enforce these minimum logical raw elements:

- `ai_stream_status`
  - `data.stream_id`
  - `data.request_id`
  - a pipeline/model hint field carried today in `data.pipeline`
- `stream_trace`
  - root `id`
  - `data.stream_id`
  - `data.request_id`
  - `data.type`
- `ai_stream_events`
  - `data.stream_id`
  - `data.request_id`
  - a pipeline/model hint field carried today in `data.pipeline`
  - `data.type`
- `stream_ingest_metrics`
  - `data.stream_id`
  - `data.request_id`
- `network_capabilities`
  - `data` must be an object or array of orchestrator capability payloads
- `discovery_results`
  - `data` must be a non-empty array
- `create_new_payment`
  - `data.sessionID`
  - `data.manifestID`
  - `data.sender`
  - `data.recipient`
  - `data.orchestrator`
  - `data.faceValue`
  - `data.numTickets`
  - `data.price`
  - `data.winProb`

**Output obligations**
- the implementation must not invent missing required values
- missing required values must be rejected or explicitly ignored only when covered by a contracted ignore rule

**Validator checks**
- each accepted family has its contracted minimum elements present
- family-specific shape exceptions are documented and testable

### RULE-INGEST-003 Ignored Raw Families And Subtypes Must Be Explicit

- `layer`: `ingest`
- `domain`: accepted vs ignored raw events
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Prevent low-value or known non-contract traces from contaminating correctness and parity metrics.

**Raw data scope**
- `stream_trace`
- envelope `sender.type`
- envelope `sender.id`
- `data.type`
- `data.stream_id`
- `data.request_id`
- `data.client_source`

**Requirement**
A compliant system must enforce an explicit allowlist at the earliest controlled
post-Kafka ingest boundary. Unsupported families or subtypes must bypass the
accepted raw analytics path and land only in ignored-event diagnostics.

A compliant system must explicitly classify these inbound cases as ignored, not canonical:

- unsupported top-level event types
- `stream_trace` with `data.type = app_send_stream_request`
- daydream app traces where all of the following hold:
  - `sender.type = app`
  - `sender.id = daydream`
  - `data.type` starts with `app_`
  - either `data.stream_id` or `data.request_id` is missing
- scope client traces where all of the following hold:
  - `sender.type` is blank
  - `sender.id` is blank
  - `data.client_source = scope`
  - `data.stream_id` is missing
  - `data.type` is one of:
    - `stream_heartbeat`
    - `pipeline_load_start`
    - `pipeline_loaded`
    - `session_created`
    - `stream_started`
    - `playback_ready`
    - `session_closed`
    - `stream_stopped`
    - `pipeline_unloaded`
    - `websocket_connected`
    - `websocket_disconnected`
    - `error`

**Output obligations**
- ignored rows do not enter canonical outputs
- ignored rows do not appear as DLQ failures
- ignored rows are still counted diagnostically

**Validator checks**
- ignored-count metrics exist by ignore family
- no ignored row appears in canonical typed, fact, or serving outputs

### RULE-INGEST-004 Reject, Quarantine, And Drop Accounting Must Explain Loss

- `layer`: `ingest`
- `domain`: failure and drop accounting
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Make every non-canonical path explainable.

**Raw data scope**
- malformed envelopes
- schema-invalid payloads
- duplicates
- oversize mapped rows
- capability-local stale/no-op behavior

**Requirement**
A compliant system must distinguish at least these classes:

- ignored: known non-contract raw traffic
- rejected/DLQ: malformed, schema-invalid, or unsupported-version events
- duplicate or quarantine: rows excluded because they would double-count canonical accepted events
- sink-guard drop: rows too large or otherwise unsafe to materialize
- capability-local no-op or stale suppression: capability rows intentionally not materialized because they add no new valid state

**Output obligations**
- every excluded row belongs to one explainable class
- replay markers are preserved on failure evidence when replayed events fail

**Validator checks**
- unexplained loss between raw input and accepted canon is zero
- duplicate handling emits evidence rather than silently lowering counts
- no-op/stale capability suppression is visible and not mistaken for missing data

## Lineage Rules

### RULE-LINEAGE-001 Canonical Accepted-Event Identity Must Be Stable And Replay-Safe

- `layer`: `lineage`
- `domain`: accepted-event identity
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Give every accepted event one stable identity that survives replay, dedup, and recomputation.

**Raw data scope**
- upstream event identifier
- source partition or tenant namespace
- event type
- canonicalized payload
- replay metadata

**Requirement**
Every accepted event must receive one non-empty canonical lineage key.

To match current behavior:

- when upstream `event_id` is present, the canonical key is namespaced by source and event id
- when upstream `event_id` is absent, the canonical key is a deterministic canonical-payload hash
- replay metadata must be stripped before payload-hash dedup so replayed copies remain idempotent
- source partition or tenant namespace must be part of dedup identity so equivalent payloads from different sources do not collide

**Output obligations**
- canonical lineage survives into all normalized records that descend directly from the accepted event

**Validator checks**
- no accepted normalized row has blank canonical lineage
- replaying the same raw envelope window does not change canonical accepted-event identity

### RULE-LINEAGE-002 Derived Outputs Must Preserve Traceable Lineage Coverage

- `layer`: `lineage`
- `domain`: downstream traceability
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Allow any validator to trace final metrics back to raw evidence and attribution decisions.

**Raw data scope**
- accepted-event lineage
- lifecycle source-event lineage
- capability source-event lineage
- session first/last source lineage

**Requirement**
The system must preserve enough lineage to answer:

- which accepted raw events created a normalized row
- which raw event first opened a session
- which raw event most recently updated a session
- which capability snapshot was selected for attribution
- when that selected capability changed and was last seen

**Output obligations**
- latest session outputs carry first and last source-event lineage
- attribution decision outputs carry selected capability lineage when resolved
- unresolved, ambiguous, and stale cases still carry reviewable decision lineage where possible

**Validator checks**
- a reviewer can trace any sampled serving row back through aggregate keys, latest facts, and accepted-event lineage
- no resolved attribution row is missing its selected capability lineage

## Normalized Raw And Fact Rules

### RULE-TYPED_RAW-001 Non-Fanout Families Must Project Losslessly Into Normalized Records

- `layer`: `typed_raw`
- `domain`: accepted raw to normalized record parity
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Lock the raw-to-normalized parity for families that should not fan out.

**Raw data scope**
- accepted `stream_trace`
- accepted `ai_stream_status`
- accepted `stream_ingest_metrics`
- accepted non-fanout `ai_stream_events`

**Requirement**
For non-fanout families, one accepted raw event must produce exactly one canonical normalized record at the current family grain.

**Output obligations**
- lineage is preserved 1:1
- multiplicity does not change before later lifecycle or aggregate logic

**Validator checks**
- accepted counts equal normalized counts for non-fanout families
- latest views collapse only versioned corrections, not raw multiplicity drift

### RULE-TYPED_RAW-002 Capability Payloads Must Expand Into Canonical Snapshot Rows Without Losing Model Or GPU Identity

- `layer`: `typed_raw`
- `domain`: capability normalization
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Define the raw capability shapes that must be preserved before attribution or serving logic starts.

**Raw data scope**
- `network_capabilities.address`
- `network_capabilities.local_address`
- `network_capabilities.orch_uri`
- `hardware[].pipeline`
- `hardware[].model_id`
- `hardware[].gpu_info`
- model constraints
- capability prices

**Requirement**
One raw capability payload may expand into many canonical normalized rows by:

- orchestrator identity
- pipeline
- model
- GPU
- advertised capability
- model constraint
- price record

If hardware is absent but model identity is still advertised through constraints, the system must emit a hardware-less model-scoped snapshot row so canonical orchestrator and model identity remain attributable, while `gpu_id` stays blank.

**Output obligations**
- GPU-backed capability rows preserve per-GPU fidelity
- hardware-less model rows remain visible for attribution and demand/SLA surfaces
- upstream source-event lineage is preserved across all expanded rows

**Validator checks**
- GPU-row fanout is lossless
- hardware-less model-scoped rows appear when raw capability payloads advertise models without hardware
- source-event id and canonical lineage survive expansion

### RULE-TYPED_RAW-003 Capability No-Op And Stale Snapshots Must Not Distort Downstream Coverage

- `layer`: `typed_raw`
- `domain`: capability suppression semantics
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_inferred`
- `confidence`: `mixed`

**Business purpose**
Prevent validators from interpreting intentional capability suppression as unexplained data loss.

**Raw data scope**
- repeated capability snapshots
- out-of-order stale snapshots
- capability semantic dedup state

**Requirement**
Capability processing may suppress:

- unchanged snapshots that add no new canonical state
- stale snapshots outside the accepted state progression window

This suppression is allowed only when it is explicit, deterministic, and observable.

**Output obligations**
- immutable raw archive still contains the original capability input
- canonical changelog and current-state outputs reflect only accepted meaningful state changes

**Validator checks**
- reduced normalized capability volume is explainable by unchanged or stale suppression counters
- stale/no-op suppression is not mixed into generic reject or duplicate counts

### RULE-FACT-001 Historical State And Latest State Must Both Be Recoverable

- `layer`: `fact`
- `domain`: historical vs latest canonical state
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Keep correction, replay, attribution, and serving parity possible.

**Requirement**
For stateful entities, the contract requires both:

- historical state change visibility
- latest canonical state visibility

This applies at minimum to:

- capability state
- lifecycle sessions
- lifecycle segments
- selection decisions

**Output obligations**
- a reviewer can reconstruct how current state was reached
- serving logic can consume latest state without losing auditability

**Validator checks**
- latest-state rows can be tied back to historical versions or changes
- historical lineage can explain current canonical values

### RULE-FACT-002 Latest-State Outputs Must Converge Deterministically

- `layer`: `fact`
- `domain`: versioned convergence
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Ensure late enrichment and correction produce one best-known answer instead of metric inflation.

**Requirement**
When facts are versioned or corrected over time:

- there must be one deterministic latest row per logical entity key
- later rows may correct attribution or lifecycle state
- serving and aggregate layers must consume latest semantics, not all raw versions additively

**Output obligations**
- no logical entity has multiple conflicting latest rows
- additive rollups are not inflated by superseded versions

**Validator checks**
- latest views have exactly one row per logical entity key
- aggregate parity is computed from latest rows, not raw duplicate versions

### RULE-FACT-003 Selection Decisions Are Part Of The Canonical Fact Contract

- `layer`: `fact`
- `domain`: attribution decision spine
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Make attribution explainable and portable across implementations.

**Requirement**
There must be a canonical decision surface that records, at minimum:

- the segment or logical attribution window being decided
- decision status
- decision reason
- selected capability lineage when resolved
- selection confidence and attribution method

**Output obligations**
- downstream lifecycle, status, trace, latency, and serving outputs can be traced back to explicit attribution decisions

**Validator checks**
- no implementation relies on hidden query-time heuristics without a corresponding explicit decision record
- resolved decisions carry selected capability lineage

## Lifecycle Rules

### RULE-LIFECYCLE-001 Session Identity Must Be Deterministic

- `layer`: `lifecycle`
- `domain`: session identity
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Provide one stable workflow-attempt identity for demand, reliability, and attribution.

**Raw data scope**
- `stream_id`
- `request_id`
- fallback event identity

**Requirement**
The canonical session identifier is derived deterministically:

- if both `stream_id` and `request_id` exist: `stream_id|request_id`
- if only `stream_id` exists: `stream_id|_missing_request`
- if only `request_id` exists: `_missing_stream|request_id`
- if both are missing but fallback event identity exists: `_missing_stream|_missing_request|fallback_event_id`
- otherwise: `_missing_stream|_missing_request|_missing_event`

**Output obligations**
- every lifecycle-derived row carries a non-empty session identity

**Validator checks**
- no session-derived row has blank session identity
- repeated raw replay yields the same session identity assignment

### RULE-LIFECYCLE-002 Trace Subtypes Must Map To Fixed Lifecycle Semantics

- `layer`: `lifecycle`
- `domain`: trace normalization
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Prevent trace-type drift from changing denominator membership or startup timing semantics.

**Raw data scope**
- `stream_trace.data.type`
- `stream_trace.data.message`

**Requirement**
At minimum, these trace subtypes have fixed meaning:

- `gateway_receive_stream_request`
  - adds the session to the known-stream denominator
- `gateway_ingest_stream_closed`
  - explicit close edge for the active session or segment
- `gateway_send_first_ingest_segment`
  - ingest-to-processing latency anchor
- `gateway_receive_first_processed_segment`
  - first processed output signal
- `gateway_receive_few_processed_segments`
  - startup success and first playable signal
- `gateway_receive_first_data_segment`
  - valid trace edge, but not the startup-success signal
- `gateway_no_orchestrators_available`
  - explicit excused startup-failure signal
- `orchestrator_swap`
  - explicit swap evidence

**Output obligations**
- startup, latency, and swap metrics use these canonical meanings only
- latency proxies are fixed to these edge pairs:
  - startup proxy: `gateway_receive_stream_request` -> `gateway_receive_first_processed_segment`
  - E2E proxy: `gateway_send_first_ingest_segment` -> `runner_send_first_processed_segment`
  - prompt-to-playable proxy: `ai_stream_status.start_time` -> `gateway_receive_few_processed_segments`
- latency is nullable when either edge is missing or the ordering is inverted; it is never fabricated from unrelated status timestamps

**Validator checks**
- trace normalization never repurposes these subtypes silently
- denominator membership and startup-success counts match these subtype rules

### RULE-LIFECYCLE-003 Startup Outcome Classification Must Use Fixed Precedence

- `layer`: `lifecycle`
- `domain`: startup outcome
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Make request denominator, selection outcome, startup outcome, and excusal treatment portable and deterministic.

**Requirement**
The lifecycle model is split into explicit steady-state fields:

- `requested_seen`
- `playable_seen`
- `selection_outcome = selected | no_orch | unknown`
- `startup_outcome = success | failed | unknown`
- `excusal_reason = none | no_orch | excusable_error`

Classification precedence is fixed:

1. if the session is not a known stream, `requested_seen = 0`, `startup_outcome = unknown`, and `selection_outcome = unknown`
2. if playable evidence exists, `startup_outcome = success`
3. else if explicit no-orchestrator evidence exists, `selection_outcome = no_orch`, `startup_outcome = failed`, and `excusal_reason = no_orch`
4. else if the session has errors and all counted errors are excusable, `startup_outcome = failed` and `excusal_reason = excusable_error`
5. else if it is a known stream, `startup_outcome = failed` and `excusal_reason = none`

Known stream means a `gateway_receive_stream_request` was observed.
Playable evidence means `gateway_receive_few_processed_segments` was observed.

**Output obligations**
- `requested_seen` does not imply success or playability
- startup success, excusal treatment, and failure treatment remain mutually exclusive for known streams

**Validator checks**
- success always overrides excusal treatment
- unexcused failure treatment is only possible for known streams

### RULE-LIFECYCLE-004 Excused Error Taxonomy Must Be Exact And Case-Insensitive

- `layer`: `lifecycle`
- `domain`: excused error strings
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_inferred`
- `confidence`: `tested`

**Business purpose**
Stop semantically excused startup failures from being treated as product defects.

**Raw data scope**
- `ai_stream_events` where subtype or event type represents error
- error message text

**Requirement**
Error excusability is substring-based, case-insensitive, and currently defined by this exact set:

- `no orchestrators available`
- `mediamtx ingest disconnected`
- `whip disconnected`
- `missing video`
- `ice connection state failed`
- `user disconnected`

**Output obligations**
- excusable error count is explicit
- `excusal_reason = excusable_error` may use these strings

**Validator checks**
- case differences do not change classification
- messages outside this set are not treated as excused by default

### RULE-LIFECYCLE-005 Segment Boundaries Must Be Driven By Canonical Orchestrator Change Only

- `layer`: `lifecycle`
- `domain`: segment boundary semantics
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_inferred`
- `confidence`: `mixed`

**Business purpose**
Keep segments semantically stable and portable.

**Raw data scope**
- attributed canonical orchestrator identity over time
- close edges

**Requirement**
Under the active contract, a new segment opens only when canonical orchestrator identity changes from one non-empty value to another.

Explicit close edges close the current segment but do not by themselves force a new segment until a new active segment begins.

**Output obligations**
- segment boundaries correspond to orchestrator changes, not to every parameter or GPU update

**Validator checks**
- segment count changes only when canonical orchestrator identity changes or the session opens initially

### RULE-LIFECYCLE-006 Parameter Updates And Other Non-Boundary Changes Must Not Re-Segment Sessions

- `layer`: `lifecycle`
- `domain`: non-boundary lifecycle updates
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Avoid fragmenting sessions and segments when only attributes changed.

**Raw data scope**
- `ai_stream_events` with parameter update semantics
- model changes
- GPU changes
- worker changes

**Requirement**
The following may update attributes but must not open a new segment by themselves:

- parameter updates
- GPU identity changes
- worker identity changes
- model changes when canonical orchestrator identity is unchanged

**Output obligations**
- parameter updates are attached to an existing session
- change flags may be set without re-segmenting

**Validator checks**
- parameter updates never produce orphan session references
- pipeline/model change flags match distinct segment values rather than segment-count inflation

### RULE-LIFECYCLE-007 Session Health Signals Must Be Explicit Additive Facts

- `layer`: `lifecycle`
- `domain`: health and reliability evidence
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Define the raw session health evidence used by reliability, demand, GPU, and SLA surfaces.

**Raw data scope**
- status samples
- first processed signal
- first playable signal
- status last-error values

**Requirement**
The session health contract includes these additive signals:

- `status_sample_count`
- `status_error_sample_count`
- `last_error_occurred`
- `loading_only_session`
  - true when status samples exist and every observed state is `LOADING`
- `zero_output_fps_session`
  - true when status samples exist and no status sample has `output_fps > 0`
- `health_signal_count`
- `health_expected_signal_count`
- `health_signal_coverage_ratio`

Current expected-signal semantics are:

- if the session is a known stream, expected signals = 3
- the three expected signals are:
  - at least one status sample
  - first processed signal
  - first playable signal
- if the session is not a known stream, expected signals = 0
- when expected signals = 0, coverage ratio defaults to `1.0`

**Output obligations**
- downstream outputs use these additive fields rather than infer health solely from startup class

**Validator checks**
- `loading_only_session` and `zero_output_fps_session` are computed from raw status evidence, not from serving-time heuristics
- coverage ratios match additive numerators and denominators exactly

### RULE-LIFECYCLE-008 Demand Uses Session-Start-Hour Semantics

- `layer`: `lifecycle`
- `domain`: demand temporal boundaries
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Define where demand belongs when sessions are short, long, sparse, or only partially observed.

**Raw data scope**
- session start timestamp
- session-level reliability and attribution fields

**Requirement**
Demand is anchored to the hour containing the canonical session start timestamp.

Implications:

- a session contributes demand to its start hour even if status/perf evidence is sparse there
- a session is not smeared across every hour it spans just because it remained open
- demand-only rows are valid when lifecycle demand exists without status/perf rollup coverage

**Output obligations**
- demand outputs can contain rows with demand counters but zero perf counters

**Validator checks**
- every canonical session start hour is represented in demand outputs
- later hours without a demand contract do not gain duplicate demand just because the session stayed open

### RULE-LIFECYCLE-009 SLA And GPU Reliability Use Status-Hour Semantics With Tail Filtering

- `layer`: `lifecycle`
- `domain`: status-hour temporal boundaries
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Separate active-hour reliability from session-start demand semantics and prevent false tail noise.

**Raw data scope**
- attributed status sample timestamps
- session start and end timestamps
- per-hour counts of status samples, positive FPS samples, and running-state samples

**Requirement**
SLA and GPU reliability surfaces are anchored to hours with attributed status evidence, not session-start hours.

Only true rollover terminal tail artifacts may be filtered. A terminal tail artifact requires all of the following:

- the session started before the hour
- the session ended during the hour
- the current hour has:
  - `fps_positive_samples = 0`
  - `running_state_samples = 0`
  - `status_samples < 3`
- the previous contiguous hour also has:
  - `fps_positive_samples = 0`
  - `running_state_samples = 0`
  - `status_samples < 3`

The following must remain visible:

- same-hour failed no-output attempts
- active zero-output hours with material status evidence

**Output obligations**
- reliability/SLA rows describe active status-hour evidence
- tail filtering removes only true rollover artifacts

**Validator checks**
- tail candidates never appear in GPU metrics or SLA surfaces
- same-hour failed no-output cases remain visible
- active no-output cases remain visible

## Attribution Rules

### RULE-ATTRIBUTION-001 Canonical Orchestrator Identity Must Be Distinguished From Proxy Identity

- `layer`: `attribution`
- `domain`: orchestrator identity
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Prevent proxy or hot-wallet drift from corrupting model, GPU, and serving attribution.

**Raw data scope**
- raw signal orchestrator address
- canonical capability orchestrator address
- local/hot-wallet address
- orchestrator URL

**Requirement**
The contract distinguishes:

- canonical orchestrator identity
- proxy or local/hot-wallet identity
- orchestrator URL identity

When canonical capability evidence exists, gold and serving outputs must use canonical orchestrator identity, not raw proxy identity.

**Implementation note (current simplified schema)**
`ai_stream_status` carries two orchestrator fields:

- `orchestrator_info.address` — the local/hot-wallet keypair address (not the on-chain ETH address)
- `orchestrator_info.url` — the orchestrator's public service URL

`network_capabilities` carries the canonical on-chain ETH address keyed by `orch_uri`.

The current implementation resolves canonical identity via URI join:

```
ai_stream_status.orchestrator_info.url
    → agg_orch_state.uri  (populated from network_capabilities.orch_uri)
    → agg_orch_state.orch_address  (canonical lowercase ETH address)
```

Aggregate tables (`agg_orch_reliability_hourly`, `agg_stream_state`) store the resolved canonical address.
Events where `orchestrator_info.url` is absent or has no matching `agg_orch_state` entry resolve to `orch_address = ''`.
All stored addresses are lowercased at write time.

**Output obligations**
- canonicalized lifecycle outputs do not persist hot-wallet identity as canonical orchestrator identity
- aggregate tables use the canonical ETH address resolved via URI, not the raw proxy address from status events
- addresses are stored in lowercase throughout all aggregate tables

**Validator checks**
- canonical rows are keyed by canonical orchestrator identity
- proxy-to-canonical multiplicity is surfaced and unsafe fanout is rejected
- mixed-case raw addresses must not appear in any aggregate table
- the URI-based join correctly maps status event URLs to canonical ETH addresses from capability snapshots

**Resolver outcome notes**
- `missing_uri_snapshot_local_alias_present` means exact URI-based canonicalization
  failed inside the attribution window even though a local/proxy alias match
  existed
- `missing_uri_snapshot_address_match_present` means exact URI-based
  canonicalization failed inside the attribution window even though a canonical
  address-compatible match existed
- both of the above remain `attribution_status = unresolved`
- `matched_without_hardware` is the distinct `hardware_less` case and is only
  valid when canonical attribution succeeded but hardware metadata did not
  resolve
- unresolved URI/address-gap sessions remain visible in session and
  status-hour facts, but they are not equivalent to hardware-less GPU matches

### RULE-ATTRIBUTION-002 Pipeline And Model Must Be Normalized Across Event Families

- `layer`: `attribution`
- `domain`: pipeline/model normalization
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Stop raw field-name inconsistency from splitting or merging the wrong aggregates.

**Raw data scope**
- status and event fields that carry model-like text in a field named `pipeline`
- trace `pipeline_id`
- capability `pipeline`
- capability `model_id`

**Requirement**
The system must explicitly normalize raw pipeline-like and model-like fields into:

- canonical workflow pipeline
- canonical model id
- compatibility model hint

To match current behavior:

- raw status/event `pipeline` is often model-like and must be treated as a model hint
- canonical pipeline and canonical model are semantically distinct
- if pipeline text equals model id case-insensitively, canonical pipeline becomes blank rather than duplicating model identity
- capability/state semantics are authoritative for canonical pipeline in the current legacy behavioral contract

**Output obligations**
- attributed outputs do not carry model labels in canonical pipeline
- serving keys do not split or merge due to model-as-pipeline leakage

**Validator checks**
- rows with non-empty model id are not incorrectly using identical pipeline text as a second copy of the model
- cross-family pipeline/model parity holds for comparable attributed rows

### RULE-ATTRIBUTION-003 Capability Selection Must Favor Safe Compatible Evidence

- `layer`: `attribution`
- `domain`: candidate selection
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Prevent the wrong capability snapshot from being selected just because it is nearby in time.

**Raw data scope**
- capability candidate bucket
- signal timestamp
- model hint
- orchestrator URL hint
- GPU hint

**Requirement**
Selection precedence is:

1. compatibility first
2. freshness second
3. deterministic tie-breaking

Compatibility means an incompatible model candidate must be ignored even if it is temporally closer.

Freshness semantics are:

- prefer exact timestamp match
- then nearest prior match within a bounded TTL
- then a small bounded future-skew window

The active behavioral contract uses a bounded prior TTL and a smaller future-skew window.

**Output obligations**
- no compatible candidate is better than a fresh but incompatible candidate

**Validator checks**
- mixed-model hot-wallet scenarios do not resolve to incompatible snapshots
- exact and prior matches outrank future-skew matches when otherwise compatible

### RULE-ATTRIBUTION-004 Ambiguous, Unresolved, And Stale Attribution Must Fail Safe

- `layer`: `attribution`
- `domain`: unsafe attribution handling
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Make “not enough evidence” explicit rather than silently wrong.

**Raw data scope**
- candidate set after compatibility filtering
- candidate timing window

**Requirement**
When attribution is unsafe, the system must not invent a canonical answer.

Canonical statuses are:

- `resolved`
- `unresolved`
- `ambiguous`
- `stale`

Canonical reasons are:

- `matched`
- `missing_candidate`
- `ambiguous_candidates`
- `stale_candidate`

Ambiguous means compatible in-window candidates map to multiple canonicals.
Unresolved means no compatible candidate exists.
Stale means the only available candidate is outside the accepted time window.

**Output obligations**
- unsafe attribution states are explicit
- downstream canonical outputs that require safe attribution do not coerce these states into resolved identity

**Validator checks**
- ambiguous compatible candidates never yield a resolved canonical row
- unresolved and stale cases are visible on the decision spine

### RULE-ATTRIBUTION-005 Lifecycle Facts May Be Enriched From Attribution But Must Not Invent Identity

- `layer`: `attribution`
- `domain`: lifecycle enrichment
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Allow lifecycle facts to become more correct over time without permitting false identity fabrication.

**Raw data scope**
- signal pipeline/model hints
- selected capability snapshot

**Requirement**
Lifecycle records may start from raw signal hints and later be enriched from capability attribution.

When capability evidence exists:

- canonical orchestrator identity may be replaced by the selected canonical identity
- canonical model id may be replaced by the selected model identity
- canonical pipeline may be replaced by the selected pipeline identity

When capability evidence does not exist:

- the system may keep prior safe values
- it must not fabricate canonical orchestrator, model, pipeline, or GPU identity

**Output obligations**
- later corrected lifecycle rows converge toward the best-known canonical identity

**Validator checks**
- enriched lifecycle rows match selected capability decisions
- unresolved rows do not backfill fake canonical fields

### RULE-ATTRIBUTION-006 Hardware-Less Attribution Must Preserve Model Identity Without Manufacturing GPU Identity

- `layer`: `attribution`
- `domain`: hardware-less attribution
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Preserve canonical orchestrator/model identity even when hardware metadata is absent.

**Raw data scope**
- capability model constraints
- capability hardware arrays
- selected capability snapshot

**Requirement**
When a capability snapshot resolves canonical orchestrator and model identity but does not contain hardware-backed GPU identity:

- canonical model identity remains valid
- canonical GPU identity remains blank
- a later hardware-less snapshot may clear previously unresolved GPU identity if the snapshot is authoritative for model identity

**Output obligations**
- hardware-less attributed rows are valid for SLA and GPU-sliced demand
- hardware-less attributed rows are not exposed as resolved GPU metric rows

**Validator checks**
- GPU-unresolved but model-resolved rows appear explicitly where allowed
- GPU metrics excludes unresolved GPU rows

### RULE-ATTRIBUTION-007 Attribution Coverage Must Be Observable And Reviewable

- `layer`: `attribution`
- `domain`: attribution coverage
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Turn attribution quality into something a reviewer can measure instead of assume.

**Requirement**
The implementation must expose enough information to review:

- percent of rows resolved
- percent unresolved
- percent ambiguous
- percent stale
- canonical pipeline coverage when model identity is present
- explicit hardware-less coverage
- explicit job/session denominator state before attribution quality is computed

For request/response jobs specifically:

- `canonical_ai_batch_jobs.selection_outcome` and
  `canonical_byoc_jobs.selection_outcome` must be
  `selected | no_orch | unknown`
- job attribution quality metrics use `selection_outcome = selected` as the
  denominator
- `no_orch` and `unknown` remain visible as lifecycle outcomes and must not be
  silently folded into attribution failure percentages

Current readiness expectation: for attributable rows where model identity is known, canonical pipeline should be non-empty for at least 99% of rows over a rolling 24-hour window, unless the rows are intentionally pipeline-empty because pipeline text duplicated the model id.

**Output obligations**
- attribution coverage is visible on diagnostic or monitoring surfaces

**Validator checks**
- coverage metrics and unresolved decision surfaces exist
- reviewers can sample failures, not only summary percentages

## Aggregate Rules

### RULE-AGGREGATE-001 Minute Performance Rollups Must Be Latest-Only And Attribution-Aware

- `layer`: `aggregate`
- `domain`: performance rollups
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `tested`

**Business purpose**
Ensure performance rollups count canonical status evidence once and only once.

**Raw data scope**
- latest attributed status samples
- canonical orchestrator/pipeline/model/GPU identity

**Requirement**
Minute performance rollups are derived from latest attributed status samples only.

At minimum:

- duplicate logical status versions must be collapsed before aggregation
- rollups requiring canonical orchestrator identity exclude unattributed rows

**Output obligations**
- session, stream, sample, and FPS aggregates match independent recomputation from latest status facts

**Validator checks**
- minute rollups match latest-status recomputation exactly
- raw duplicate versions do not inflate aggregates

### RULE-AGGREGATE-002 Demand Rollups Must Combine Perf Keys And Session Demand Keys Correctly

- `layer`: `aggregate`
- `domain`: demand rollups
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Represent both observed activity and latent demand, even when only one side exists.

**Raw data scope**
- hourly performance aggregates
- latest session demand and reliability fields
- payment events joined through request/session lineage

**Requirement**
Demand outputs must be built from the union of:

- perf-backed hourly keys
- session-demand hourly keys

Demand rows are valid even when one side is absent.

Current active demand formulas:

- `requested_sessions = sessions with requested_seen = 1`
- `startup_success_sessions = requested sessions with startup_outcome = success`
- `no_orch_sessions = requested sessions with selection_outcome = no_orch`
- `startup_failed_sessions = requested sessions with startup_outcome = failed AND excusal_reason = none`
- `startup_success_rate = startup_success_sessions / requested_sessions`
- `effective_failed_sessions = startup_failed_sessions OR zero_output_fps_session OR loading_only_session`
- `effective_success_rate = 1 - effective_failed_sessions / requested_sessions`

`last_error_occurred` alone does not reduce effective success under the current active contract.

Pipeline fallback is allowed at the public demand grain only when exactly one non-empty pipeline exists for the shared `(hour, gateway, region, model)` cohort.
If the fallback pipeline equals model id case-insensitively, canonical public pipeline stays blank.

**Output obligations**
- demand-only rows remain visible
- perf-only rows remain visible

**Validator checks**
- demand rollups match independent recomputation at model-aware grain
- effective success never exceeds startup success
- rows with effective-failure indicators do not report perfect effective success

### RULE-AGGREGATE-003 GPU-Sliced Demand May Use Fallback GPU Attribution But Must Mark Unresolved Hardware Explicitly

- `layer`: `aggregate`
- `domain`: GPU-sliced demand and capacity
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Expose GPU-sliced demand and reliability even when GPU identity is resolved late or remains hardware-less.

**Raw data scope**
- hourly perf aggregates by GPU key
- latest session GPU identity by `(hour, org, gateway, orchestrator, region, pipeline, model)`
- capability current model-capacity state

**Requirement**
GPU-sliced demand may use hourly fallback GPU attribution from latest session snapshots when perf rows lack a GPU id but session-level canonical GPU identity is available for the same hourly key.

When canonical model/orchestrator attribution exists but GPU identity is still unresolved:

- the row is valid on GPU-sliced demand surfaces
- the row must be explicitly marked as hardware-unresolved

Capacity fields on these surfaces are capability-based advertised capacity proxies, not observed saturation metrics.

**Output obligations**
- GPU-sliced demand carries explicit GPU identity status
- unresolved GPU rows remain distinguishable from malformed rows

**Validator checks**
- reliability counters align with latest-session recomputation at GPU-aware grain
- unresolved GPU rows use explicit status rather than being dropped silently

### RULE-AGGREGATE-004 GPU Metrics Must Remain Hardware-Backed

- `layer`: `aggregate`
- `domain`: GPU metrics surface
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Prevent hardware-unresolved rows from masquerading as resolved GPU quality measurements.

**Requirement**
GPU metrics rows require hardware-backed GPU identity.

Null KPI behavior is also part of the contract:

- if there are no valid supporting samples for a KPI in the hour, the KPI is null
- null must not be silently converted to zero

Current derived formulas include:

- `avg_output_fps = output_fps_sum / status_samples` when `status_samples > 0`
- `fps_jitter_coefficient = stddev(output_fps) / mean_output_fps` when mean FPS is positive
- latency averages use `sum / sample_count` only when sample count is positive
- `startup_failed_rate = startup_failed_sessions / requested_sessions`
- `swap_rate = total_swapped / requested_sessions`

**Output obligations**
- GPU metrics excludes unresolved GPU identity rows
- null KPIs remain null when evidence is absent

**Validator checks**
- hardware-less attributed rows are absent from GPU metrics but present where allowed elsewhere
- averages and rates equal independent recomputation from additive support fields

### RULE-AGGREGATE-005 SLA Score And Reliability Rates Must Use The Contracted Formula Set

- `layer`: `aggregate`
- `domain`: SLA rollup calculations
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Freeze the exact meaning of SLA and related reliability ratios.

**Raw data scope**
- known session counts
- startup outcome counts
- swap counts
- loading-only counts
- zero-output counts
- health-signal counts

**Requirement**
The current active formulas are:

- `startup_success_rate = startup_success_sessions / requested_sessions`
- `effective_failed_sessions = startup_failed_sessions OR zero_output_fps_session OR loading_only_session`
- `effective_success_rate = 1 - effective_failed_sessions / requested_sessions`
- `no_swap_rate = 1 - total_swapped / requested_sessions`
- `health_signal_coverage_ratio = min(health_signal_count / health_expected_signal_count, 1.0)`, defaulting to `1.0` when the denominator is `0`
- `output_failed_sessions = count(requested session where loading_only_session OR zero_output_fps_session)`
- `output_viability_rate = 1 - output_failed_sessions / requested_sessions`
- `avg_output_fps = output_fps_sum / status_samples` when `status_samples > 0`
- `avg_prompt_to_first_frame_ms = prompt_to_first_frame_sum_ms / prompt_to_first_frame_sample_count` when `prompt_to_first_frame_sample_count > 0`
- `avg_e2e_latency_ms = e2e_latency_sum_ms / e2e_latency_sample_count` when `e2e_latency_sample_count > 0`
- `reliability_score = (0.4 * startup_success_rate) + (0.2 * no_swap_rate) + (0.4 * output_viability_rate)`
- `ptff_score_raw` compares `avg_prompt_to_first_frame_ms` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; rows at or below cohort `p50` score `1.0`, rows at or above `p90` score `0.0`, and values in between interpolate linearly
- `e2e_score_raw` compares `avg_e2e_latency_ms` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; rows at or below cohort `p50` score `1.0`, rows at or above `p90` score `0.0`, and values in between interpolate linearly
- `fps_score_raw` compares `avg_output_fps` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; rows at or above cohort `p50` score `1.0`, rows at or below `p10` score `0.0`, and values in between interpolate linearly
- `ptff_score = clamp(0.5 + min(prompt_to_first_frame_sample_count / 10, 1.0) * (ptff_score_raw - 0.5), 0, 1)`
- `e2e_score = clamp(0.5 + min(e2e_latency_sample_count / 10, 1.0) * (e2e_score_raw - 0.5), 0, 1)`
- `fps_score = clamp(0.5 + min(status_samples / 30, 1.0) * (fps_score_raw - 0.5), 0, 1)`
- `latency_score = (0.6 * ptff_score) + (0.4 * e2e_score)`
- `quality_score = (0.6 * latency_score) + (0.4 * fps_score)`
- `sla_score = clamp(100 * health_signal_coverage_ratio * ((0.7 * reliability_score) + (0.3 * quality_score)), 0, 100)`

Bounds are part of the contract:

- startup, effective, no-swap, output-viability, health-coverage, reliability, PTFF, E2E, latency, FPS, and quality scores are in `[0,1]`
- `sla_score` is in `[0,100]`

**Output obligations**
- SLA rows carry additive support fields for FPS and latency, derived averages, `gpu_model_name`, and the derived component scores
- additive org-hour SLA facts remain the freshness source of truth
- final public/org SLA serving rows are published from those additive inputs so
  serving reads do not recompute the full benchmark-scoring path on every
  request

**Validator checks**
- derived ratios and score match recomputation exactly
- ratios stay within bounds

### RULE-AGGREGATE-006 Rollup Algebra, Join Grain, And Null Semantics Must Be Explicit

- `layer`: `aggregate`
- `domain`: rollup safety
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Prevent mathematically invalid re-rollups and accidental join inflation.

**Requirement**
The implementation must explicitly distinguish:

- additive counters
- derived ratios
- weighted averages
- distinct counts
- percentiles or distribution states

Rules:

- sum additive counters directly
- recompute ratios from summed numerators and denominators
- recompute means from additive sums and counts
- weighted averages require explicit additive support counts and sums
- do not average precomputed ratios or percentiles directly
- percentiles require merge-safe aggregate state; scalar percentile values are not re-rollup inputs
- overlapping classifications must expose an explicit additive union counter when higher-grain recomputation needs the union count
- model-aware joins must include model identity or pre-aggregate away model grain first
- null means “no valid evidence,” not zero

**Output obligations**
- consumers can safely re-roll windows using additive support fields

**Validator checks**
- public rollups equal explicit recomputation from org-aware or lower-grain additive fields
- naive pipeline-only join inflation is detectable and documented as unsafe

## Serving Rules

### RULE-SERVING-001 Public Output Families Must Expose Stable Logical Grains And Required Fields

- `layer`: `serving`
- `domain`: public and org-aware outputs
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Define what a compliant system must ultimately expose, regardless of storage model.

**Requirement**
The minimum logical output families are:

- GPU metrics
  - grain: hour x orchestrator x pipeline x model x GPU x region
- network demand
  - grain: hour x gateway x region x pipeline x model
- GPU-sliced network demand
  - grain: hour x gateway x orchestrator x region x pipeline x model x GPU
- SLA compliance
  - grain: hour x orchestrator x pipeline x model x GPU x region

Org-aware variants, when present, must preserve organization or tenant identity at the same logical grain.

Required serving fields include:

- canonical identity keys for the surface grain
- additive support fields
- derived rates or scores defined by this contract
- explicit identity-resolution flags where hardware-less or unresolved states are allowed

**Output obligations**
- public outputs are stable enough for validators to compare implementations directly

**Validator checks**
- output grain and required fields are stable
- org-aware variants expose tenant identity

### RULE-SERVING-002 Serving Outputs Must Be Derivable From Canonical Facts And Aggregates

- `layer`: `serving`
- `domain`: serving boundary
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Keep public outputs aligned with canonical lifecycle and attribution semantics.

**Requirement**
Serving outputs must be derived from canonical normalized, fact, dimension, and aggregate semantics.
They must not bypass canonical state by reinterpreting raw ingest payloads directly.

Where org-aware and public outputs both exist:

- public outputs must equal explicit re-aggregation of the org-aware contract where applicable

**Output obligations**
- one implementation can swap storage engines or pipeline topology without changing output semantics

**Validator checks**
- public outputs equal explicit recomputation from lower canonical layers
- serving logic does not redefine lifecycle or attribution semantics
- `/v1/perf/e2e-latency`, status samples, and GPU metrics reuse the canonical session-edge latency facts rather than raw producer-specific latency payloads

## Parity And Quality Rules

### RULE-PARITY-001 Conformance Must Be Proven From Raw Replay Through Final Outputs

- `layer`: `parity`
- `domain`: end-to-end validation
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Require proof that the whole pipeline is correct, not merely internally consistent.

**Requirement**
Canonical validation must include:

- replay of raw source envelopes
- accepted/ignored/rejected accounting
- independent recomputation from canonical latest facts
- aggregate-to-serving parity

Direct insertion into downstream facts is not sufficient to prove conformance.

**Output obligations**
- a reviewer can validate an implementation starting from raw envelopes

**Validator checks**
- replay windows can be rerun deterministically
- replay plus recomputation reproduces the same final outputs

### RULE-PARITY-002 Parity Must Cover Ingest, Lineage, Attribution, Lifecycle, Aggregate, And Serving Layers

- `layer`: `parity`
- `domain`: multi-layer parity coverage
- `priority`: `P0`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Ensure parity checks are not limited to one attractive but incomplete layer.

**Requirement**
A compliant parity suite must cover:

- raw input presence
- accepted vs ignored vs rejected counts
- lineage-key coverage
- raw-to-normalized parity
- historical/latest-state convergence
- attribution selection and unresolved coverage
- session and segment lifecycle facts
- demand, GPU, and SLA aggregates
- public serving outputs

**Output obligations**
- parity failures identify the broken layer instead of surfacing only as final-metric drift

**Validator checks**
- each layer has at least one independent parity check
- lineage and coverage diagnostics exist alongside pass/fail parity

### RULE-QUALITY-001 Quality Diagnostics Must Expose Coverage, Drift, And Failure Classes Without Redefining Truth

- `layer`: `quality`
- `domain`: diagnostics and governance
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `mixed`

**Business purpose**
Keep diagnostics actionable while preserving one canonical contract.

**Requirement**
The implementation must expose diagnostics for:

- ignored raw-event counts by ignore family
- reject and DLQ counts by failure class
- duplicate or quarantine counts
- capability stale/no-op suppression counts
- unresolved, ambiguous, and stale attribution decisions
- lifecycle edge coverage
- canonical pipeline coverage on attributable rows
- hardware-less GPU coverage
- health-signal coverage

Diagnostics may explain the contract, but they must not replace or contradict the canonical rules.

**Output obligations**
- validators can investigate why a rule failed, not just that it failed

**Validator checks**
- warning-tier drift signals remain distinguishable from blocking contract failures
- diagnostic surfaces point back to these canonical rule meanings

### RULE-QUALITY-002 Scenario Coverage Must Exercise Core Edge Semantics

- `layer`: `quality`
- `domain`: scenario validation
- `priority`: `P1`
- `severity`: blocking
- `status`: `active_contracted`
- `confidence`: `documented`

**Business purpose**
Catch behavioral regressions that aggregate parity alone can miss.

**Requirement**
Scenario coverage must include, at minimum:

- clean success with no swap
- no-orchestrator startup failure
- successful session with swap
- successful session with parameter updates
- same-hour failed no-output retention
- active no-output retention
- rollover terminal tail filtering
- hardware-less attribution visibility
- mixed-model or ambiguous capability attribution
- ignored daydream and scope trace handling

**Output obligations**
- the scenario suite demonstrates both happy paths and contract-critical edge paths

**Validator checks**
- each core scenario can be replayed from raw envelopes
- scenario expectations cover both lifecycle facts and final serving outputs

## Non-Active And Excluded Items

These items are intentionally not part of the active standalone contract:

- transport-bandwidth metrics that require a new telemetry family
- future SLA score versions that add latency or FPS baseline scoring
- future policy changes for terminal disconnect treatment beyond the current active behavior
- future policy changes that redefine effective success to penalize unserved demand differently
- future source-aware fallback session identity semantics not yet promoted into the active contract

## What A Compliant Validator Must Be Able To Do

Without depending on the current repository layout, a compliant validator must be able to:

1. look at raw envelopes and classify each as accepted, ignored, rejected, duplicate, or suppressed
2. reproduce canonical lineage and replay-safe dedup behavior
3. normalize pipeline, model, orchestrator, and GPU identity from inconsistent raw shapes
4. reconstruct canonical sessions, segments, startup outcomes, and health signals
5. reproduce safe attribution decisions, including unresolved, ambiguous, stale, and hardware-less cases
6. recompute demand, GPU, and SLA outputs from canonical latest facts
7. prove that final outputs preserve the same business semantics and data-quality guarantees as this contract

## Validation Test Harness

The repository includes a scenario-based data-quality test harness at `api/internal/validation/`.
Tests route synthetic raw events through the ingest-router contract into
`naap.accepted_raw_events` or `naap.ignored_raw_events` and then assert on the
normalized, canonical, and serving tables that ClickHouse materialized views
populate synchronously.

### Running the harness

```bash
# Runs against the isolated validation profile
make test-validation
```

Or directly:

```bash
CLICKHOUSE_ADDR=localhost:9000 \
CLICKHOUSE_WRITER_USER=naap_writer \
CLICKHOUSE_WRITER_PASSWORD=naap_writer_changeme \
go test -tags=validation ./internal/validation/... -v -timeout=120s
```

Tests skip automatically when `CLICKHOUSE_ADDR` is not set.

### Resolver operational seam

The production resolver deployment now runs in `auto` mode:

- live lateness-window tail updates run first
- the latest eligible same-day dirty hour is repaired next
- queued explicit repair requests run ahead of historical dirty-day replay
- late accepted raw rows can still enqueue exact dirty historical `(org, event_date)` repairs
- remaining closed historical backlog is processed last

Manual `backfill` and `repair-window` remain operator tools, but they are no
longer the normal steady-state path.

### Isolation model

Each test generates a unique `org` tag (`vtest_<rand32>`). All inserted rows and aggregate queries are scoped to that org, so tests can run against a live cluster without touching production data and require no cleanup.

Exception: `agg_orch_state` is keyed by `orch_address` only (no org). Tests that seed this table use unique UIDs per run to avoid cross-test pollution.

### Rule coverage

| Rule | Tests | Notes |
|------|-------|-------|
| `RULE-INGEST-001` | `TestRuleIngest001_AcceptedEventsHaveNonBlankEnvelope`, `TestRuleIngest001_SupportedFamiliesAreContracted` | envelope validity and family contract |
| `RULE-INGEST-002` | `TestRuleIngest002_StreamFamiliesHaveStreamId`, `TestRuleIngest002_PaymentHasRequiredFields` | minimum required fields per family |
| `RULE-INGEST-003` | `TestRuleIngest003_AppSendStreamRequestDoesNotCountAsStarted`, `TestRuleIngest003_ScopeClientTracesDoNotCountAsStarted`, `TestRuleIngest003_UnknownTraceTypesAreDetectable` | ignored subtypes do not inflate lifecycle counters |
| `RULE-LINEAGE-001` | `TestRuleLineage001_DuplicateEventIdCollapsesUnderFinal`, `TestRuleLineage001_AllAcceptedEventsHaveNonBlankId`, `TestRuleLineage001_ReplayDoesNotInflateAggregates` | dedup via full composite key `(org, event_type, event_ts, event_id)`; known gap: SummingMergeTree counts both writes before background merge |
| `RULE-LINEAGE-002` | `TestRuleLineage002_StatusWithoutTraceStartIsDetectable`, `TestRuleLineage002_ClosedStreamHasTraceableLineage` | orphan detection and full lifecycle traceability |
| `RULE-TYPED_RAW-002` | `TestRuleTypedRaw002_CapabilityArrayFansOutToOneRowPerOrch`, `TestRuleTypedRaw002_BlankAddressOrchIsFilteredOut`, `TestRuleTypedRaw002_EmptyCapabilityDataProducesNoOrchRows`, `TestRuleTypedRaw002_CapabilityNameFallback` | array fanout, blank-address filter, empty-data guard, `local_address` → `address` name fallback |
| `RULE-FACT-001` | `TestRuleFact001_HistoricalVersionsAreRecoverable`, `TestRuleFact001_LatestStateReflectsLastEvent` | historical vs FINAL views; correct latest-state selection |
| `RULE-FACT-002` | `TestRuleFact002_LatestStateHasOneRowPerStream`, `TestRuleFact002_NoConflictingLatestRowsForSameStream`, `TestRuleFact002_OrchestratorStateConvergesOnLatest` | one canonical row per stream and orch after FINAL dedup |
| `RULE-LIFECYCLE-002` | `TestRuleLifecycle002_ContractedSubtypesDriveCorrectCounters` | `requested_seen`, `completed`, `selection_outcome`, `orch_swap` counts |
| `RULE-LIFECYCLE-003` | `TestRuleLifecycle003_CleanSuccessStream`, `TestRuleLifecycle003_NoOrchStartupFailure`, `TestRuleLifecycle003_UnexcusedFailureIsDetectable`, `TestRuleLifecycle003_OrphanedCloseDoesNotCountAsKnownStream` | startup outcome classification and orphaned close detection |
| `RULE-LIFECYCLE-007` | `TestRuleLifecycle007_DarkStreamsAreDetectable`, `TestRuleLifecycle007_HealthSignalCoverageRatioIsComputable` | dark stream detection and health signal coverage ratio |
| `RULE-ATTRIBUTION-001` (partial) | `TestRuleAttribution001_OrchAddressNormalizedToLowercase`, `TestRuleAttribution001_StreamStateOrchAddressIsLowercase`, `TestRuleAttribution001_DiscoveryOrchAddressIsLowercase` | lowercase normalization via URI-join resolution chain for status/reliability tables; direct lowercasing for discovery |
| `RULE-ATTRIBUTION-007` | `TestRuleAttribution007_BlankGatewayRateIsComputable`, `TestRuleAttribution007_BlankGatewayRateByFamilyIsComputable` | blank-gateway rate is computable per event family |
| `RULE-AGGREGATE-004` (partial) | `TestRuleAggregate004_ZeroFpsSamplesExcludedFromAggregate`, `TestRuleAggregate004_PositiveFpsSamplesAreAggregated`, `TestRuleAggregate004_ZeroAndPositiveSamplesAreSeparated` | `WHERE fps > 0` filter in `mv_fps_hourly` |

### Known gaps

The following rules require intermediate tables (session spine, attribution decision, GPU-level metrics) or serving-layer APIs not present in the current simplified schema and are not yet covered by the harness:

- `RULE-INGEST-004` — reject/quarantine accounting
- `RULE-TYPED_RAW-001` — lossless non-fanout projection
- `RULE-TYPED_RAW-003` — no-op capability snapshot handling
- `RULE-FACT-003` — selection decision facts
- `RULE-LIFECYCLE-001` — session identity determinism
- `RULE-LIFECYCLE-004` through `RULE-LIFECYCLE-006`, `RULE-LIFECYCLE-008`, `RULE-LIFECYCLE-009` — segment boundaries, parameter updates, demand and SLA hour semantics
- `RULE-ATTRIBUTION-002` through `RULE-ATTRIBUTION-006` — pipeline/model normalization, capability selection, safe failure, hardware-less attribution
- `RULE-AGGREGATE-001` through `RULE-AGGREGATE-003`, `RULE-AGGREGATE-005`, `RULE-AGGREGATE-006` — rollup algebra and SLA formulas
- `RULE-SERVING-001`, `RULE-SERVING-002` — serving output contract
- `RULE-PARITY-001`, `RULE-PARITY-002` — end-to-end parity replay
- `RULE-QUALITY-001`, `RULE-QUALITY-002` — quality diagnostics and scenario coverage
