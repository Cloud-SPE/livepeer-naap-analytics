# Selection-Centered Attribution

## Why

The previous canonical-refresh design rebuilt session-level truth first and then
tried to infer attribution from that reconstructed session state. That made
late/out-of-order data expensive to repair because a small attribution change
could fan out into large rescans of session and serving tables.

The selection-centered design changes the canonical anchor:

`raw events -> normalized events -> canonical_selection_events -> capability versions/intervals -> attribution decisions/current -> session current -> status-hour and serving stores`

This moves the system onto the real business event: orchestrator selection.

## Core rules

- `canonical_selection_events` is the sparse decision spine.
- `canonical_selection_attribution_decisions` is append-only audit history.
- `canonical_selection_attribution_current` is the latest materialized decision.
- Runtime attribution against raw capability churn is forbidden.
- Capability matching must use `canonical_orch_capability_versions` and
  `canonical_orch_capability_intervals`.
- `canonical_session_current_store` and `canonical_status_hours_store` are
  downstream derivatives, never attribution inputs.

## Job types and attribution paths

Three job types flow through the resolver. All share `resolveSelectionDecision`
and produce the same five-class attribution status. Only the event source and
`SelectionEvent` construction differ.

### Live video-to-video (V2V)

- Source: `stream_trace`, `ai_stream_status`, `ai_stream_events`
- SelectionTS: derived from the candidate event timestamp
- ObservedURL / ObservedAddress: from stream trace or status events
- PipelineHintVerbatim: `false` — uses canonical pipeline allow-list
- Output: `canonical_session_current_store`, `canonical_status_hours_store`

### AI batch

- Source: `normalized_ai_batch_job` (received + completed subtypes)
- SelectionTS: `received_at` when available (when the gateway chose the
  orchestrator); `completed_at` as fallback
- ObservedURL: always from the `completed` event — it is the only event that
  carries orch identity
- ObservedAddress: always empty — batch events do not carry orch address
- ObservedPipeline: canonical pipeline name from the job (e.g. `"text-to-image"`)
- PipelineHintVerbatim: `false` — uses canonical pipeline allow-list
- Capability interval source: PerCapability block when `hardware` is null
  (see three-path capability interval building below)
- Output: `canonical_ai_batch_job_store`

### BYOC

- Source: `normalized_byoc_job` (`job_gateway_completed` subtype)
- SelectionTS: `completed_at`
- ObservedURL / ObservedAddress: both carried in job events
- ObservedPipeline: BYOC capability string verbatim (e.g.
  `"openai-chat-completions"`) — never normalized against canonical allow-list
- PipelineHintVerbatim: `true` — bypasses `compatiblePipelineHint`;
  `isCompatible` matches `ObservedPipeline` against `interval.Pipeline`
  case-insensitively and directly
- Model: resolved from `normalized_worker_lifecycle` snapshot (most recent
  event where `event_ts <= job.completed_at`); CI `canonical_model` is fallback
- Worker identity precedence: match `worker_lifecycle` by
  `(org, capability, orch_address, worker_url)` first, then fall back to
  `(org, capability, orch_address)` only when worker URLs are missing
- Model-hint precedence: when worker lifecycle resolves a model, that model is
  pushed into `SelectionEvent.ObservedModelHint` before compatibility
  filtering; BYOC model evidence participates in candidate selection, not only
  final row decoration
- Output: `canonical_byoc_job_store`
a
## Capability interval building — three paths

`buildCapabilityIntervalTemplates` dispatches on the structure of each
orchestrator's capability payload:

**Path 1 — hardware entries present** (live V2V and BYOC orchestrators):
`hardware[]` contains pipeline, model_id, and gpu_info. One interval template
per (pipeline, model, gpu) combination. `HardwarePresent = true` when GPU info
is present.

**Path 2 — no hardware, PerCapability present** (AI batch orchestrators):
`hardware` is null but `capabilities.constraints.PerCapability` carries a map
of capability number → model info. The `capabilityNumberToPipeline` map
translates capability numbers to canonical pipeline names. Creates hardware-less
intervals (`HardwarePresent = false`) with pipeline and model populated from
PerCapability. Capability 37 (byoc) is excluded from this map — BYOC uses
Path 1.

**Path 3 — neither** (fallback): A single hardware-less placeholder interval
with no pipeline or model. Attribution can still resolve orchestrator identity
but will produce `hardware_less` status.

## PipelineHintVerbatim flag

`SelectionEvent.PipelineHintVerbatim` controls the pipeline matching path in
`isCompatible`:

- `false` (live V2V and AI batch): `compatiblePipelineHint` checks the
  canonical pipeline allow-list. Unrecognized pipeline names produce no filter,
  allowing any interval pipeline to match.
- `true` (BYOC): `isCompatible` performs a direct case-insensitive equality
  comparison between `selection.ObservedPipeline` and `interval.Pipeline`.
  This is required because BYOC capability strings (e.g.
  `"openai-chat-completions"`) are not in the canonical allow-list and must not
  be silently dropped.

## Canonical pipeline allow-list

`normalizeCanonicalPipeline` and `compatiblePipelineHint` both recognize:

```
live-video-to-video
text-to-image, image-to-image, image-to-video, text-to-video
audio-to-text, text-to-speech
upscale, llm, image-to-text, segment-anything-2
noop
```

BYOC capability strings are intentionally excluded. They are matched verbatim
via `PipelineHintVerbatim` and stored as-is in `ci.canonical_pipeline` within
BYOC capability intervals.

## Freshness policy

For a given `selection_ts`, the resolver applies:

1. exact interval match
2. nearest prior interval within `10m`
3. bounded future interval within `30s`

If only older evidence exists, the result is `stale`.

## Attribution outcomes

The resolver persists both `attribution_status` and `attribution_reason`.

Steady-state status classes are:

- `resolved`
- `hardware_less`
- `stale`
- `ambiguous`
- `unresolved`

Important unresolved subclasses currently include:

- `missing_uri_snapshot_local_alias_present`
- `missing_uri_snapshot_address_match_present`
- `no_selection_no_orch_excused`

`missing_uri_snapshot_local_alias_present` means the session carried an
observed orchestrator URI, no compatible capability snapshot existed for that
exact URI in the attribution window, but a compatible local/proxy identity
match did exist. This stays `unresolved`; it is not promoted to
`hardware_less`.

`hardware_less` is reserved for sessions where canonical attribution succeeded
but GPU or hardware metadata was absent. It is a distinct attribution class,
not a synonym for unresolved alias/URI gaps.

For AI batch specifically: `hardware_less` is the expected outcome for
orchestrators that report via PerCapability only (Path 2). This is not an
error; it means orchestrator identity was confirmed but no GPU data was
available.

Fact-surface obligations:

- unresolved, stale, ambiguous, and hardware-less sessions remain visible in
  session and status-hour facts
- demand and reliability views must not silently drop these sessions
- only `resolved` counts as fully attributed identity
- GPU-oriented rollups must not treat unresolved URI/local-alias cases as
  hardware-less matches

Operational measurement rule:

- the primary recent-window attribution success metric is
  `resolved + hardware_less` on `selection_outcome = selected`
- this combined rate shows that orchestrator identity and canonical
  pipeline/model attribution succeeded
- `hardware_less` is separated only to show missing hardware metadata; it is
  not a canonicalization failure class
- the remaining selected-session buckets are `stale`, `ambiguous`, and
  `unresolved`

Job-level selection outcome rule:

- `canonical_ai_batch_jobs` and `canonical_byoc_jobs` now carry
  `selection_outcome = selected | no_orch | unknown`
- `selection_outcome` is a job lifecycle field, not an attribution field
- AI-batch uses URI presence as `selected`, failed `error_type = no_orchestrators`
  as `no_orch`, and falls back to `unknown` otherwise
- BYOC uses address/URI presence as `selected`; blank-identity failures remain
  `unknown` until explicit pre-selection failure evidence exists
- request/response attribution quality must be judged on
  `selection_outcome = selected`, not on all jobs combined

## Independent identity fetches

AI batch and BYOC attribution phases each call `fetchCapabilitySnapshots`
independently with their own identity sets:

- **AI batch**: URI-only identities (`orch_url_norm`). Batch events carry no
  orch address.
- **BYOC**: both address (`orch_address`) and URI (`orch_url_norm`) identities.
  Both are available in BYOC completion events.

The live V2V path continues to collect identities from `SelectionEvent` rows
via `collectSelectionIdentities` — unchanged.

## Worker lifecycle resolution (BYOC)

Model and pricing for BYOC jobs come from `normalized_worker_lifecycle`, not
from the job event itself. The resolver:

1. Collects unique orch addresses from the BYOC job batch.
2. Fetches snapshots with a 30-day lookback before the window start — BYOC
   workers may have registered well before the attribution window.
3. For each job, picks the most recent snapshot where
   `snapshot.event_ts <= job.completed_at`, preferring exact worker identity
   matches on `(org, capability, orch_address, worker_url)` and falling back to
   `(org, capability, orch_address)` only when worker URLs are absent.
4. `worker_lifecycle` model takes precedence; CI `canonical_model` is fallback.
5. When worker lifecycle resolves a model, that model becomes the BYOC
   `ObservedModelHint` used by `isCompatible`.

## One-row-per-job contract

- `canonical_ai_batch_jobs` must be one row per `(org, request_id)`.
- `canonical_byoc_jobs` must be one row per `(org, event_id)`; the view exposes
  `event_id` as `request_id` only for downstream contract reuse.
- both canonical job facts must project `selection_outcome` directly from the
  resolver-owned job stores so API, dashboards, and scripts share one
  denominator contract
- `canonical_ai_llm_requests` must be one row per `(org, request_id)` before it
  is joined into `canonical_ai_batch_jobs`.
- Direct one-to-many enrichment joins into canonical job fact views are
  forbidden.
- Resolver-owned job stores may be append/update stores internally, but the
  canonical read layer must collapse them deterministically to one latest row
  per job key before any downstream rollup or API view reads them.

## Dirty partition tracking

Late-arriving events for any of these event types trigger re-attribution of the
affected calendar day:

```
stream_trace, ai_stream_status, ai_stream_events, network_capabilities,
ai_batch_request, ai_llm_request,
job_gateway, job_orchestrator, worker_lifecycle
```

## Operational seam

- `auto` owns the production scheduler:
  - live lateness-window `tail` first
  - latest eligible same-day closed-hour repair second
  - queued explicit bounded repair requests third
  - dirty closed historical `(org, event_date)` partitions from late accepted
    raw arrivals fourth
  - visible closed historical backlog fifth
- `bootstrap` is a bounded/operator backlog catch-up mode
- `backfill` owns deterministic manual historical replays
- `tail` owns `event_ts > cutoff - lateness_window` when run directly
- `repair-window` claims exclusive ownership for its requested window
- `verify` never writes current-state tables

`verify` and `dry-run` still execute the full compute graph, including AI-batch
and BYOC attribution. They skip only inserts/publication.

## Resolver-owned job-store rollout

AI-batch and BYOC public views now read from resolver-owned attribution stores.
That changes deploy behavior:

- a schema migration alone only creates empty stores
- `dbt` publication alone does not backfill those stores
- fresh deploys and fresh-volume rebuilds must include a resolver bootstrap or
  bounded backfill so historical non-streaming jobs are republished into the
  stores before the jobs API and dashboard are considered healthy

Automatic historical repair is quiet-period gated. Closed days dirtied by newly
accepted late arrivals are repaired only after that day has gone quiet for the
configured dirty-repair quiet period, and repeated late arrivals for an already
claimed day are coalesced onto the in-flight repair.

All modes share the same canonical tables, claim model, padded-read semantics,
and exact write ownership rules.
