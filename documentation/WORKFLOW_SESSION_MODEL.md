# Workflow Session Model (Locked Draft)

**Purpose**

Provide a generalized lifecycle model that supports streams today and future AI inference/transform workflows without reworking core correlation or ClickHouse schema.

## Core Identity Model

The core lifecycle entity is a **workflow session**. It represents a unit of work that flows through Gateway → Orchestrator → Worker → GPU over time.

Definition rules (Flink-owned, deterministic):

- `workflow_session_id`: deterministic session key with precedence
  - `stream_id` when present
  - else `session_id` (payment / legacy)
  - else `request_id`
  - else `event_id`
- `workflow_type`: `stream`, `inference`, `transform`, `payment`, `discovery`, `capability`, `unknown`
- `workflow_id`: pipeline/workflow identifier (today: `pipeline_id` or `pipeline`)
- `pipeline_id`: required for inference workflows (distinct from human-readable `pipeline`)

Required correlation fields in all curated facts (when deterministically available):

- `workflow_session_id`
- `workflow_type`
- `workflow_id`
- `pipeline_id`
- `event_id` (root `id`)
- `event_timestamp`
- `request_id` (nullable)
- `stream_id` (nullable)
- `session_id` (nullable)
- `gateway` (nullable but required if present in root)
- `orchestrator_address` (nullable)
- `orchestrator_url` (nullable)
- `region` (nullable, only when present in payload)
- `capability` (nullable)

Optional (best-effort, not guaranteed deterministic today):

- `worker_id`
- `gpu_id`
- `wallet`

## Proposed Fact Tables (Grain + Metric Mapping)

The tables below are the "clean facts" produced by Flink. Each table explicitly states its grain and the metrics it supports. Aggregate tables (rollups) are built only on top of these curated facts.

### `workflow_sessions` (one row per session)

Recommended fields:

- `workflow_session_id` (String)
- `workflow_type` (LowCardinality String)
- `workflow_id` (String)
- `request_id` (String, nullable)
- `stream_id` (String, nullable)
- `session_id` (String, nullable)
- `gateway` (String, nullable)
- `orchestrator_address` (String, nullable)
- `worker_id` (String, nullable)
- `gpu_id` (String, nullable)
- `wallet` (String, nullable)
- `region` (LowCardinality String, nullable)
- `capability` (LowCardinality String, nullable)
- `model_id` (String, nullable)
- `start_time` (DateTime64)
- `end_time` (DateTime64, nullable)
- `status` (LowCardinality String, nullable)
- `swap_count` (UInt16, default 0)
- `event_count` (UInt32, default 0)
- `ingestion_timestamp` (DateTime64)

**Grain:** 1 row per `workflow_session_id`.

**Metric support:** session counts, failure rate by session, swap rate by session, startup latency (derived from related facts), lifecycle duration, per-session filtering.

### `workflow_session_segments` (one row per segment or actor change)

Used for orchestrator swaps and GPU/worker changes during a session.

Recommended fields:

- `workflow_session_id` (String)
- `segment_index` (UInt16)
- `segment_start_time` (DateTime64)
- `segment_end_time` (DateTime64, nullable)
- `gateway` (String, nullable)
- `orchestrator_address` (String, nullable)
- `orchestrator_url` (String, nullable)
- `worker_id` (String, nullable)
- `gpu_id` (String, nullable)
- `region` (LowCardinality String, nullable)
- `reason` (LowCardinality String, nullable) -- swap, timeout, restart, etc.
- `source_event_type` (LowCardinality String)
- `source_event_id` (String)

Notes:

- `workflow_sessions` powers rollups and high-level KPIs.
- `workflow_session_segments` provides lifecycle attribution and swap analysis.
- Both are Flink-produced; ClickHouse only stores and aggregates.

**Grain:** 1 row per segment (contiguous period of a session with a stable actor set).

**Metric support:** swap rate, orchestrator/gateway attribution, per-segment performance rollups, drill-down timelines.

### `fact_status_event` (AI stream status)

Use the existing `ai_stream_status` as the curated fact table, but extend it to include core correlation fields (`workflow_session_id`, `workflow_type`, `workflow_id`, `event_id`, `gateway`, `orchestrator_address`, `orchestrator_url`, `region`, `capability` when available).

**Grain:** 1 row per `ai_stream_status` event.

**Metric support:** output FPS, jitter (via windowed aggregates), prompt-to-first-frame (requires `start_time`), restart counts, error rates (with `ai_stream_events`).

### `fact_ingest_event` (stream ingest metrics)

Use the existing `stream_ingest_metrics` as curated facts; extend with correlation fields (especially `workflow_session_id`, `gateway`, `orchestrator_address` if deterministically derivable).

**Grain:** 1 row per ingest metrics event.

**Metric support:** up/down bandwidth (bytes per window), packet loss, ingest jitter, network latency.

### `fact_trace_event` (stream trace)

Use the existing `stream_trace_events` as curated facts; extend with correlation fields (`workflow_session_id`, `gateway`, `orchestrator_address`, `orchestrator_url`).

**Grain:** 1 row per trace event.

**Metric support:** E2E latency, gateway/orchestrator hop timing, swap detection.

### `fact_ai_event` (AI lifecycle events)

Use the existing `ai_stream_events` as curated facts; extend with correlation fields.

**Grain:** 1 row per AI event.

**Metric support:** failure rate, error rates by workflow/operator.

### `fact_payment_event`

Use the existing `payment_events` as curated facts; extend with correlation fields.

**Grain:** 1 row per payment event.

**Metric support:** payment success ratios, economic metrics by workflow/operator.

## Aggregate Tables (Rollups + Join-Back Keys)

Aggregates are materialized views that roll up curated facts by time window and dimensions. Each aggregate must specify the join keys back to facts for drill-through.

### `agg_performance_window_5s|1m|1h`

**Source:** `fact_status_event` (ai_stream_status)

**Group keys:** `window_start`, `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway`, `orchestrator_address`, `region`

**Join-back keys to facts:** same as group keys + `event_timestamp` range in the window.

### `agg_network_window_5s|1m|1h`

**Source:** `fact_ingest_event`

**Group keys:** `window_start`, `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway`, `orchestrator_address`, `region`

**Join-back keys:** same as above + window time range.

### `agg_reliability_window_1m|1h`

**Source:** `fact_ai_event` + `workflow_sessions`

**Group keys:** `window_start`, `workflow_type`, `workflow_id`, `gateway`, `orchestrator_address`, `region`

**Join-back keys:** same as above; use `workflow_session_id` for session-level drill-down.

### `agg_latency_window_1m|1h`

**Source:** `fact_trace_event`

**Group keys:** `window_start`, `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway`, `orchestrator_address`

**Join-back keys:** same as above + window range.

## Correlation Field Audit (Current Events)

The table below lists fields currently present in payloads plus deterministic enrichments available in Flink.

| Event Type | Present in Payload/Root | Deterministic Enrichment in Flink | Gaps / Notes |
| --- | --- | --- | --- |
| `ai_stream_status` | `stream_id`, `request_id`, `pipeline`, `pipeline_id`, `state`, `orchestrator_address`, `orchestrator_url` + root `gateway`, root `id` | Add `workflow_session_id`, `workflow_type`, `workflow_id`, `event_id`, `gateway` | `region`, `worker_id`, `gpu_id`, `wallet` not present in payload |
| `stream_ingest_metrics` | `stream_id`, `request_id`, `pipeline_id` + root `id` | Add `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway` | `orchestrator_address` not in payload; can be correlated via sessionization using trace/status events |
| `stream_trace` | `stream_id`, `request_id`, `pipeline_id`, `trace_type`, `orchestrator_address`, `orchestrator_url` + root `id` | Add `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway` | Good for swap/segment edges |
| `ai_stream_events` | `stream_id`, `request_id`, `pipeline`, `pipeline_id`, `event_type`, `capability` + root `id` | Add `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway` | `orchestrator_address` missing; can be inferred by sessionization |
| `network_capabilities` | `orchestrator_address`, `orch_uri`, `gpu_id`, `gpu_name`, `pipeline`, `model_id`, `runner_version` + root `id` | Add `event_id`, `workflow_type=capability` | Not session-scoped; use as dimension source |
| `discovery_results` | `orchestrator_address`, `orchestrator_url`, `latency_ms` + root `id` | Add `workflow_type=discovery`, `gateway` | Not session-scoped |
| `create_new_payment` | `request_id`, `session_id`, `manifest_id`, `sender`, `recipient`, `orchestrator`, `capability` + root `id` | Add `workflow_session_id` using `session_id` fallback; `workflow_type=payment` | No stream_id; correlation via session_id/request_id |

## Deterministic Enrichment Rules (Flink)

Flink should append the following fields to every curated fact row:

- `event_id`: root `id`
- `workflow_session_id`: `stream_id` → `session_id` → `request_id` → `event_id`
- `workflow_type`: by `event_type` mapping
- `workflow_id`: `pipeline_id` if present else `pipeline` else empty
- `gateway`: root `gateway` (when available)
- `orchestrator_address`, `orchestrator_url`: from payload when present; else set from session correlation (trace/status)

## Backfill Approach (Idempotent)

- Backfills are done by replaying raw events through the Flink pipeline with the same deterministic sessionization and dedup rules.
- Chunk by time (e.g., day/week) and write to backfill tables or directly to fact tables with deterministic keys.
- Aggregate MVs can be rebuilt from facts per partition/window after backfill.

## Correlation Field Matrix (Current Events)

| Event Type | Present in Payload/Root | Correlation Gaps | Notes |
| --- | --- | --- | --- |
| `ai_stream_status` | `stream_id`, `request_id`, `gateway` (root), `orchestrator_address`, `orchestrator_url`, `pipeline`, `pipeline_id`, `state` | `region`, `gpu_id`, `worker_id`, `wallet` | Good core for workflow sessions. Add gateway + region to table for filtering. |
| `stream_ingest_metrics` | `stream_id`, `request_id`, `pipeline_id` | `gateway`, `orchestrator_address`, `region`, `gpu_id`, `worker_id` | Needs enrichment from session correlation or additional producer fields. |
| `stream_trace` | `stream_id`, `request_id`, `pipeline_id`, `orchestrator_address`, `orchestrator_url`, `trace_type`, `data_timestamp` | `gateway`, `region`, `gpu_id`, `worker_id` | Strong for segment changes (swap events). Add gateway from root. |
| `ai_stream_events` | `stream_id`, `request_id`, `pipeline`, `pipeline_id`, `event_type`, `capability` | `gateway`, `orchestrator_address`, `region`, `gpu_id`, `worker_id` | Use for lifecycle edges (errors, restarts). Add gateway from root. |
| `network_capabilities` | `orchestrator_address`, `orch_uri`, `gpu_id`, `gpu_name`, `pipeline`, `model_id`, `runner_version`, `capacity` | `region`, `gateway`, `worker_id` | Dimension source for GPU enrichment; not session-scoped. |
| `discovery_results` | `orchestrator_address`, `orchestrator_url`, `latency_ms` | `gateway`, `region` | Useful for gateway discovery performance. Add gateway from root. |
| `create_new_payment` | `request_id`, `session_id`, `manifest_id`, `sender`, `recipient`, `orchestrator`, `capability` | `stream_id`, `gateway`, `region`, `gpu_id`, `worker_id` | Potential wallet correlation; maps to `workflow_session_id` via `session_id` or `request_id`. |

## Immediate Recommendations

- Standardize enrichment in Flink so every emitted ClickHouse row includes `workflow_session_id`, `workflow_type`, `workflow_id`, `gateway`, `orchestrator_address`, and `region` when available.
- Add `gateway` to `stream_ingest_metrics`, `stream_trace_events`, `ai_stream_events`, and `discovery_results` tables to improve correlation and filtering.
- Define `workflow_session_id` derivation and embed it in all curated fact tables to avoid query-time dedup or joins.
