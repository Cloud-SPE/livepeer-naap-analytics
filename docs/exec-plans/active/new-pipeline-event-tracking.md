# Plan: Add New Pipeline Event Tracking to livepeer-naap-analytics

## Context

The analytics system ingests all Kafka events but only processes 7 event types into `accepted_raw_events`. Everything else lands in `ignored_raw_events`. Real Kafka data shows significant unhandled event volume:

| Topic | Volume (ignored) | Priority |
|---|---|---|
| `pipelines_api_request` | 17.9M | Out of scope |
| `webrtc_stats` | 8.6M | Out of scope |
| `stream_trace` (app subtypes) | 1.9M | Out of scope |
| `ai_batch_request` | 7,442 | **High — AI batch job lifecycle** |
| `worker_lifecycle` | 6,021 | **High — BYOC worker inventory** |
| `job_gateway` | 2,759 | **High — BYOC job routing** |
| `ai_llm_request` | 80 | **High — LLM metrics (= AI batch)** |
| `job_payment` | 42 | **High — BYOC payment tracking** |
| `job_orchestrator` | 20 | **High — BYOC execution detail** |
| `job_auth` | 10 | **High — BYOC auth events** |

**Two separate tracking systems — must NOT be merged:**

| System | Event Sources | Pipelines |
|---|---|---|
| **AI Batch** | `ai_batch_request`, `ai_llm_request` | Fixed: `text-to-image`, `image-to-image`, `image-to-video`, `upscale`, `audio-to-text`, `llm`, `segment-anything-2`, `image-to-text`, `text-to-speech` (cap IDs 27-36) |
| **BYOC Jobs** | `job_gateway`, `job_orchestrator`, `job_payment`, `job_auth`, `worker_lifecycle` | Dynamic: `openai-chat-completions`, `openai-image-generation`, `openai-text-embeddings` (and future ones — stored verbatim, never hardcoded) |

**LLM = AI batch**: `ai_llm_request` provides rich metrics (token counts, TPS, TTFT) that pair with `ai_batch_request` lifecycle events for the `llm` pipeline via `request_id`.

**Key event schemas confirmed from real Kafka data:**

`ai_batch_request_completed`: `request_id`, `pipeline`, `model_id`, `success`, `tries`, `duration_ms`, `orch_url`, `latency_score`, `price_per_unit`, `error_type`, `error`

`ai_llm_request` (completed): `request_id`, `model`, `completion_id`, `orch_url`, `prompt_tokens`, `completion_tokens`, `total_tokens`, `total_duration_ms`, `tokens_per_second`, `latency_score`, `price_per_unit`, `ttft_ms`, `finish_reason`

`job_gateway`: `capability` (openai-*), `type` (submitted/completed/discovery_result/token_fetch_result), `request_id`, `success`, `duration_ms`, `orch_url`, `latency_ms`, `available_capacity`, `orchestrator_info`

`job_payment`: `capability`, `type` (payment_created/payment_balance_sync), `sender`, `balance`, `orchestrator_info`

`job_orchestrator`: `capability`, `duration_ms`, `success`, `http_status`, `worker_url`, `orchestrator_info`, `charged_compute`, `has_payment`, `price_per_unit`, `pixels_per_unit`

`worker_lifecycle`: `capability`, `orchestrator_info`, `price_per_unit`, `worker_options` (array of {model}), `worker_url`

---

## Implementation Plan

### Phase 1: ClickHouse — Accept New Event Types

**File:** `infra/clickhouse/bootstrap/v1.sql` (or new migration file)

#### Step 1a: Update `accepted_raw_events` event_subtype extraction

Extend the CASE statement to compute `event_subtype` from `data.type` for new event types:
```sql
WHEN event_type IN ('ai_batch_request', 'ai_llm_request', 'job_gateway',
                    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle')
  THEN JSONExtractString(data, 'type')
```

#### Step 1b: Update the routing filter

Add to the accepted list (WHERE clause routing `accepted_raw_events` vs `ignored_raw_events`):
- `ai_batch_request`, `ai_llm_request`
- `job_gateway`, `job_payment`, `job_orchestrator`, `job_auth`, `worker_lifecycle`

#### Step 1c: Add normalization tables (5 new materialized views)

**`normalized_ai_batch_job`** — AI batch job lifecycle (fixed pipelines):
```sql
-- Source: accepted_raw_events WHERE event_type = 'ai_batch_request'
--   AND event_subtype IN ('ai_batch_request_received', 'ai_batch_request_completed')
-- Columns: event_id, event_ts, org, gateway, request_id, pipeline, model_id,
--   success (Nullable Bool), tries, duration_ms, orch_url, latency_score,
--   price_per_unit, error_type, error, subtype
```

**`normalized_ai_llm_request`** — LLM-specific metrics (AI batch, links via request_id):
```sql
-- Source: accepted_raw_events WHERE event_type = 'ai_llm_request'
--   AND event_subtype IN ('llm_request_completed', 'llm_stream_completed')
-- Columns: event_id, event_ts, org, gateway, request_id, model, orch_url,
--   prompt_tokens, completion_tokens, total_tokens, total_duration_ms,
--   tokens_per_second, latency_score, price_per_unit, ttft_ms, finish_reason,
--   streaming (Bool), subtype
```

**`normalized_byoc_job`** — BYOC job lifecycle (dynamic capabilities, stored verbatim):
```sql
-- Source: accepted_raw_events WHERE event_type IN ('job_gateway', 'job_orchestrator')
-- IMPORTANT: event_subtype = JSONExtractString(data, 'type') yields the SHORT form
--   ('completed', 'submitted', …) — NOT composite ('job_gateway_completed').
--   Canonical records: WHERE event_subtype = 'completed' AND source_event_type = 'job_gateway'
-- Columns: event_id, event_ts, org, gateway, request_id, capability,
--   success (Nullable Bool), duration_ms, http_status, orch_address, orch_url,
--   worker_url, charged_compute (Bool), latency_ms, available_capacity,
--   error, subtype, source_event_type
-- NOTE: no raw `data` column — typed fields only (plan deviation: "store only typed fields")
```

**`normalized_byoc_auth`** — BYOC auth events:
```sql
-- Source: accepted_raw_events WHERE event_type = 'job_auth'
-- Columns: event_id, event_ts, org, gateway, request_id, capability,
--   orch_address, orch_url, success (Bool), error, subtype
-- NOTE: no raw `data` column
```

**`normalized_worker_lifecycle`** — BYOC worker/model inventory:
```sql
-- Source: accepted_raw_events WHERE event_type = 'worker_lifecycle'
-- Columns: event_id, event_ts, org, gateway, capability, orch_address, orch_url,
--   worker_url, price_per_unit, model, worker_options_raw
-- NOTE: capability stored verbatim; no raw `data` column
```

**`normalized_byoc_payment`** — BYOC payment events (added during implementation):
```sql
-- Source: accepted_raw_events WHERE event_type = 'job_payment'
-- Columns: event_id, event_ts, org, gateway, request_id, capability,
--   orch_address, amount, currency, payment_type (= event_subtype)
-- TTL: 90 days (matches other normalized tables)
```

---

### Phase 2: dbt Warehouse Models

**Directory:** `warehouse/models/staging/`

- `stg_ai_batch_jobs.sql` — from `normalized_ai_batch_job`
- `stg_ai_llm_requests.sql` — from `normalized_ai_llm_request`
- `stg_byoc_jobs.sql` — from `normalized_byoc_job`; one row per completed job
- `stg_byoc_auth.sql` — from `normalized_byoc_auth`
- `stg_worker_lifecycle.sql` — from `normalized_worker_lifecycle`
- `stg_byoc_payments.sql` — from `normalized_byoc_payment`

**Directory:** `warehouse/models/canonical/` (not `marts/` — project uses canonical layer)

- `canonical_ai_batch_jobs.sql` — one row per completed AI batch job:
  - LEFT JOIN `stg_ai_llm_requests` on `request_id` → LLM token/TPS/TTFT columns (null for non-LLM)
  - Time-valid `argMaxIf` attribution join on `orch_uri_norm` → `gpu_id`, `gpu_model_name`, `attributed_model`, `attribution_status` (`resolved`/`unresolved`)
- `canonical_byoc_jobs.sql` — one row per completed BYOC job (job_gateway source only):
  - `argMax` worker join on `capability + orch_address` → `model`, `price_per_unit`
  - Time-valid `argMaxIf` hardware inference on `orch_address` → `gpu_id`, `gpu_model_name`, `attribution_status` (`resolved`/`inferred`/`unresolved`)
- `canonical_ai_llm_requests.sql`, `canonical_byoc_auth.sql`, `canonical_byoc_workers.sql`, `canonical_byoc_payments.sql` — pass-through canonical models

**Directory:** `warehouse/models/api/` (serving layer — Go repo reads only these)

- `api_ai_batch_jobs.sql`, `api_byoc_jobs.sql`, `api_byoc_workers.sql`, `api_byoc_auth.sql`, `api_ai_llm_requests.sql`, `api_byoc_payments.sql`

---

### Phase 3: Go API — New Types

**New file:** `api/internal/types/ai_batch.go`

```go
// AI Batch pipelines (fixed, cap IDs 27-36)
type AIBatchJobSummary struct {
    Pipeline        string  `json:"pipeline"`
    TotalJobs       int64   `json:"total_jobs"`
    SuccessRate     float64 `json:"success_rate"`
    AvgDurationMs   float64 `json:"avg_duration_ms"`
    AvgLatencyScore float64 `json:"avg_latency_score"`
}

type AIBatchLLMSummary struct {
    Model           string  `json:"model"`
    TotalRequests   int64   `json:"total_requests"`
    AvgTokensPerSec float64 `json:"avg_tokens_per_sec"`
    AvgTTFTMs       float64 `json:"avg_ttft_ms"`
    AvgTotalTokens  float64 `json:"avg_total_tokens"`
    SuccessRate     float64 `json:"success_rate"`
}

// BYOC dynamic capabilities
type BYOCJobSummary struct {
    Capability      string  `json:"capability"`  // verbatim, e.g. "openai-chat-completions"
    TotalJobs       int64   `json:"total_jobs"`
    SuccessRate     float64 `json:"success_rate"`
    AvgDurationMs   float64 `json:"avg_duration_ms"`
    ActiveWorkers   int64   `json:"active_workers"`
}

type BYOCWorkerSummary struct {
    Capability      string   `json:"capability"`
    WorkerCount     int64    `json:"worker_count"`
    Models          []string `json:"models"`
    AvgPricePerUnit float64  `json:"avg_price_per_unit"`
}
```

---

### Phase 4: Go API — New Endpoints

**`api/internal/runtime/handlers_ai_batch.go`**

- `GET /v1/ai-batch/summary` → `{data: AIBatchJobSummary[], meta}`
- `GET /v1/ai-batch/jobs` — cursor-paginated → `{data: AIBatchJobRecord[], pagination: CursorPageInfo, meta}`
- `GET /v1/ai-batch/llm/summary` → `{data: AIBatchLLMSummary[], meta}`

**`api/internal/runtime/handlers_byoc.go`**

- `GET /v1/byoc/summary` → `{data: BYOCJobSummary[], meta}`
- `GET /v1/byoc/jobs` — cursor-paginated → `{data: BYOCJobRecord[], pagination: CursorPageInfo, meta}`
- `GET /v1/byoc/workers` → `{data: BYOCWorkerSummary[], meta}`
- `GET /v1/byoc/auth` → `{data: BYOCAuthSummary[], meta}`

**`api/internal/runtime/handlers_dashboard_jobs.go`** (added during implementation)

- `GET /v1/dashboard/jobs/overview` → `{data: DashboardJobsOverview, meta}`
- `GET /v1/dashboard/jobs/by-pipeline` → `{data: DashboardJobsByPipelineRow[], meta}`
- `GET /v1/dashboard/jobs/by-capability` → `{data: DashboardJobsByCapabilityRow[], meta}`

**Response envelope:** All endpoints use `{data, meta}` per ADR-002. List endpoints add `pagination` (cursor-based, not offset). Cursor tokens are opaque; pass `?cursor=<token>` to page forward.

---

### Phase 5: Inspector Tool

**`tools/inspector/src/inspector/request_job_analyzer.py`** (new module):
- Contains the 5 request-job handlers extracted from `analyzer.py`:
  - `_handle_ai_batch_request()` — AI batch job lifecycle by pipeline
  - `_handle_ai_llm_request()` — LLM metrics linked via request_id
  - `_handle_job_gateway()` — BYOC job routing; canonical records are `subtype='completed'` rows
  - `_handle_job_auth()` — BYOC auth success/failure by capability
  - `_handle_worker_lifecycle()` — BYOC worker inventory by capability
- Imported and dispatched from `analyzer.analyze()`

**`tools/inspector/src/inspector/analyzer.py`:**
- `AnalysisResult` dataclass retains all AI batch + BYOC tracking fields
- The 5 request-job handlers are imported from `request_job_analyzer` (module split)

**`tools/inspector/src/inspector/report.py`:**
- AI batch section: pipeline | jobs | success% | avg_duration_ms | avg_latency
- AI batch LLM section: model | requests | avg_tps | avg_ttft_ms | success%
- BYOC jobs section: capability | jobs | success% | avg_duration_ms (dynamic rows)
- BYOC workers section: capability | workers | models | avg_price

---

## Critical Files

| File | Change |
|---|---|
| `infra/clickhouse/bootstrap/v1.sql` (or migration) | Update routing + event_subtype CASE; add 5 normalization tables |
| `warehouse/models/staging/stg_ai_batch_jobs.sql` | New |
| `warehouse/models/staging/stg_ai_llm_requests.sql` | New |
| `warehouse/models/staging/stg_byoc_jobs.sql` | New |
| `warehouse/models/staging/stg_byoc_auth.sql` | New |
| `warehouse/models/staging/stg_worker_lifecycle.sql` | New |
| `warehouse/models/marts/fct_ai_batch_jobs.sql` | New |
| `warehouse/models/marts/fct_byoc_jobs.sql` | New |
| `api/internal/types/ai_batch.go` | New |
| `api/internal/runtime/handlers_ai_batch.go` | New |
| `api/internal/runtime/handlers_byoc.go` | New |
| `tools/inspector/src/inspector/analyzer.py` | Add 5 handlers + 3 dataclasses |
| `tools/inspector/src/inspector/report.py` | Add 4 report sections |

---

## Resolver Impact

**The resolver does not need to change for this work.** Confirmed from real data:

The same physical orchestrator (e.g. xodeapp `0xd003...`) exposes **two separate URIs on two separate reporting paths**:
- `rtav-orch.xodeapp.xyz:28935` → standard `network_capabilities` events → resolver → `canonical_orch_capability_intervals` with GPU data ✓
- `livepeer-ai.xodeapp.xyz:18935` (BYOC endpoint) → `worker_lifecycle` only → NOT in `network_capabilities` → `canonical_orch_capability_intervals` has this URI with all-null capability/GPU columns

| System | Attribution approach | Resolver dependency |
|---|---|---|
| AI Batch | `orch_url` is in the event; JOIN `canonical_orch_capability_intervals` on `orch_url` + timestamp → GPU/model | Reads resolver output (read-only, no changes) |
| BYOC Jobs | JOIN `normalized_worker_lifecycle` on `capability` + `orch_address` → model; JOIN `canonical_orch_capability_intervals` on `orch_address` (not URI) + `hardware_present = 1` → GPU | Reads resolver output (read-only, no changes) |

**BYOC hardware inference — no resolver changes required:** The cross-reference is done at the dbt layer. Both the BYOC endpoint and the standard endpoint share the same `orch_address`. A JOIN on `lower(orch_address)` (not `orch_uri`) against `canonical_orch_capability_intervals WHERE hardware_present = 1` infers GPU hardware for BYOC jobs.

```sql
-- In fct_byoc_jobs.sql, hardware attribution:
LEFT JOIN canonical_orch_capability_intervals ci
  ON lower(ci.orch_address) = lower(bj.orch_address)
  AND ci.hardware_present = 1
  AND ci.valid_from_ts <= bj.event_ts
  AND (ci.valid_to_ts IS NULL OR ci.valid_to_ts > bj.event_ts)
-- Deduplicate via DISTINCT ON gpu_id if multiple URIs match
```

---

## Out of Scope

- `pipelines_api_request` (17.9M/day) — high storage cost, separate discussion needed
- `webrtc_stats` (8.6M/day) — partially handled via `stream_ingest_metrics`
- `stream_trace` with app subtypes (1.9M) — frontend events, separate schema discussion

---

## Verification

1. **ClickHouse routing**: `SELECT event_type, count() FROM naap.accepted_raw_events GROUP BY event_type` — new types should appear
2. **AI batch normalization**: `SELECT count() FROM naap.normalized_ai_batch_job` should return ~7,500+ rows
3. **BYOC normalization**: `SELECT capability, count() FROM naap.normalized_byoc_job GROUP BY capability` — should show openai-* capabilities
4. **dbt**: `dbt run --select stg_ai_batch_jobs stg_ai_llm_requests fct_ai_batch_jobs stg_byoc_jobs fct_byoc_jobs` — all models compile and return rows
5. **API**: `GET /ai-batch/summary` returns pipelines `text-to-image`, `llm`, `upscale`; `GET /byoc/summary` returns `openai-chat-completions`, `openai-image-generation`, `openai-text-embeddings`
6. **Inspector**: Run against live Kafka; AI batch and BYOC sections appear in terminal report
