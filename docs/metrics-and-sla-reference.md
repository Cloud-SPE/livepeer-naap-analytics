# Metrics & SLA Reference

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-15 |
| **Ticket** | TASK 7.7 / [#274](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/274) |
| **Audience** | External developers, partners, integrators |

This document is the community-facing reference for all metrics, scoring formulas, SLA targets, and terminology used in the NaaP analytics platform. For the internal behavioral contract governing data validation, see [`docs/design-docs/data-validation-rules.md`](design-docs/data-validation-rules.md).

---

## 1. Overview

The NaaP analytics platform collects telemetry from AI inference streams on the Livepeer network and exposes it through a REST API. It tracks how well each **orchestrator** (an AI compute node) is performing across the streams it handles.

Two distinct scoring models are used:

| Model | Purpose | Range | API endpoint |
|---|---|---|---|
| **SLA Score** | Per-orchestrator composite reliability and quality score for a given time window. Used for compliance reporting and alerting. | 0–100 | `GET /v1/streaming/sla` |
| **Leaderboard Score** | Competitive ranking logic retained for internal reference only. It is not a public API surface. | 0–100 | _(no public endpoint)_ |

These two scores use different formulas and different inputs — a high leaderboard score does not imply SLA compliance, and vice versa. See §3 for both formulas.

### Quick reference: core metrics to API endpoints

| Metric | Primary API endpoint |
|---|---|
| Output FPS | `GET /v1/streaming/models` |
| Jitter Coefficient | _(endpoint removed; metric tracked internally)_ |
| E2E Latency | _(endpoint removed; metric tracked internally)_ |
| Failure Rate / Output Viability | `GET /v1/streaming/sla` |
| Swap Rate | `GET /v1/streaming/sla` |
| Prompt-to-First-Frame (PTFF) | `GET /v1/streaming/gpu-metrics` |
| SLA Score | `GET /v1/streaming/sla` |
| Leaderboard Score | _(no public endpoint)_ |

---

## 2. Core Metrics

### 2.1 Output FPS

**Definition:** Average frames per second of inference output delivered by an orchestrator. Measures throughput of the AI inference pipeline — how many frames per second the orchestrator's GPU is generating.

**Formula:**
```
avg_output_fps = avg(ai_stream_status.data.inference_status.fps)
```
Samples where `fps = 0` are excluded (they indicate the stream has not yet started producing output, not a genuine zero-FPS reading). The platform also tracks `p95_output_fps` per window.

**Data source:** `ai_stream_status` events → `normalized_ai_stream_status.output_fps`

**SLA target:** No fixed target. Acceptable FPS varies significantly by pipeline (e.g. `streamdiffusion-sdxl` on an A100 vs an H100) and is not directly comparable across GPU types. FPS is used as a component in the leaderboard score and for degradation detection.

**Degradation flag:** If the average FPS for a window drops ≥ 20% relative to the prior hour bucket for the same orchestrator/pipeline combination, the window is flagged as degraded.

**API endpoint:** `GET /v1/streaming/models` — FPS broken down by model

---

### 2.2 Jitter Coefficient

**Definition:** Coefficient of variation in frame delivery timing. Measures the *consistency* of frame delivery — not how fast frames arrive, but how evenly spaced they are. A jitter coefficient of 0.05 means the standard deviation of frame intervals is 5% of the mean interval (very consistent). A value of 0.3 means high irregularity.

**Formula:**
```
fps_jitter_coefficient = StdDev(frame_interval) / Mean(frame_interval)
```
This is a dimensionless ratio (coefficient of variation). Lower is better.

**Data source:** `stream_ingest_metrics.data.stats.track_stats[type=video].jitter` → `normalized_ai_stream_status.fps_jitter_coefficient`

**SLA target: ≤ 0.1** — A coefficient above 0.1 means frame intervals vary by more than 10% of the mean, which produces perceptible temporal inconsistency in interactive AI video streams.

**Status: not yet active.** The underlying jitter signal is collected, but `fps_jitter_coefficient` currently returns `null`. Computed values will be populated in a future release.

**API endpoint:** _(endpoint removed; metric tracked internally)_

---

### 2.3 E2E Latency

**Definition:** End-to-end latency in milliseconds from prompt submission to final inference output. Measures the full round-trip experienced by the user — from sending the prompt to receiving usable AI output.

**Formula:**
```
e2e_latency_ms  — captured per session
avg_e2e_latency_ms = avg(e2e_latency_ms) per orchestrator/pipeline/window
p95_e2e_latency_ms = p95(e2e_latency_ms) per orchestrator/pipeline/window
```

**Data source:** `ai_stream_status` events → `normalized_ai_stream_status.e2e_latency_ms`

**SLA target: ≤ 3000ms (avg per window).** At 3 seconds, users of interactive AI streams experience noticeable lag that disrupts the real-time interaction model. 3s is the standard perceptibility threshold for interactive media.

**Related metric:** `avg_startup_latency_ms` measures only the selection-to-first-output portion and is tracked separately. E2E latency includes all phases from prompt receipt.

**API endpoint:** _(endpoint removed; metric tracked internally)_

---

### 2.4 Failure Rate

**Definition:** Fraction of sessions that fail to produce viable output. Measures the probability that a stream request results in no usable inference output for the user.

**Formula:**
```
failure_rate = output_failed_sessions / requested_sessions

output_viability_rate = 1 − failure_rate
```

Session classification (see §6 for full taxonomy):
- `loading_only_sessions` — inference ran but zero output frames were produced
- `zero_output_fps_sessions` — frames were received but at 0 FPS (unplayable)
- `output_failed_sessions` — union count of sessions that were loading-only or zero-output; one session contributes at most once
- `requested_sessions` — all sessions attempted against this orchestrator

**SLA target: failure_rate < 0.01 (i.e. output_viability_rate > 0.99).** Less than 1% failure rate is the production-grade availability standard for inference services, ensuring > 99% of stream requests produce usable output.

**Note:** Startup failures — where the orchestrator is never selected or the stream never reaches inference — are tracked separately as `startup_success_rate` and are a distinct reliability dimension (see SLA Score, §3.1).

**Data source:** `canonical_session_current` session outcome fields

**API endpoint:** `GET /v1/streaming/sla`

---

### 2.5 Swap Rate

**Definition:** Fraction of sessions where the gateway switched to a different orchestrator mid-stream. A swap indicates the gateway lost confidence in the current orchestrator and redirected the session to another.

**Formula:**
```
swap_rate = total_swapped_sessions / requested_sessions

no_swap_rate = 1 − swap_rate
```

Where:
- `confirmed_swapped` — explicit `orchestrator_swap` event observed
- `inferred_swap` — swap inferred from attribution discontinuity (no explicit event, but session attribution changed)
- `total_swapped_sessions = confirmed_swapped + inferred_swap`

**SLA target:** No fixed threshold. Swap rate is an input to the SLA Score (20% weight via `no_swap_rate`). Minimise.

**Data source:** `stream_trace` events with `data.type = "orchestrator_swap"`

**API endpoint:** `GET /v1/streaming/sla`

---

### 2.6 Prompt-to-First-Frame (PTFF)

**Definition:** Latency in milliseconds from stream request (prompt submission) to the first playable inference output frame. Measures how quickly a user receives *any* visual output after sending a prompt.

**Formula:**
```
prompt_to_playable_latency_ms  — captured per session
avg_prompt_to_first_frame_ms = avg(prompt_to_playable_latency_ms) per window
p95_prompt_to_first_frame_ms = p95(prompt_to_playable_latency_ms) per window
```

**Data source:** `canonical_session_current.prompt_to_playable_latency_ms`

**SLA target:** No fixed threshold. PTFF varies significantly by pipeline and GPU. Used for trend monitoring and outlier detection within a cohort of similar pipeline/GPU combinations.

**Distinction from startup latency:** `startup_latency_ms` covers orchestrator selection to first output only. PTFF includes all gateway processing time before orchestrator selection, making it the user-perceived latency.

**API endpoint:** `GET /v1/streaming/gpu-metrics`

---

### 2.7 CUE (Cumulative Uptime Events)

**Status: not yet implemented.** CUE is a planned metric that will measure the cumulative duration of healthy inference output per orchestrator as a proxy for sustained availability. It has not been defined or computed in the current release. This section will be updated when CUE is implemented.

---

## 3. Derived Metrics

### 3.1 SLA Score

**Purpose:** A single 0–100 composite reliability and quality score per orchestrator per hourly window. Used for compliance reporting and alerting. Null when `requested_sessions = 0`.

**Formula:**
```
sla_score = 100
          × health_signal_coverage_ratio
          × (  0.7 × reliability_score
             + 0.3 × quality_score )

reliability_score = (0.4 × startup_success_rate)
                  + (0.2 × no_swap_rate)
                  + (0.4 × output_viability_rate)

latency_score = (0.6 × ptff_score) + (0.4 × e2e_score)

quality_score = (0.6 × latency_score) + (0.4 × fps_score)
```

**Component breakdown:**

| Component | Weight | Formula | What it measures |
|---|---|---|---|
| `reliability_score` | 70% | `0.4×startup_success_rate + 0.2×no_swap_rate + 0.4×output_viability_rate` | Startup reliability, stability, and output viability |
| `quality_score` | 30% | `0.6×latency_score + 0.4×fps_score` | Relative user-visible quality inside the active benchmark cohort |
| `health_signal_coverage_ratio` | multiplier | `min(health_signal_count / health_expected_signal_count, 1.0)` | Data completeness; defaults to 1.0 when no health signals expected and is capped at 1.0 |

**Quality scoring details**

- `ptff_score` compares `avg_prompt_to_first_frame_ms` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; lower latency scores higher
- `e2e_score` compares `avg_e2e_latency_ms` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; lower latency scores higher
- `fps_score` compares `avg_output_fps` to the prior 7 full UTC days of the network-wide `(pipeline_id, model_id)` benchmark; higher FPS scores higher
- benchmark thresholds use PTFF/E2E `p50 → p90` and FPS `p10 → p50`
- rows at or better than the favorable benchmark edge score `1.0`; rows at or beyond the unfavorable edge score `0.0`; values in between interpolate linearly
- quality components shrink toward neutral `0.5` on sparse current-window evidence
- if the prior 7 full UTC days do not have enough benchmark rows, quality components default to neutral `0.5`
- hardware is intentionally not part of cohort normalization; `gpu_model_name` is exposed separately for drilldown
- the contracted API field remains `sla_score`; there is no parallel legacy or `v2` score surface

The coverage ratio multiplier penalises scores where telemetry data was sparse or missing. A ratio of `1.0` means all expected health signals were received.

**Recommended threshold: ≥ 95.** A score below 95 indicates meaningful reliability gaps in one or more components that warrant investigation.

**Source:** final SLA serving rows are published from the resolver-owned
additive SLA input surface via the `api_base_*` scoring helpers, then exposed
through `warehouse/models/api/api_hourly_streaming_sla.sql` as the contracted
read model.

**API endpoint:** `GET /v1/streaming/sla`

---

### 3.2 Leaderboard Score

**Purpose:** A competitive ranking score for the public leaderboard. Ranks orchestrators relative to each other within a time window. Distinct from SLA Score — it rewards high-performing orchestrators, not just reliable ones.

**Formula:**
```
leaderboard_score = 100 × (  0.30 × normFPS
                            + 0.30 × (1 − normDegradedRate)
                            + 0.20 × normVolume
                            + 0.20 × (1 − normLatency)  )
```

All four inputs are **min-max normalised** across all orchestrators competing in the same window. The normalisation means scores are relative, not absolute — a score of 80 means the orchestrator is performing well compared to its peers, not that any specific metric threshold was met.

**Component breakdown:**

| Component | Weight | Raw metric | Higher = better? |
|---|---|---|---|
| FPS quality | 30% | `avg_output_fps` | Yes |
| Inference quality | 30% | `1 − degraded_rate` | Yes (lower degraded rate) |
| Volume | 20% | `streams_handled` | Yes |
| Latency | 20% | `1 − avg_latency_ms` | Yes (lower latency) |

**Range:** 0–100; when all orchestrators have identical values on a dimension, all receive 1.0 for that component.

**Source:** `api/internal/service/service.go` (`scoreLeaderboard`)

**API endpoint:** _(leaderboard endpoints removed; no public endpoint)_

---

## 4. Metric Dimensions

All metrics can be filtered and grouped by the following dimensions via API query parameters.

| Dimension | API parameter | Example values | Notes |
|---|---|---|---|
| Organisation | `org` | `org-abc123` | Customer/tenant scope; omit for network-wide view |
| Orchestrator address | `orch_address` | `0x1a2b3c…` | Ethereum address identifying the compute node |
| Pipeline | `pipeline` | `streamdiffusion-sdxl` | AI pipeline type |
| Model | _(via pipeline)_ | `stabilityai/sdxl-turbo` | Specific model within a pipeline |
| GPU hardware | _(in response)_ | `A100`, `H100` | GPU model name; available in `/v1/streaming/gpu-metrics` |
| Time window start | `start` / `end` | `2026-03-01T00:00:00Z` | ISO 8601 UTC; windows are hourly |
| Granularity | `granularity` | `5m`, `1h`, `1d` | Time bucket size; not all endpoints support all granularities |

**Standard pagination parameters for cursor-paginated list endpoints:** `limit`
(default 50) and `cursor` (opaque continuation token from the previous page).
Responses use the shared `{data, pagination, meta}` envelope with
`pagination.next_cursor`, `pagination.has_more`, and `pagination.page_size`.

---

## 5. SLA Targets Reference

| Metric | Target | Rationale |
|---|---|---|
| Output FPS | No fixed target | Baseline varies by GPU type and pipeline; not directly comparable across hardware cohorts |
| Jitter Coefficient | ≤ 0.1 | Coefficient of variation > 0.1 produces perceptible temporal irregularity in interactive AI video |
| E2E Latency | ≤ 3000ms avg | 3 seconds is the perceptibility threshold for interactive AI stream response; exceeding it breaks real-time UX |
| Failure Rate | < 1% | Production-grade availability standard; ensures > 99% of stream requests produce usable output |
| PTFF | No fixed target | Varies by pipeline; used for trend monitoring and outlier detection within cohorts |
| SLA Score | ≥ 95 recommended | Score below 95 indicates meaningful reliability gaps in one or more scored components |
| CUE | Not yet defined | Metric not yet implemented |

---

## 6. Session Outcome Classification

Every stream request results in a session that is classified into one or more outcome states. These states are the direct inputs to the failure rate, swap rate, and SLA Score calculations.

### Session lifecycle

```
Stream request received
        │
        ▼
  Orchestrator selected?
   ├── No  → no_orch_session (startup failure — no orch available)
   └── Yes → startup attempted
               │
               ▼
         First inference output received?
          ├── No  → startup_failed_session
          │         (subdivided: excused / unexcused — see below)
          └── Yes → startup_success_session
                     │
                     ▼
               Output quality?
                ├── loading_only_session   (inference running, zero output frames)
                ├── zero_output_fps_session (frames received, 0 FPS — unplayable)
                ├── swapped_session        (orch replaced mid-stream)
                └── completed normally
```

### Outcome states

| State | Counted as failure? | Description |
|---|---|---|
| `startup_success` | No | Orchestrator selected and first inference output received |
| `startup_failed_unexcused` | Yes | Selection failed or no output received; no accepted reason |
| `startup_failed_excused` | No (excused) | Failed but for an accepted reason (e.g. no orchestrator available during a known network event) |
| `no_orch_session` | Yes | No orchestrator available at request time |
| `loading_only` | Yes (output failure) | Inference ran but produced zero output frames |
| `zero_output_fps` | Yes (output failure) | Frames received but at 0 FPS (unplayable) |
| `confirmed_swapped` | Swap counted | Explicit `orchestrator_swap` event observed |
| `inferred_swap` | Swap counted | Swap inferred from attribution discontinuity |

### Failure event types

These event types appear in `stream_trace` records and identify specific failure causes:

| Event type | Meaning |
|---|---|
| `gateway_no_orchestrators_available` | No orchestrator available at request time |
| `orchestrator_swap` | Gateway replaced the orchestrator mid-stream |
| `inference_restart` | Orchestrator restarted the inference pipeline |
| `inference_error` | Inference pipeline encountered an error |
| `DEGRADED_INFERENCE` | Inference quality below acceptable threshold |
| `DEGRADED_INPUT` | Input stream quality issue detected |

---

## 7. Glossary

| Term | Definition |
|---|---|
| **Orchestrator** | An AI compute node on the Livepeer network, identified by its Ethereum address. Runs inference pipelines (e.g. image generation, video diffusion) for stream sessions. |
| **Gateway** | The Livepeer network component that receives stream requests, selects an orchestrator, and routes inference traffic. Not an orchestrator — it coordinates, not computes. |
| **Pipeline** | A named AI inference workflow, e.g. `streamdiffusion-sdxl`. Defines the model type and processing graph used for a stream. |
| **Model** | A specific AI model (e.g. `stabilityai/sdxl-turbo`) used within a pipeline. Multiple models can be available within a single pipeline type. |
| **Session** | A single end-to-end stream interaction: from the time a stream request is received by the gateway to stream termination or error. The unit of measurement for all reliability metrics. |
| **Stream** | The continuous data connection carrying inference output from orchestrator to consumer during a session. |
| **Inference** | The process of running an AI model to produce output (e.g. generating image frames) given an input prompt. |
| **Attribution** | The association of a session's inference output with a specific orchestrator. Attribution can change mid-session if a swap occurs. |
| **Health Signal** | A periodic telemetry event indicating the orchestrator is alive and processing. Used to compute `health_signal_coverage_ratio`. |
| **Health Signal Coverage Ratio** | `health_signal_count / health_expected_signal_count`. Measures telemetry completeness for a window. Defaults to 1.0 when no signals expected. Applied as a multiplier in the SLA Score. |
| **SLA Score** | A 0–100 reliability score per orchestrator per hourly window. Weighted combination of startup success, swap avoidance, and output viability, scaled by data completeness. |
| **Leaderboard Score** | A 0–100 competitive score per orchestrator. Min-max normalised across all active orchestrators in the window; rewards high FPS, low degradation, high volume, and low latency. Leaderboard endpoints have been removed; the score formula is preserved here for reference. |
| **Output Viability Rate** | `1 − failure_rate`. Fraction of sessions that produced usable output. |
| **Startup Success Rate** | Fraction of requested sessions where an orchestrator was successfully selected and first inference output was received. |
| **No-Swap Rate** | Fraction of sessions completed without mid-stream orchestrator replacement. |
| **Jitter Coefficient** | Coefficient of variation of frame delivery intervals (`StdDev / Mean`). Dimensionless; measures consistency of frame timing. |
| **E2E Latency** | End-to-end latency in ms from prompt submission to final inference output. Covers the full user-perceived round-trip. |
| **PTFF (Prompt-to-First-Frame)** | Latency in ms from stream request to first playable output frame. A subset of E2E latency that measures time-to-first-output specifically. |
| **CUE (Cumulative Uptime Events)** | Planned metric for cumulative healthy inference duration. Not yet implemented. |
| **Loading Only** | Session state where inference is running but zero output frames are produced — the stream is in a perpetual loading state. |
| **Zero Output FPS** | Session state where output frames are received but at 0 FPS, making the stream unplayable. |
| **Excused Failure** | A startup failure attributed to an accepted external cause (e.g. no orchestrators available network-wide). Not counted against the orchestrator's reliability score. |
| **Degraded Inference** | Inference quality detected as below an acceptable threshold, typically by the gateway. Triggers the `DEGRADED_INFERENCE` stream trace event. |
| **Degraded Input** | Input stream quality detected as insufficient for inference, triggering the `DEGRADED_INPUT` stream trace event. |
| **No-Orch Rate** | Fraction of sessions where no orchestrator was available at request time. Tracked separately from other startup failures. |
| **Min-Max Normalisation** | Scaling technique that maps a set of values to [0, 1] relative to the observed minimum and maximum. Used in leaderboard scoring; values are not absolute thresholds. |
| **Window** | A time bucket (typically 1 hour) over which metrics are aggregated. Identified by `window_start` (UTC). |
