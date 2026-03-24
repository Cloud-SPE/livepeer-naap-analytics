# Spec: R6 — Orchestrator Leaderboard

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** LDR-001 through LDR-005
**Source events:** `network_capabilities`, `ai_stream_status`, `create_new_payment`, `stream_trace`
**ADRs:** ADR-001, ADR-002

---

## Problem

Consumers — developers, potential orch operators, and network participants — want to
compare orchestrators by performance, reliability, and earnings. This is a **public**
endpoint. Privacy implications are addressed below.

## Privacy and competitive considerations

- ETH addresses are pseudonymous and already public on-chain. Exposing them is acceptable.
- Payment amounts are probabilistic face values, not confirmed settlement. This is documented.
- Orch operators may view this as competitive intelligence. Accepted: the network is
  permissionless and this data is derivable from on-chain activity anyway.
- Individual stream-level data is not exposed in the leaderboard (only aggregates).

**Design decision:** No opt-out mechanism for orchs at launch. The leaderboard is a
network transparency feature. If orch operator feedback requires it, an opt-out can
be added as an additive change.

---

## Scoring model

The leaderboard score is a composite metric. Each dimension is normalised 0–1
across all orchs in the window, then weighted:

| Dimension | Weight | Source |
|-----------|--------|--------|
| Inference FPS (avg) | 30% | `ai_stream_status` |
| Reliability (1 - failure_rate) | 30% | `stream_trace`, `ai_stream_status` |
| Streams handled (volume) | 20% | `stream_trace` |
| Discovery latency (inverted) | 20% | `discovery_results` |

**Design decision:** Weights are configurable via environment variable. The formula
is documented in the API response so consumers understand how scores are derived.
Tradeoff: any weighting is subjective. Documented clearly rather than hidden.

---

## Requirements

### LDR-001: Orchestrator leaderboard

`GET /v1/leaderboard/orchestrators`

**Query params:**
- `?start=` / `?end=` (default: last 7 days)
- `?org=`
- `?pipeline=`
- `?sort=score|fps|reliability|streams|latency` (default: `score`)
- `?limit=50&cursor=<opaque>`

**Response:**
```json
{
  "data": {
    "scoring_model": {
      "fps_weight": 0.30,
      "reliability_weight": 0.30,
      "volume_weight": 0.20,
      "latency_weight": 0.20,
      "min_streams_threshold": 10
    },
    "time_window": { "start": "...", "end": "..." },
    "leaderboard": [
      {
        "rank": 1,
        "address": "0xdef1c70578b2b5e8589a42e26980687fc5153079",
        "name": "speedybird.xyz",
        "org": "daydream",
        "score": 0.924,
        "dimensions": {
          "avg_inference_fps": 17.8,
          "reliability_rate": 0.979,
          "streams_handled": 1842,
          "discovery_latency_ms_p95": 130
        },
        "gpu_count": 2,
        "gpu_models": ["NVIDIA GeForce RTX 5090"],
        "total_vram_gb": 63.6,
        "supported_pipelines": ["live-video-to-video"],
        "total_earned_wei": "48240000000000000",
        "total_earned_eth": "0.04824"
      }
    ]
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] LDR-001-a: Only orchs with ≥ `min_streams_threshold` streams in the window are ranked
- [ ] LDR-001-b: `score` is always between 0 and 1
- [ ] LDR-001-c: `scoring_model` weights are returned in the response so consumers can audit the ranking
- [ ] LDR-001-d: `?pipeline=` filter ranks only by streams of that pipeline type
- [ ] LDR-001-e: `name` derived from orch URI hostname (e.g. `speedybird.xyz` from `https://livepeer-ai.speedybird.xyz:58935`)
- [ ] LDR-001-f: Orchs with no `ai_stream_status` events are excluded from FPS scoring but may still appear ranked by volume/latency

---

### LDR-002: Single orchestrator profile

`GET /v1/leaderboard/orchestrators/{address}`

Full profile for a single orchestrator.

**Response:** Same as one leaderboard entry plus:
- Full history: streams by day (last 30 days)
- FPS trend (last 7 days, hourly)
- Failure event count by type (last 7 days)
- All supported models and current pricing

**Acceptance criteria:**
- [ ] LDR-002-a: Returns `HTTP 404` if address is unknown (never seen in any event)
- [ ] LDR-002-b: Address lookup is case-insensitive (normalise to lowercase)

---

## Data sources and mapping

| Leaderboard field | Source |
|------------------|--------|
| `avg_inference_fps` | `ai_stream_status.data.inference_status.fps` (avg over window) |
| `reliability_rate` | 1 - (failure events / stream events) — see R5 |
| `streams_handled` | count of `gateway_receive_stream_request` where orch is first assigned |
| `discovery_latency_ms_p95` | `discovery_results.data[].latency_ms` for this orch |
| `total_earned_wei` | sum of `create_new_payment.data.faceValue` where `recipient == address` |
| `gpu_count`, `gpu_models` | `network_capabilities.data[].hardware[].gpu_info` |

---

## Out of scope

- Historical score trends (orch score over time)
- Comparison view (side-by-side two orchs)
- Orch self-service dashboard (requires auth, not in scope)
