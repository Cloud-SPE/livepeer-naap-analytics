# Spec: R1 — Network State

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** NET-001 through NET-012
**Source events:** `network_capabilities`, `discovery_results`
**ADRs:** ADR-001, ADR-002

---

## Problem

API consumers need to understand the current state of the Livepeer network:
how many orchestrators are active, how many GPUs are available, which AI models
are running, and what the current pricing is.

## Solution

Derive network state from `network_capabilities` events, which are periodic full
snapshots of all orchestrators. Augment with `discovery_results` for latency data.
Materialise into ClickHouse views that are queryable in sub-second time.

**Design decision:** Use the most recent `network_capabilities` event per orchestrator
address as the source of truth for current state. An orchestrator is considered "active"
if it appeared in a `network_capabilities` event within the last 10 minutes.
This threshold is configurable. Tradeoff: if an orch stops broadcasting, it will
appear stale after 10 minutes rather than immediately. Accepted as a reasonable lag.

---

## Requirements

### NET-001: Network summary endpoint

`GET /v1/network/summary`

Returns a snapshot of the entire network at query time.

**Response:**
```json
{
  "data": {
    "org": "all | daydream | cloudspe",
    "snapshot_time": "2026-03-24T09:00:00Z",
    "orchestrators": {
      "total_registered": 53,
      "total_active": 42,
      "active_threshold_minutes": 10
    },
    "gpus": {
      "total_count": 187,
      "total_vram_gb": 4523.4,
      "models": {
        "NVIDIA GeForce RTX 5090": 89,
        "NVIDIA GeForce RTX 4090": 61,
        "NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation": 27
      }
    },
    "pipelines": {
      "live-video-to-video": { "orchs": 38, "models": ["streamdiffusion-sdxl", "streamdiffusion-sdxl-faceid"] },
      "llm": { "orchs": 4, "models": ["meta-llama/Meta-Llama-3.1-8B-Instruct"] }
    },
    "capacity": {
      "live-video-to-video": 42,
      "llm": 4
    }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] NET-001-a: Response time ≤ 200ms at P99
- [ ] NET-001-b: `orchestrators.total_active` only counts orchs seen within `active_threshold_minutes`
- [ ] NET-001-c: `?org=` filter returns data scoped to that org's Kafka topic

---

### NET-002: Orchestrator list endpoint

`GET /v1/network/orchestrators`

Returns a paginated list of all known orchestrators with their current capabilities.

**Query params:**
- `?org=` — filter by org
- `?active_only=true` — only orchs active in last N minutes
- `?pipeline=live-video-to-video` — filter by supported pipeline
- `?limit=50&cursor=<opaque>`

**Response per orchestrator:**
```json
{
  "address": "0xdef1c70578b2b5e8589a42e26980687fc5153079",
  "name": "speedybird.xyz",
  "uri": "https://livepeer-ai.speedybird.xyz:58935",
  "version": "0.8.8",
  "org": "daydream",
  "last_seen": "2026-03-24T09:23:50Z",
  "is_active": true,
  "gpus": [
    { "id": "GPU-...", "name": "NVIDIA GeForce RTX 5090", "vram_gb": 31.8 }
  ],
  "gpu_count": 2,
  "total_vram_gb": 63.6,
  "pipelines": ["live-video-to-video"],
  "models": { "live-video-to-video": ["streamdiffusion-sdxl"] },
  "capacity": { "live-video-to-video": 2 },
  "pricing": [
    { "pipeline": "live-video-to-video", "model": "streamdiffusion-sdxl", "price_per_unit": 1588, "pixels_per_unit": 1 }
  ]
}
```

**Acceptance criteria:**
- [ ] NET-002-a: Pagination is stable — inserting new events mid-query does not skip or duplicate records
- [ ] NET-002-b: `is_active` accurately reflects last-seen threshold
- [ ] NET-002-c: `address` is normalised to lowercase hex
- [ ] NET-002-d: Orchs with no hardware data in events are returned with empty `gpus: []`

---

### NET-003: GPU inventory endpoint

`GET /v1/network/gpus`

Returns aggregated GPU counts and VRAM by model across the network.

**Response:**
```json
{
  "data": {
    "total_gpus": 187,
    "total_vram_gb": 4523.4,
    "by_model": [
      { "model": "NVIDIA GeForce RTX 5090", "count": 89, "total_vram_gb": 2830.2, "vram_per_gpu_gb": 31.8 },
      { "model": "NVIDIA GeForce RTX 4090", "count": 61, "total_vram_gb": 1464.0, "vram_per_gpu_gb": 24.0 }
    ],
    "by_orchestrator_count": {
      "1_gpu": 22,
      "2_to_4_gpus": 14,
      "5_plus_gpus": 6
    }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] NET-003-a: Counts reflect only active orchestrators
- [ ] NET-003-b: GPU UUIDs are deduplicated (same GPU ID on multiple events counted once)

---

### NET-004: Model availability endpoint

`GET /v1/network/models`

Returns which AI models are available on the network, how many orchs support them,
and current pricing range.

**Response:**
```json
{
  "data": [
    {
      "pipeline": "live-video-to-video",
      "model": "streamdiffusion-sdxl",
      "warm_orchs": 38,
      "total_capacity": 42,
      "price_min_wei_per_pixel": 1588,
      "price_max_wei_per_pixel": 2100,
      "price_avg_wei_per_pixel": 1750
    }
  ]
}
```

**Acceptance criteria:**
- [ ] NET-004-a: Only includes models marked `warm: true` in capabilities constraints
- [ ] NET-004-b: Price range derived from `capabilities_prices` across active orchs

---

## Data sources and mapping

| API field | Source event | Source field path |
|-----------|-------------|------------------|
| `address` | `network_capabilities` | `data[].address` |
| `gpu_count` | `network_capabilities` | `data[].hardware[].gpu_info` (count keys) |
| `total_vram_gb` | `network_capabilities` | `data[].hardware[].gpu_info[n].memory_total` |
| `is_active` | `network_capabilities` | Derived: `timestamp` within threshold |
| `pricing` | `network_capabilities` | `data[].capabilities_prices[]` |
| `models` | `network_capabilities` | `data[].capabilities.constraints.PerCapability` |
| `discovery latency` | `discovery_results` | `data[].latency_ms` |

---

## Out of scope

- Historical GPU count trends (belongs in R2 / stream activity time-series)
- Per-GPU utilisation (not in current event data)
- Orch geographic location (not in current event data)
