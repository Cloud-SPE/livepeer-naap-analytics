# Spec: R1 — Network State

**Status:** approved  
**Priority:** P0 / launch  
**Requirement IDs:** NET-001 through NET-003  
**Source events:** `network_capabilities`, `discovery_results`

## Problem

API consumers need to understand which orchestrators are active, which models
are available, and how current supply compares to observed demand.

## Solution

Use the latest capability and inventory state published into the semantic
serving layer to expose three current network-state surfaces:

- `GET /v1/network/orchestrators`
- `GET /v1/network/models`
- `GET /v1/network/capacity`

## Requirements

### NET-001: Orchestrator list

`GET /v1/network/orchestrators`

Returns a paginated list of orchestrators ordered by `last_seen` descending.

**Query params:**

- `?org=` — optional org filter
- `?active_only=true` — optional warm-state filter
- `?limit=` / `?cursor=` — cursor-pagination controls

**Representative fields:**

| Field | Meaning |
|---|---|
| `address` | canonical lowercase orchestrator address |
| `name` | display name derived from URI hostname when available |
| `uri` | current orchestrator service URI |
| `version` | current orchestrator version when present |
| `last_seen` | last capability/inventory observation timestamp |
| `is_active` | whether the row falls within the active warm-state threshold |
| capability payload | current hardware, model, and pricing source payload |

Required behavior:

- supports `org`, `active_only`, `limit`, and `cursor`
- returns normalized orchestrator identity and current capability payload
- lowercases canonical addresses
- keeps records with missing hardware metadata rather than dropping them
- orders by most recent `last_seen`
- returns the shared list envelope `{data, pagination, meta}` with
  `pagination.next_cursor`, `pagination.has_more`, and `pagination.page_size`

### NET-002: Model availability

`GET /v1/network/models`

Returns model availability per pipeline, including warm orchestrator count and
price range.

**Query params:**

- `?org=` — optional org filter
- `?pipeline=` — optional pipeline filter
- `?limit=` — optional result cap

**Representative fields:**

| Field | Meaning |
|---|---|
| `pipeline` | canonical pipeline identifier |
| `model` | canonical model identifier |
| `warm_orch_count` | count of warm orchestrators advertising the model |
| `total_capacity` | published capacity for the `(pipeline, model)` pair |
| `price_*` | min, max, and average published pricing across warm orchestrators |

Required behavior:

- supports `org`, `pipeline`, and `limit`
- derives counts and price range from current capability snapshots
- exposes only models represented in the current serving contract
- derives pricing from the published capability pricing data, not ad hoc request-time reconstruction

### NET-003: Network capacity

`GET /v1/network/capacity`

Returns current GPU supply versus active demand grouped by `(pipeline, model)`.

**Query params:**

- `?org=` — optional org filter

**Representative fields:**

| Field | Meaning |
|---|---|
| `pipeline` | canonical pipeline identifier |
| `model` | canonical model identifier |
| `warm_orch_count` | number of warm orchestrators advertising supply |
| `active_stream_count` | current active demand count |
| `utilization` | active demand divided by warm supply |
| `total_vram` | total advertised VRAM for the supply cohort |

Required behavior:

- supports `org`
- exposes warm orchestrator count, active stream count, utilization, and total VRAM
- is computed from published serving state rather than ad hoc raw-table scans
- keeps supply and demand aligned on the same canonical `(pipeline, model)` vocabulary

## Data Sources

| Surface | Primary source |
|---|---|
| orchestrators | latest capability and inventory state |
| models | capability snapshots and published availability rollups |
| capacity | published capacity and demand serving models |

## Source Mapping

| Concept | Source event or serving layer |
|---|---|
| orchestrator identity | `network_capabilities` snapshots and published latest orchestrator state |
| hardware inventory | capability hardware payload and published inventory rollups |
| model support | capability constraints and pricing payloads |
| active demand | published demand and capacity serving models |
| warm-state threshold | latest-seen capability freshness policy in the serving layer |

## Out of Scope

- historical GPU inventory trend analysis
- per-GPU utilization telemetry
- geographic placement or geolocation enrichment
- separate public network-summary or GPU-inventory endpoints outside the live `/v1/network/*` route set
