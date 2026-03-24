# Spec: R4 — Payments & Economics

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** PAY-001 through PAY-008
**Source events:** `create_new_payment`
**ADRs:** ADR-001, ADR-002

---

## Problem

Consumers need to understand the economics of the Livepeer network: how much has been
paid in ETH, the cost per stream and pipeline, revenue earned by orchestrators, and
pricing trends over time.

## Payment model

Livepeer uses **probabilistic micropayments**. Each `create_new_payment` event represents
a probabilistic ticket, not a confirmed on-chain transaction. The `faceValue` is the
ticket's face value in WEI. The expected value of a ticket is `faceValue × winProb`.

**Design decision:** Report `faceValue` totals (not expected value) in the API, because:
1. It is the value the orch *could* receive and is the standard Livepeer accounting unit.
2. `winProb` fluctuates and expected-value accounting would be misleading to consumers
   unfamiliar with the payment model.

Tradeoff: reported totals are higher than actual on-chain settlement. This is documented
in all API responses via a `payment_model` metadata field.

---

## Requirements

### PAY-001: Payment summary

`GET /v1/payments/summary`

**Query params:**
- `?start=` / `?end=` (default: last 24 hours)
- `?org=`
- `?pipeline=`
- `?window=today|yesterday|7d|30d`

**Response:**
```json
{
  "data": {
    "time_window": { "start": "...", "end": "..." },
    "payment_model": "probabilistic_face_value",
    "total_payments_wei": "125248320000000000",
    "total_payments_eth": "0.12524832",
    "payment_event_count": 52,
    "unique_gateways": 1,
    "unique_orchestrators": 17,
    "avg_face_value_wei": "2408621538461538",
    "by_org": {
      "daydream":  { "total_wei": "81712320000000000",  "count": 34 },
      "cloudspe":  { "total_wei": "43536000000000000",  "count": 18 }
    }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] PAY-001-a: `total_payments_wei` stored and returned as string (avoids float precision loss)
- [ ] PAY-001-b: `payment_model` field always present to contextualise the numbers
- [ ] PAY-001-c: `total_payments_eth` = `total_payments_wei` / 1e18, rounded to 8 decimal places

---

### PAY-002: Payment history (time-series)

`GET /v1/payments/history`

Time-bucketed payment volume for charting.

**Query params:**
- `?start=` / `?end=`
- `?granularity=5m|1h|1d`
- `?org=`

**Response:**
```json
{
  "data": {
    "granularity": "1h",
    "series": [
      {
        "timestamp": "2026-03-24T08:00:00Z",
        "total_wei": "28800000000000000",
        "event_count": 12,
        "unique_orchs": 5
      }
    ]
  }
}
```

**Acceptance criteria:**
- [ ] PAY-002-a: WEI values stored as `UInt64` in ClickHouse (not float)
- [ ] PAY-002-b: Time buckets are UTC-aligned

---

### PAY-003: Payments by pipeline

`GET /v1/payments/by-pipeline`

**Query params:**
- `?start=` / `?end=`
- `?org=`

**Response:**
```json
{
  "data": [
    {
      "pipeline": "streamdiffusion-sdxl",
      "total_wei": "28932240000000000",
      "total_eth": "0.02893224",
      "event_count": 12,
      "avg_price_wei_per_pixel": 1635.0,
      "payment_model": "probabilistic_face_value"
    }
  ]
}
```

**Note:** Pipeline is derived from `manifestID` field in payment events
(format: `<capability_id>_<pipeline_name>` or bare `<pipeline_name>`).
When the full pipeline name is not parseable from `manifestID`, the raw `manifestID`
is returned as the pipeline identifier.

**Acceptance criteria:**
- [ ] PAY-003-a: Pipeline derived from `manifestID` by stripping numeric prefix
- [ ] PAY-003-b: Unparseable `manifestID` values returned as-is, not dropped
- [ ] PAY-003-c: `avg_price_wei_per_pixel` derived from `price` field in payment events

---

### PAY-004: Revenue by orchestrator

`GET /v1/payments/by-orchestrator`

**Query params:**
- `?start=` / `?end=`
- `?org=`
- `?limit=50&cursor=<opaque>`

**Response:**
```json
{
  "data": [
    {
      "address": "0x...",
      "name": "speedybird.xyz",
      "total_received_wei": "14400000000000000",
      "total_received_eth": "0.0144",
      "payment_event_count": 6,
      "payment_model": "probabilistic_face_value"
    }
  ]
}
```

Sorted by `total_received_wei` descending by default.

**Acceptance criteria:**
- [ ] PAY-004-a: `address` matches `recipient` field in `create_new_payment` events (normalised to lowercase)
- [ ] PAY-004-b: Gateways (senders) are excluded from this endpoint

---

## Data sources and mapping

| API field | Source event | Source field path |
|-----------|-------------|------------------|
| Face value | `create_new_payment` | `data.faceValue` (parse: strip `" WEI"` suffix) |
| Win probability | `create_new_payment` | `data.winProb` (float string) |
| Recipient (orch) | `create_new_payment` | `data.recipient` |
| Sender (gateway) | `create_new_payment` | `data.sender` |
| Session ID | `create_new_payment` | `data.sessionID` |
| Request ID | `create_new_payment` | `data.requestID` |
| Pipeline | `create_new_payment` | `data.manifestID` (parse: strip `<N>_` prefix) |
| Price | `create_new_payment` | `data.price` (parse: strip `" wei/pixel"` suffix) |
| Orch URL | `create_new_payment` | `data.orchestrator` |

---

## Out of scope

- On-chain settlement data (would require indexing the Ethereum blockchain)
- Individual ticket-level data (only aggregates are exposed)
- Gateway spend analytics (gateways are identifiable but not the focus)
