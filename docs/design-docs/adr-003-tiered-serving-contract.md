# ADR-003: Tiered Serving Contract

**Status:** accepted  
**Date:** 2026-03-27

## Context

As the analytics warehouse grew beyond a single `canonical_*` layer, we needed explicit rules about which tiers can derive truth from which other tiers. Without a formal contract, downstream teams could inadvertently read from API-facing read models (`api_*`) in warehouse models, creating circular dependencies that make reprocessing and historical backfill impossible.

The two failure modes we wanted to prevent:

1. **Downstream models sourcing from `api_*`** — those tables are ephemeral service-read models optimized for latency; they may be dropped, reshaped, or regenerated at any time.
2. **`api_*` or `operational_*` feeding back into `canonical_*`** — this inverts the derivation order and prevents clean recomputation from raw events.

## Decision

We enforce the following one-way derivation hierarchy across all tiers:

```
raw_* → normalized_* → canonical_* → api_*
raw_* / normalized_* → operational_*
```

**Tier semantics:**

| Prefix | Semantic | Source of truth |
|--------|----------|----------------|
| `raw_*` | Accepted raw envelopes | Kafka ingest |
| `normalized_*` | Normalized event-family records | raw_* via materialized views |
| `canonical_*` | Authoritative corrected derivation source | normalized_* via dbt |
| `operational_*` | Low-latency live-ops tables | raw_* / normalized_* only |
| `api_*` | Service-facing read models | canonical_* only |

**Allowed flows:**

- `canonical_*` → `api_*`
- `normalized_*` → `canonical_*`
- `raw_*` → `normalized_*`
- `raw_*` or `normalized_*` → `operational_*`
- `canonical_*` → `operational_*` (read-only; operational tables must not feed canonical back)

**Forbidden flows:**

- `api_*` → `canonical_*` (or any non-api tier)
- `operational_*` → `canonical_*`
- Any warehouse model (staging, canonical, facts, serving) `ref()`-ing or `FROM`-ing an `api_*` table

**Future consumer surfaces** (`dashboard_*`, `export_*`, `partner_*`, `feature_*`) must derive exclusively from `canonical_*`.

## Enforcement

The rule is machine-checked by `TestTierContract_NonAPIModelsDoNotDeriveTruthFromAPIModels` in `api/internal/validation/contract_static_test.go`. The test scans all `.sql` files under `warehouse/models/{canonical,facts,refresh,serving,staging}` and fails if any file references `ref('api_*')` or `FROM naap.api_*` (with a narrow exception for serving-layer `_store` tables that legitimately back-fill from an api surface).

## Consequences

- Warehouse models can always be recomputed from `normalized_*` without touching the API layer.
- API read models can be dropped and regenerated without affecting canonical truth.
- New event families (e.g., AI batch, BYOC) must follow the same path: migration creates `normalized_*` tables, dbt populates `canonical_*`, handlers query `canonical_*` directly or via `api_*` views.
- The contract is intentionally narrow: it does not restrict non-prefixed infrastructure tables (`accepted_raw_events`, `kafka_*`, `resolver_*`, `agg_*`), which are managed outside dbt.
