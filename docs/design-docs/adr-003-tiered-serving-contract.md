# ADR-003: Tiered Serving Contract

**Status:** accepted; amended by [`api-table-contract.md`](api-table-contract.md) (Phase 5 of serving-layer-v2 retired the `api_base_*` tier)
**Date:** 2026-03-27 (amended 2026-04-18)

## Summary

The analytics serving contract is intentionally tiered:

| Prefix | Semantic | Source of truth |
|--------|----------|----------------|
| `raw_*` | Accepted raw envelopes | Kafka ingest |
| `normalized_*` | Normalized event-family records | `raw_*` via materialized views |
| `canonical_*` | Authoritative corrected derivation source | `normalized_*` via resolver and dbt |
| `api_*` | Service-facing read models, organized into three families: `api_hourly_*`, `api_current_*`, `api_fact_*` | `canonical_*` only |
| `operational_*` | Low-latency live-ops tables | `raw_*` / `normalized_*` / curated read-only canonical inputs |

Consumers must derive truth from `canonical_*`, not from `api_*` or `operational_*`.

The previously documented `api_base_*` intermediate tier was retired in Phase 5
of the serving-layer-v2 refactor. Intermediate computation now lives in the
resolver (stateful) or in MV definitions (stateless); see
[`api-table-contract.md`](api-table-contract.md) for the three-families shape.

## Enforcement

The rule is machine-checked by `TestTierContract_NonAPIModelsDoNotDeriveTruthFromAPIModels` in `api/internal/validation/contract_static_test.go`.

## Rollup Rule

Rollups must never rely on unsafe aggregate-of-aggregate math.

- Sum additive counters directly.
- Recompute ratios from additive numerators and denominators.
- Recompute means from additive sums and counts.
- Merge explicit aggregate state for percentiles and distribution metrics.
- Do not average averages, ratios, or scalar percentiles.

## Source Of Truth

The maintained design references now live in:

- [architecture.md](architecture.md)
- [data-validation-rules.md](data-validation-rules.md)
- [../design.md](../design.md)
