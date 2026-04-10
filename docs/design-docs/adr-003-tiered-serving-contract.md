# ADR-003: Tiered Serving Contract

**Status:** accepted  
**Date:** 2026-03-27

## Summary

The analytics serving contract is intentionally tiered:

| Prefix | Semantic | Source of truth |
|--------|----------|----------------|
| `raw_*` | Accepted raw envelopes | Kafka ingest |
| `normalized_*` | Normalized event-family records | `raw_*` via materialized views |
| `canonical_*` | Authoritative corrected derivation source | `normalized_*` via resolver and dbt |
| `api_base_*` | Internal semantic helpers used to publish API models | `canonical_*` only |
| `api_*` | Service-facing read models | `canonical_*` and `api_base_*` only |
| `operational_*` | Low-latency live-ops tables | `raw_*` / `normalized_*` / curated read-only canonical inputs |

Consumers must derive truth from `canonical_*`, not from `api_base_*`, `api_*`, or `operational_*`.

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
