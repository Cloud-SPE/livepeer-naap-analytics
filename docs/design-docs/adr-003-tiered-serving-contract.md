# ADR-003: Tiered Serving Contract

## Status

Current

## Summary

The analytics serving contract is intentionally tiered:

- `raw_*` captures accepted raw envelopes
- `normalized_*` captures family-specific normalized records
- `canonical_*` captures authoritative corrected facts and latest-state outputs
- `api_base_*` captures internal semantic helpers used to publish API models
- `api_*` captures public and dashboard read models only

Consumers must derive truth from `canonical_*`, not from `api_base_*` or `api_*`.

## Rollup rule

Rollups must never rely on unsafe aggregate-of-aggregate math.

- Sum additive counters directly.
- Recompute ratios from additive numerators and denominators.
- Recompute means from additive sums and counts.
- Merge explicit aggregate state for percentiles and distribution metrics.
- Do not average averages, ratios, or scalar percentiles.

## Source of truth

The maintained design references now live in:

- [architecture.md](architecture.md)
- [data-validation-rules.md](data-validation-rules.md)
- [../design.md](../design.md)

This ADR remains as a stable repository reference for the tier contract named in
older docs and reviews.
