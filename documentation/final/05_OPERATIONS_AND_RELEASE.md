# Operations and Release

## Deployment Order
1. Tables
2. Materialized views
3. Derived dimensions
4. Serving views
5. Optional projections
6. Health checks

## Health Checks
- Fact freshness lag
- Rollup lag
- Attribution coverage
- Cardinality anomalies
- Session/segment consistency checks

## Replay and Backfill
- Keep idempotent versioned facts.
- Replay by bounded windows and validate each chunk.
- Backfill rollups after fact backfill chunks.

## Release Readiness Checklist
- Lifecycle acceptance checks green.
- Rollup parity checks green.
- API view sanity checks green.
- Documentation updated.
- Cutover plan approved.
