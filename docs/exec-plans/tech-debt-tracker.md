# Technical Debt Tracker

Known technical debt, ordered by priority. Updated as debt is introduced or resolved.

| ID | Description | Priority | Introduced | Owner |
|----|-------------|----------|-----------|-------|
| TD-001 | `naap.events` queries in `performance.go` and `reliability.go` read raw events without `FINAL`. `ReplacingMergeTree` deduplication is background/lazy, so these queries can transiently over-count in the window after a crash-restart. Aggregate MV tables are not affected. Fix: add `FINAL` to each direct `naap.events` query. | low | Phase 3 | — |
| TD-002 | `agg_orch_reliability_hourly` and `agg_stream_state` have empty `orch_address` for events processed before `agg_orch_state` was fully backfilled. Once the `network_events` Kafka backfill completes, re-run the INSERT section of migration 011 to resolve attribution. | medium | Phase (fix) | — |

## Format

When adding debt:
1. Assign the next sequential ID (TD-001, TD-002, ...)
2. Describe the debt and the tradeoff that created it
3. Set priority: `high` / `medium` / `low`
4. Record the date introduced

When resolving debt:
1. Move the row to the **Resolved** section below with a resolution date

## Resolved

*(None yet.)*
