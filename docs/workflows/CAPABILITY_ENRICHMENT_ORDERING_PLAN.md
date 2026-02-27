# Capability Enrichment Ordering Plan

## Objective

Reduce attribution/key gaps caused by out-of-order `network_capabilities` vs session/status events while keeping medallion contracts clear and testable.

## Scope

- Session and status attribution paths that depend on capability snapshots.
- Downstream rollup/view parity (`agg_stream_performance_1m` vs `v_api_gpu_metrics` and related outputs).
- Fixture export behavior for scenario tests.

## Current Problem

- ClickHouse materialized views enrich at insert time only.
- If `ai_stream_status` rows arrive before matching `network_capabilities`, derived status facts can persist empty attribution keys.
- Gold/API views that apply fallback logic can diverge from rollups that do not.

## Target End State

1. Attribution correctness is deterministic under late/out-of-order delivery.
2. Rollups and serving views share one consistent key contract.
3. Test packs expose ordering defects instead of masking them.
4. Notebook stays diagnostic-focused and does not become a second implementation.

## Phased Plan

## Phase 1: Guardrails and Fixture Controls

### Deliverables

- Keep strict parity checks in integration SQL:
  - fail when both sides have rows but overlap is zero.
  - emit diagnostics for `rollup_rows`, `view_rows`, `joined_rows`, and key completeness.
- Fixture exporter supports both modes:
  - default unbiased mode (reflect replay reality),
  - optional `--capability-precede-session` mode (contract-focused scenarios).

### Validation

- `tests/integration/sql/assertions_pipeline.sql` fails on non-overlap parity regressions.
- Scenario export produces reproducible manifests in either mode.

## Phase 2: Canonical Enrichment Ownership

### Deliverables

- Move correctness-critical status enrichment ownership to Flink for `fact_stream_status_samples`.
- Use broadcast capability state and deterministic selection semantics already used by lifecycle operators.
- Emit enriched status facts directly from Flink.

### Validation

- Flink tests cover:
  - capability-before-status,
  - status-before-capability,
  - model compatibility selection,
  - wallet/canonical address mapping behavior.

## Phase 3: Late Capability Correction Path

### Deliverables

- Add bounded correction logic in Flink:
  - keep recent unmatched status samples in keyed state.
  - when late capability arrives, emit higher-version correction rows.
- Make sink table upsert-safe (`ReplacingMergeTree(version)`).

### Validation

- Replay with induced out-of-order input converges to same attributed output as in-order replay.
- Empty-key rates fall below agreed thresholds.

## Phase 4: Optional Trace Alignment

### Deliverables

- Apply same ownership model to `fact_stream_trace_edges` if needed.
- Remove duplicated enrichment semantics between Flink and ClickHouse for trace context.

### Validation

- Swap and lifecycle evidence checks remain stable across replay windows.

## Test Pack Impacts

- Keep assertion logic as source of truth for contracts.
- Add explicit checks for:
  - empty key rates in silver/rollup tables,
  - parity overlap health,
  - attributable vs unattributed session proportions.
- Add at least one scenario that intentionally stresses ordering.

## Notebook Impacts

- Keep notebook focused on triage:
  - show status flags and failure modes,
  - show top offending keys/sessions,
  - link to assertion queries for authoritative pass/fail.
- Do not embed heavy recompute logic that can drift from production SQL.

## Operational Impacts

- Add cutover/backfill runbook steps for status fact migration.
- During migration, ensure dual-write ambiguity is prevented (single authoritative writer).

## Acceptance Criteria

1. `gpu_view_matches_rollup` passes with non-zero overlap in standard fixture runs.
2. Ordering stress scenario either passes correction expectations or fails with clear diagnostics.
3. Notebook highlights attribution/parity issues without requiring deep SQL debugging.
4. Contract docs and tests are updated in the same change set.
