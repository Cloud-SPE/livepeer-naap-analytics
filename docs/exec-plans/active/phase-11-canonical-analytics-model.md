# Plan: Phase 11 — Minimal Canonical Analytics Model

**Status:** active
**Started:** 2026-03-25
**Depends on:** Phase 10
**Blocks:** canonical API cutover, warehouse-driven rebuilds

## Goal

Introduce the smallest canonical analytics layer that fixes v3's known data
quality and replay issues without replacing the current ClickHouse-first
architecture.

Ownership boundary:

- ClickHouse: `raw -> typed`
- dbt in `warehouse/`: `typed -> facts -> serving`
- Go API: serving-view reader only

## Adopted Rule Set

`CANONICAL_RULE_REQUIREMENTS.md` is a conformance reference only. It is not a
warehouse blueprint. This phase adopts only the subset needed to fix identified
v3 defects:

- pipeline/model normalization
- deterministic session identity
- startup precedence and canonical lifecycle edges
- replay-safe rebuildable downstream facts
- historical attribution snapshots with safe failure states
- status-hour semantics with narrow tail filtering
- payment-to-session linkage with explicit unresolved handling
- serving outputs derived from canonical typed/fact layers
- fail-safe attribution visibility so `resolved`, `hardware_less`, `stale`,
  `ambiguous`, and `unresolved` remain explicit in serving outputs

## Implementation Shape

- ClickHouse typed tables:
  - `typed_stream_trace`
  - `typed_ai_stream_status`
  - `typed_ai_stream_events`
  - `typed_discovery_results`
  - `typed_stream_ingest_metrics`
  - `typed_network_capabilities`
  - `typed_payments`
- dbt facts:
  - `capability_snapshots`
  - `fact_workflow_sessions`
  - `fact_workflow_status_hours`
  - `fact_workflow_payment_links`
- dbt serving views:
  - latest orchestrator/network state
  - canonical status samples
  - stream/reliability/performance/payment/network serving views used by Go repo methods

## Guardrails

- No extra fact table unless it closes a named bug, replay risk, or validation gap.
- No segment fact family in this phase.
- No generic edge warehouse in this phase.
- No correctness-critical dependency on the Go enrichment worker.
- No business-semantic Go query may read raw accepted-event history directly from `accepted_raw_events`.

## Acceptance Gates

- replaying the same raw window does not inflate downstream serving outputs
- payment-linked demand and economics recompute consistently from canonical facts
- unresolved, ambiguous, stale, hardware-less, and unlinked states remain explicit
- API latency stays within the existing product budget after cutover
