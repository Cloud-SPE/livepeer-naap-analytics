# Phase 11 — Deferred Areas

This document records the areas intentionally not addressed by the minimal
canonical analytics implementation. It exists to keep scope bounded while
preserving a clear record of what remains out of scope and why.

Fail-safe attribution visibility is not deferred. Sessions and orchestrators
must remain visible in serving outputs even when attribution is
`hardware_less`, `stale`, `ambiguous`, or `unresolved`.

## Deferred By Design

### Segment-Fact Modeling

Not addressed:

- a dedicated `fact_workflow_session_segments`
- generic orchestrator-change segment timelines
- segment-level serving APIs

Why deferred:

- the current named defects can be fixed with session facts and status-hour facts
- segment modeling adds table count, validation burden, and attribution edge
  complexity without being required for the initial cutover

Trigger to promote later:

- a concrete metric or API requires pre/post-swap segment truth that cannot be
  computed safely from session-level aggregates

### Generic Edge Warehouse

Not addressed:

- a full `fact_stream_trace_edges`
- a broad decision-spine framework for every normalization and attribution step

Why deferred:

- the current design goal is a small canonical layer, not a generalized
  warehouse architecture
- lifecycle semantics can stay modular inside typed models and dbt macros

Trigger to promote later:

- repeated debugging or validation work shows that macro-level lineage is not
  sufficient for explainability

### Full Excused-Error Taxonomy

Not addressed:

- an exhaustive standalone implementation of every inferred excused-error rule

Why deferred:

- startup precedence and explicit no-orchestrator semantics fix the highest-risk
  correctness issues first
- wider error taxonomy can be introduced later once backed by stronger test
  evidence and product need

Trigger to promote later:

- recurring false positive startup-failure classifications from known safe error
  messages

### Prompt-To-First-Frame Metrics

Not addressed:

- dedicated prompt-to-first-frame facts or serving fields beyond current
  available evidence

Why deferred:

- current raw data does not provide a strong enough canonical signal to model it
  without guesswork

Trigger to promote later:

- stable raw edges arrive that unambiguously anchor prompt receipt and first
  rendered output

### Settlement Accounting

Not addressed:

- expected-value payment accounting
- on-chain settlement reconciliation
- financial ledger semantics beyond probabilistic face-value reporting

Why deferred:

- current product surfaces use probabilistic face-value accounting
- settlement modeling would materially expand scope without fixing the present
  analytics drift issues

Trigger to promote later:

- product requirements expand from network payment telemetry to accounting or
  reconciliation workflows

### Broad Warehouse Expansion

Not addressed:

- additional fact families not tied to a named v3 defect
- parallel legacy and canonical serving stacks kept long-term

Why deferred:

- this phase is about removing semantic sprawl, not shifting it into more
  tables and jobs

Trigger to promote later:

- a new requirement cannot be met safely within the existing typed/fact/serving
  split and has a documented acceptance test to justify the expansion
