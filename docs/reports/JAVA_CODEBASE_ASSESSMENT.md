> **Status:** Historical report (point-in-time evidence). Canonical guidance is in `docs/`; mapping is in `docs/references/DOC_INVENTORY_AND_MIGRATION_MAP.md`.

# Java Codebase Assessment

Date: 2026-02-18
Scope: `flink-jobs/src/main/java`, `flink-jobs/src/test/java`

## Snapshot

- Main Java files: 49
- Test Java files: 14
- Classes/interfaces/enums: 75
- `@Test` methods: 51
- Main-source Javadoc blocks (`/**`): 44

## Overall Status

The codebase has a solid package split (pipeline, lifecycle, parse, quality, sink, capability) and strong deterministic state-machine coverage. The biggest maintainability risk is concentration of orchestration and mapping logic into a few large files.

## Findings

1. High complexity hotspot in pipeline assembly
- File: `flink-jobs/src/main/java/com/livepeer/analytics/pipeline/StreamingEventsToClickHouse.java`
- Risk: This file is the orchestration center for parsing, lifecycle derivation, side outputs, and sinks. At current size, small changes can introduce regressions and make reviews slower.

2. High complexity hotspot in sink row mapping
- File: `flink-jobs/src/main/java/com/livepeer/analytics/sink/ClickHouseRowMappers.java`
- Risk: Large static mapping surface increases schema-drift risk and makes mapper evolution harder when adding facts/views.

3. Event payload model is flat and mutable
- File: `flink-jobs/src/main/java/com/livepeer/analytics/model/EventPayloads.java`
- Risk: Public mutable fields are fast, but reduce local type safety and make accidental partial/invalid objects easier.

4. Test coverage is good for core transforms, thinner for end-to-end behavior
- Strong: state machines, parser behavior, schema sync, dedup/quality gates.
- Gap: fewer integration-style tests for full pipeline assembly behavior across streams (especially new API-focused KPI paths).

5. Javadocs are present but uneven for API-facing semantics
- Core classes include useful docs.
- Gaps remain on semantic contracts for derived fields (attribution confidence semantics, latency edge definitions, nullability guarantees for API-facing facts).

## Recommended Refactor Plan

1. Split pipeline assembly into modules
- Add focused builders/helpers:
  - `ParsingTopologyBuilder`
  - `LifecycleTopologyBuilder`
  - `SinkTopologyBuilder`
- Keep `StreamingEventsToClickHouse` as a thin composition root.

2. Split row mappers by domain
- Replace monolithic `ClickHouseRowMappers` with:
  - `RawEventRowMappers`
  - `LifecycleFactRowMappers`
  - `CapabilityRowMappers`
- Keep one compatibility facade if needed.

3. Harden payload contracts incrementally
- Keep current POJO pattern for Flink compatibility.
- Add factory methods for derived facts (latency/session/segment) to centralize invariants.
- Add nullability/units Javadocs on all API-facing derived fields.

4. Add integration test layer for API-supporting facts
- Add tests that validate:
  - latency derivation presence/absence rules,
  - orchestrator/gpu/model attribution fallback behavior,
  - session served/unserved demand projection correctness.
- Reuse SQL assertions already added under `tests/integration/sql`.

5. Add Javadocs where they matter most
- Prioritize:
  - `WorkflowLatencyDerivation`
  - `WorkflowSession*Accumulator/StateMachine`
  - API-facing fact payload fields in `EventPayloads`
  - `ClickHouseRowMappers` mapping assumptions (units and nullability)

## Test Sufficiency Verdict

- Unit-level sufficiency: Moderate to strong.
- Contract-level sufficiency (schema and parser): Strong.
- End-to-end behavior sufficiency for API-readiness: Moderate (needs targeted integration tests as above).

## Immediate Next Steps

1. Refactor only the two hotspots (`StreamingEventsToClickHouse`, `ClickHouseRowMappers`) without changing behavior.
2. Add integration tests for latency + demand attribution invariants.
3. Add semantic Javadocs for API-facing KPI fields and attribution fields.
