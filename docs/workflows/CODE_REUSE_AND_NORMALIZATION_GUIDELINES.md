# Code Reuse and Normalization Guidelines

## Objective

Reduce drift and hidden semantic bugs by centralizing repeated normalization/fallback behavior into a small set of owned helpers.

## Canonical Helper Ownership

| Concern | Canonical helper | Notes |
|---|---|---|
| Blank checks + fallback | `com.livepeer.analytics.util.StringSemantics` | Use `isBlank`, `firstNonBlank`, `blankToNull` instead of local variants. |
| Orchestrator/wallet address normalization | `com.livepeer.analytics.util.AddressNormalizer` | Always use `normalizeOrEmpty` for hot/canonical addresses. |
| Lifecycle attribution method/confidence mapping | `com.livepeer.analytics.lifecycle.AttributionSemantics` | Session/segment/param-update facts must share the same mapping logic. |
| Source pointer + dimensions extraction for failure envelopes | `com.livepeer.analytics.quality.RejectedEventEnvelopeSupport` | `QualityGateProcessFunction` and `RejectedEventEnvelopeFactory` must stay aligned. |
| Sink row byte-size checks | `com.livepeer.analytics.sink.RowSizeUtil` | Shared UTF-8 size semantics for all sink guards. |

## Design Rules

1. Keep helper APIs narrow and deterministic.
2. Avoid adding generic cross-domain utility classes.
3. Put helper classes in the package that owns the semantics.
4. Add or update characterization tests before changing helper behavior.
5. When changing normalization semantics, update docs and run full integration assertions.

## Anti-Drift Guardrails

1. Do not reintroduce local `firstNonEmpty`, `normalizeAddress`, or copy/paste attribution mapping in operators/state machines.
2. Use refactor guard tests to fail builds when duplicated helper signatures are added.
3. Treat helper behavior changes as contract-level changes that require explicit review notes.
