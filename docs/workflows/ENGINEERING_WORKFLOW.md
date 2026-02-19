# Engineering Workflow (Human + Agent)

## Objective

Ship quickly without contract drift by making design decisions legible and mechanically validated.

## Change Flow

1. Read relevant canonical docs in `docs/`.
2. Make the smallest coherent code/doc change.
3. Run the narrowest useful tests first, then broader validation.
4. Update docs in the same PR for any contract or behavior change.
5. Keep changes reviewable: small PRs, explicit assumptions, rollback notes when risky.

## Required Update Matrix

| If you change... | You must also update... |
|---|---|
| ClickHouse schema | Flink row mappers + parser/model tests + `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |
| Lifecycle classification or edge semantics | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` + validation SQL + canonical data docs |
| API view grain/fields | metric contract docs + integration SQL assertions for API readiness |
| Replay behavior | `docs/operations/RUNBOOKS_AND_RELEASE.md` + `docs/operations/REPLAY_RUNBOOK.md` |
| Test harness scripts | `docs/quality/TESTING_AND_VALIDATION.md` |

## PR Quality Bar

- Behavior is test-backed, not only explained.
- Metric/contract changes are versioned or explicitly backward compatible.
- No silent semantic changes in SQL views.
- Docs are updated where a new contributor/agent would look first.

## Preferred Iteration Pattern

- Depth-first decomposition:
  - implement missing primitive capability,
  - validate mechanically,
  - compose into larger behavior.
- Keep active plans and rationale in-repo, not in chat-only context.

## Non-Goals

- Do not optimize for perfect one-shot docs.
- Do not delete legacy docs before migration map coverage is complete.
