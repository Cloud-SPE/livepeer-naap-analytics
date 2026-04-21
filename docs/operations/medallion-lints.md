# Medallion Lints

Five machine-checked rules that enforce the serving-layer contract
described in [`../design-docs/api-table-contract.md`](../design-docs/api-table-contract.md).
Every rule supports a known-violation allowlist so the project can
enforce discipline on new drift without demanding every legacy panel
and handler be rewritten in the same PR.

## One-liner

```bash
make lint-medallion
```

Runs all four rules. Output for each is `ALLOWLISTED / FORBIDDEN` with
phase references tracking to
[`../exec-plans/completed/serving-layer-v2.md`](../exec-plans/completed/serving-layer-v2.md).

## Rules

### 5. Store-DDL drift (Go)

**Target:** `warehouse/ddl/stores/*.sql`. **Rule:** every declared
`_store` table's live schema (columns + types) must match the
checked-in declaration. Catches silent drift between operator-applied
migrations and the committed source of truth.

**Implementation:** `api/cmd/store-ddl-lint/main.go`. Parses the
declared DDL with a paren-depth-aware tokeniser (so nested types like
`AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8)`
stay intact), queries `system.columns` for the live schema, and
reports `MISSING_IN_DECL`, `MISSING_IN_LIVE`, or `TYPE_DRIFT` per
mismatch.

**Run:** `make lint-store-ddl`

See [`store-ddl-ownership.md`](store-ddl-ownership.md) for the
workflow around adding / changing a store table.

### 1. Layer discipline (dbt)

**Target:** the `{{ ref() }}` graph. **Rule:**

```
staging    -> sources only
canonical  -> canonical, staging
api        -> canonical
operational -> canonical, operational
```

**Implementation:** `warehouse/tests/test_layer_discipline.sql` walks
`graph.nodes` in Jinja and emits a row per illegal edge.

**Run:** `make lint-dbt-layer-discipline`

### 2. Grafana serving-contract (Go)

**Target:** `infra/grafana/dashboards/naap-*.json`. **Rule:**

- No panel SQL may reference `raw_events`, `accepted_raw_events`,
  `stg_*`, `normalized_*`, `agg_*`, or `canonical_*`.
- Every panel should declare `meta.backing_table` pointing at a
  concrete `naap.api_*` relation (warn-only for now, becomes fail
  in Phase 7).

**Implementation:** `api/cmd/grafana-lint/main.go`. Allowlist:
`scripts/grafana-lint-allowlist.txt` keyed by
`<dashboard.json> / <panel title>`.

**Run:** `make lint-grafana`

### 3. Additive primitives (dbt)

**Target:** every `api_hourly_*` model. **Rule:** the declared
additive primitive columns must exist and be non-nullable on every
row, so downstream consumers can reaggregate over wider windows
without averaging ratios.

**Implementation:**
`warehouse/tests/test_api_hourly_additive_primitives.sql`. The
primitive list per model lives in the test itself — editing it is a
schema change and should land alongside the corresponding model
change.

**Run:** `make lint-dbt-additive-primitives`

### 4. Core-logic recalc (bash)

**Target:** `api/internal/service/`, `api/internal/repo/`,
`warehouse/models/api/`. **Rule:** definitional logic (scoring
formulas, classification branches, `capability_family` mapping,
semantics-version literals) must live in the resolver, not at the
API-serving boundary.

**Implementation:** `scripts/core-logic-lint.sh` + patterns in
`scripts/core-logic-signatures.txt`. Allowlist:
`scripts/core-logic-allowlist.txt` keyed by
`<repo-relative-path>:<line-number>`.

**Run:** `make lint-core-logic`

## Current Enforcement Model

- Layer discipline, additive primitives, and store-DDL drift are hard-fail checks.
- Grafana and core-logic lints still support allowlists for explicitly tracked migration debt.
- `make lint-medallion` is the canonical local entrypoint; CI runs the same rule set.

## Adding an allowlist entry

New allowlist entries require a phase reference that exists in the
plan. The idea is "this violation is known, tracked, and will go
away in Phase N." Arbitrary suppression is a code smell — if a
violation has no phase home, fix it now or file the phase first.

**Format:**

- **core-logic:** `<repo-relative-path>:<line-number>\t<phase-reference>`
- **grafana:** `<dashboard.json> / <panel title>\t<phase-reference>`

The separator is a literal tab character in both files.
