# AGENTS.md

This file is the **table of contents** for this repository. It is short by design.
Do not add detailed rules here — add them to the appropriate `docs/` file and link here.

## What this project is

Livepeer NAAP Analytics — a ClickHouse-backed analytics API for the Livepeer AI Network.
Events flow from Kafka into ClickHouse via the Kafka Engine and are surfaced via a Go REST API.

See `docs/PRODUCT_SENSE.md` for product principles and goals.

## Repository map

```
api/        — Go REST API service
infra/      — Docker, Kafka, ClickHouse infrastructure and migrations
scripts/    — Developer and ops utilities
docs/       — System of record (start here for context)
```

## Architecture

Layered domain architecture enforced per component. Dependencies flow **forward only**:

```
Types → Config → Repo → Service → Runtime
```

Cross-cutting concerns (auth, telemetry, feature flags) enter through **Providers only**.
No layer may import from a layer ahead of it. This is enforced mechanically.

See `docs/DESIGN.md` for the full architecture map and enforcement model.
See `docs/design-docs/architecture.md` for per-component layer rules.

## Documentation index

| File | Purpose |
|------|---------|
| [`docs/DESIGN.md`](docs/DESIGN.md) | Architecture overview and layering rules |
| [`docs/PLANS.md`](docs/PLANS.md) | Active and completed plans index |
| [`docs/PRODUCT_SENSE.md`](docs/PRODUCT_SENSE.md) | Product principles and goals |
| [`docs/design-docs/index.md`](docs/design-docs/index.md) | All design documents with status |
| [`docs/design-docs/core-beliefs.md`](docs/design-docs/core-beliefs.md) | Agent-first operating principles |
| [`docs/design-docs/architecture.md`](docs/design-docs/architecture.md) | Detailed per-component architecture |
| [`docs/design-docs/system-visuals.md`](docs/design-docs/system-visuals.md) | Visual data-flow and deployment diagrams |
| [`docs/design-docs/data-validation-rules.md`](docs/design-docs/data-validation-rules.md) | Behavioral contract for all data validation rules (17 rules, 31 tests) |
| [`docs/operations/run-modes-and-recovery.md`](docs/operations/run-modes-and-recovery.md) | Supported runtime modes, recovery, and rebuild procedures |
| [`docs/operations/compose-services.md`](docs/operations/compose-services.md) | Docker Compose services, profiles, and runtime responsibilities |
| [`docs/operations/runtime-validation-and-performance.md`](docs/operations/runtime-validation-and-performance.md) | Standard runtime measurement checklist for performance, attribution, SLA, and rollup safety |
| [`docs/exec-plans/active/`](docs/exec-plans/active/) | In-progress execution plans |
| [`docs/exec-plans/completed/`](docs/exec-plans/completed/) | Historical execution plans |
| [`docs/exec-plans/tech-debt-tracker.md`](docs/exec-plans/tech-debt-tracker.md) | Known technical debt |
| [`docs/generated/schema.md`](docs/generated/schema.md) | Auto-generated schema reference |
| [`docs/product-specs/index.md`](docs/product-specs/index.md) | Feature and product specifications |

## Non-negotiable tenants

Five principles that govern every decision. Read them before writing any code.
Full detail in `docs/design-docs/core-beliefs.md`.

1. **Secure by default** — non-root containers, validated inputs, no secrets in code
2. **Performance is critical** — latency targets are real; regressions are bugs
3. **No shortcuts** — tradeoffs are deliberate and documented in `docs/exec-plans/`
4. **Testing and docs are baked in** — every interface is documented; every behaviour is tested
5. **Simplicity first** — add complexity only when unavoidable, and justify it

## Working in this repo

- **All knowledge lives in the repository.** If it is not here, it does not exist.
- **Plans are first-class artifacts.** Use `docs/exec-plans/` for complex work.
- **Enforce boundaries mechanically.** Do not bypass linters or structural tests.
- **Validate data at boundaries** using typed schemas (Pydantic for Python, typed structs for Go).
- **Do not probe data shapes speculatively** — rely on typed SDKs and generated schemas.
- **Prefer shared utilities** over hand-rolled helpers. Check `api/internal/` and `pipeline/src/` first.
- **Linter error messages include remediation instructions.** Read them before guessing a fix.

## Component quick-start

```bash
# Full stack
make up

# API only (without Docker)
make dev-api        # cd api && go run ./cmd/server

# Run all tests
make test

# Run linters
make lint
```

## CI and quality

- Linters are enforced in CI. Structural tests validate layer dependency directions.
- A recurring doc-gardening pass scans for stale documentation.
- See `docs/design-docs/architecture.md` for the full enforcement model.
