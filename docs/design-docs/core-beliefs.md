# Core Beliefs

Foundational principles for this repository. These are non-negotiable.
When in doubt, consult this document before making a tradeoff.

## Project tenants

These five tenants govern every decision in this codebase.

### T1. Secure by default
Security is not a feature added later — it is a constraint from the start.
- Non-root containers, minimal images
- No secrets in code or logs
- Validate all inputs at system boundaries
- Least-privilege principle for all services

### T2. Performance is critical
Correctness comes first; then performance. But performance requirements are real and enforced.
- P99 latency targets are documented in `docs/product-sense.md`
- Benchmarks are tracked; regressions are treated as bugs
- Algorithm and data structure choices are justified in comments or design docs

### T3. Deliberate decisions, no shortcuts
No quickfixes, hacks, or corner-cutting. When a tradeoff must be made:
- Document the tradeoff in a comment, design doc, or exec-plan
- Record the decision in `docs/exec-plans/`
- Add to `docs/exec-plans/tech-debt-tracker.md` if debt is incurred

### T4. Documentation and testing are baked in
- Every public interface has documentation
- Every behaviour has a test
- Tests run fast and in CI — no test is optional

### T5. Simplicity is a core value
Prefer the simplest solution that meets the requirements.
- Complexity that cannot be avoided must be explained and justified (see T3)
- No abstractions for hypothetical future requirements
- No dependencies added without a clear reason

---

## Agent-first operating principles

## 1. The repository is the system of record

Everything the agent needs to do its job must live here.
Slack discussions, Google Docs, and tacit knowledge are **invisible to agents**.
When a decision is made, encode it into this repository as markdown, schema, or code.

## 2. Give agents a map, not a manual

`AGENTS.md` is a table of contents, not an encyclopedia.
Detailed rules live in their relevant `docs/` files.
An agent starting a task reads `AGENTS.md`, finds the right doc, and reads that.
Do not overload `AGENTS.md` — too much guidance becomes non-guidance.

## 3. Enforce invariants mechanically

Documentation alone does not keep a codebase coherent.
Linters, structural tests, and CI jobs enforce the rules so agents cannot accidentally
violate them. Error messages from linters must include remediation instructions.

## 4. Validate at boundaries, trust internally

Data entering the system (Kafka messages, HTTP requests) must be validated against
typed schemas immediately. Inside the system, trust the types.
Never probe data shapes speculatively — if the schema is unknown, define it first.

## 5. Boring technology is a feature

Composable, stable APIs with good representation in training data are easier for agents
to reason about correctly. Avoid opaque libraries when a small, well-typed
implementation is tractable.

## 6. Plans are artifacts, not intentions

For any non-trivial change, a plan exists in `docs/exec-plans/active/`.
Decisions are logged. Progress is tracked. Completed plans are moved, not deleted.
This lets future agents (and humans) understand why things are the way they are.

## 7. Humans steer, agents execute

Engineers define goals, review outcomes, and encode feedback as tooling or documentation.
When an agent struggles, the fix is almost never "try harder" — it is "what capability
or context is missing, and how do we make it legible?"
