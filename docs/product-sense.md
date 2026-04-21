# Product Sense

## What We Are Building

Livepeer NaaP Analytics is a real-time analytics pipeline and public API for the
Livepeer AI Network. It ingests `network_events` and `streaming_events`,
materializes corrected and queryable state in ClickHouse, and serves that state
through a public REST API and operator dashboards.

## Core Goals

1. Real-time visibility with sub-second API responses on published serving models.
2. Network transparency through public, documented read endpoints.
3. Reliability insight that makes failure and attribution gaps visible rather than hidden in raw logs.
4. Economics visibility grounded in the payment and demand signals the system already ingests.
5. Stable operator and partner contracts that resist design drift.

## Success Criteria

- Current documented endpoints are served from stable `api_*` contracts.
- Runtime validation passes for correctness-sensitive serving outputs.
- Recovery and rebuild procedures are documented and repeatable.
- Product specs, OpenAPI, and active docs agree on the live surface.

## Non-Goals

- End-user UI beyond the operator dashboard set already in the repo
- Authentication or API keys at launch
- Multi-region deployment design
- Long-term historical storage in Kafka
- Acting as a media transcoding service or covering the legacy broadcaster/transcoder stack

## Non-Negotiable Principles

See [`design-docs/core-beliefs.md`](design-docs/core-beliefs.md) for the full
principles. The short version is:

1. Secure by default
2. Performance is real
3. No shortcuts without documented tradeoffs
4. Testing and docs are part of the feature
5. Simplicity first

## Architecture Direction

- ClickHouse owns physical ingest and storage.
- The resolver owns corrected current state and serving-store publication.
- dbt owns semantic `canonical_*` and `api_*` views.
- The Go API serves published read contracts, not ad hoc raw-table queries.
