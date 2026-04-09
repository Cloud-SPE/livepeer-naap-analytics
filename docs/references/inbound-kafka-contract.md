# Inbound Kafka Contract

This document is the repo-authoritative inbound Kafka contract for the current
Go, ClickHouse, resolver, and dbt stack.

The current sources of truth are:

- [`../../infra/clickhouse/bootstrap/v1.sql`](../../infra/clickhouse/bootstrap/v1.sql)
- [`../design-docs/data-validation-rules.md`](../design-docs/data-validation-rules.md)
- [`../../infra/clickhouse/retention.sql`](../../infra/clickhouse/retention.sql)

For upstream producer-side context, see
[`kafka-queue-event-catalog.md`](kafka-queue-event-catalog.md).

## Accepted Event Families

Current ingest accepts these top-level Kafka event families into
`naap.accepted_raw_events`:

- `stream_trace`
- `ai_stream_status`
- `ai_stream_events`
- `stream_ingest_metrics`
- `network_capabilities`
- `discovery_results`
- `create_new_payment`

These accepted families are wired in the ClickHouse ingest materialized views in
[`../../infra/clickhouse/bootstrap/v1.sql`](../../infra/clickhouse/bootstrap/v1.sql).

## Accepted `stream_trace` Subtypes

Only these `stream_trace` subtypes are part of the current semantic contract:

- `gateway_receive_stream_request`
- `gateway_ingest_stream_closed`
- `gateway_send_first_ingest_segment`
- `gateway_receive_first_processed_segment`
- `gateway_receive_few_processed_segments`
- `gateway_receive_first_data_segment`
- `gateway_no_orchestrators_available`
- `orchestrator_swap`
- `runner_receive_first_ingest_segment`
- `runner_send_first_processed_segment`

## Ignored And Quarantined Events

Events outside the accepted contract are not promoted into semantic serving
tables. They are written to `naap.ignored_raw_events` with an `ignore_reason`.

Current ignored classes include:

- unsupported top-level event families
- unsupported `stream_trace` subtype values
- non-core `app_*` trace noise
- known scope-client trace noise
- malformed or invalid payloads

`ignored_raw_events` is the current quarantine and DLQ-equivalent store for
this repo. There is no separate Kafka quarantine topic in the active design.

## Semantic Destination Path

- accepted contract events land in `accepted_raw_events`
- ingest materialized views populate `normalized_*` tables
- the resolver publishes corrected current and serving state into `canonical_*_store` and `api_*_store`
- dbt publishes `canonical_*`, `api_base_*`, and `api_*` views
- unsupported or ignored contract misses stay queryable in `ignored_raw_events`

## Notes

- The consumer contract in this repo is narrower than the full upstream
  producer surface.
- The retained producer catalog is useful for understanding upstream payload
  shape, but it is secondary to the ClickHouse ingest rules in this repo.
