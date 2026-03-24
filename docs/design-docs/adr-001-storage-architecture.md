# ADR-001: Storage Architecture

**Status:** Accepted
**Date:** 2026-03-24
**Deciders:** Project team

---

## Context

The analytics system must ingest two high-volume Kafka topics and serve sub-second API
responses to external developers and partners. The following constraints are fixed:

- `network_events`: ~32M events over 7-day Kafka retention (~4.6M events/day)
- `streaming_events`: ~250K events over 7-day Kafka retention (~35K events/day)
- Query patterns: time-series aggregations, snapshot lookups, leaderboard rankings
- Response latency target: sub-second for all API endpoints
- Retention: configurable TTL (default 1 year), indefinite until configured
- Storage: compressed at rest; compression must not affect API query latency

---

## Decision

**Use ClickHouse with its native Kafka table engine for direct ingestion.**

Architecture:

```
Kafka topics
    │
    ▼  (ClickHouse Kafka engine — 1–5s lag from emit to stored)
Raw event tables (ClickHouse, partitioned by day, LZ4 compressed)
    │
    ▼  (materialized views — instant, auto-aggregate on every insert)
Aggregate tables (sub-second query targets for the Go API)
    │
    ▼
Go API  →  External consumers
```

No separate cache layer at launch. ClickHouse materialized views serve sub-second
responses natively on billions of rows.

---

## Consequences

### Accepted
- End-to-end event propagation latency: 1–5 seconds (Kafka emit → API visible).
  This is not the same as API response time, which is < 100ms.
- Raw events are stored in ClickHouse (not just aggregates), which increases storage
  footprint. Mitigated by LZ4 compression on hot data and ZSTD on cold/archival data.
- Schema changes to raw events require a migration plan for ClickHouse tables.

### Rejected alternatives

| Alternative | Reason rejected |
|-------------|----------------|
| **Python pipeline → Postgres** | Postgres is not suited for time-series aggregation at this event volume. Sub-second queries would require heavy indexing and caching. |
| **Python pipeline → ClickHouse** | Python pipeline as an intermediary adds latency and a failure point. ClickHouse Kafka engine is simpler and more reliable for raw ingestion. |
| **Redis hot cache in front of ClickHouse** | Adds operational complexity with no measurable benefit at launch. ClickHouse materialized views are sufficient. Can be added later if query load demands it. |
| **Batch processing (daily/hourly jobs)** | Does not meet the sub-second freshness requirement for active stream counts and network state queries. |
| **TimescaleDB** | ClickHouse has significantly better compression ratios, faster aggregation queries, and native Kafka integration. TimescaleDB offers more familiar SQL semantics but does not match ClickHouse performance at this volume. |

---

## Storage configuration

| Layer | Table type | Compression | Retention |
|-------|-----------|-------------|---------|
| Raw events | `MergeTree` partitioned by `toYYYYMM(timestamp)` | LZ4 (default) | Configurable TTL, default 1 year |
| Aggregates | `AggregatingMergeTree` materialized views | LZ4 | Same as raw |
| Archival (>90 days) | Cold storage tier | ZSTD (better ratio) | Until TTL |

Retention TTL is set via a ClickHouse `TTL` clause per table and is configurable via
environment variable without code changes. See `infra/clickhouse/retention.sql`.

---

## Tradeoffs explicitly accepted

1. **Storage cost vs query simplicity**: Storing raw events costs more than storing only
   aggregates. Accepted because raw event access enables ad-hoc analysis and future
   requirements that we cannot anticipate now.

2. **1–5s propagation lag**: For "right now" queries (e.g., active stream count), data
   may be 1–5 seconds stale. Accepted because: (a) this is within user expectations for
   a metrics API; (b) adding a real-time push layer (WebSocket/SSE) is an additive change
   that does not require rearchitecting.

3. **ClickHouse operational complexity**: ClickHouse requires more operational knowledge
   than Postgres. Accepted because the performance requirements cannot be met with simpler
   alternatives at this event volume.
