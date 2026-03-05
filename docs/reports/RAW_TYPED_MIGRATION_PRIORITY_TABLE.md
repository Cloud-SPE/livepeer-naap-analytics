# Raw/Typed Migration Priority Table

Generated: 2026-03-02

| Priority | List I suggest | How to fix | Why migrate before making changes |
|---|---|---|---|
| P0 | Legacy typed join block in schema init ([`configs/clickhouse-init/01-schema.sql`](../../configs/clickhouse-init/01-schema.sql), legacy jitter MV/backfill/ad-hoc query block near end of file) | Remove this block from production schema init and rely on fact/agg/view serving paths (`fact_*`, `agg_*`, `v_api_*`). Keep any diagnostics in docs-only query packs instead of bootstrap SQL. | This block bypasses the silver/gold contract and can silently reintroduce old semantics during schema bootstrap or replays. |
| P1 | Ingest projection MV still typed-based ([`configs/clickhouse-init/01-schema.sql`](../../configs/clickhouse-init/01-schema.sql), `mv_stream_ingest_metrics_to_fact_stream_ingest_samples`) | Keep as transitional exception for now, but explicitly isolate and document it as the only typed-to-fact ingest projection path. Add migration note to replace with Flink-emitted ingest fact when implementation is ready. | Prevents accidental expansion of typed-to-fact patterns to status/trace and keeps architecture boundaries explicit while migration is in progress. |
| P1 | Capability dimension MVs from typed capability tables ([`configs/clickhouse-init/01-schema.sql`](../../configs/clickhouse-init/01-schema.sql), `mv_network_capabilities*_to_dim_*`) | Keep as transitional dimension-projection path for now; isolate/document as dimension-only exception and prepare follow-up to emit dimension rows directly from Flink when ready. | Without boundary hardening, new feature work may continue joining typed capability tables directly in serving logic, causing drift and inconsistent semantics. |
