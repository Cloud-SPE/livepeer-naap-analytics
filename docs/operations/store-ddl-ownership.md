# Store-Table DDL Ownership

Every resolver-written `_store` MergeTree has a canonical DDL
declaration under `warehouse/ddl/stores/<table>.sql`. This is the
source of truth for the table's schema, sort key, partition scheme,
and TTL. The live ClickHouse schema and this file must agree.

The resolver owns the **data** in these tables (every INSERT is a
resolver run). This repo owns the **shape**. `warehouse/ddl/stores/`
is the authoritative declaration; `infra/clickhouse/migrations/` is
the historical record of how the shape has evolved and is applied to
fresh stacks at bootstrap.

## Why not a dbt materialization

dbt's `materialized='table'` DROPs and recreates the physical table
on every run. The resolver's in-flight data would be wiped.
`materialized='incremental'` has the same problem during schema
drift. A custom `managed_store` materialization that only emits
`CREATE TABLE IF NOT EXISTS` was considered; using plain SQL files
under `warehouse/ddl/stores/` is simpler and has no risk of dbt
ever owning the data.

## Layout

```
warehouse/ddl/stores/
  canonical_active_stream_state_latest_store.sql
  canonical_ai_batch_job_store.sql
  canonical_byoc_job_store.sql
  canonical_capability_offer_inventory_store.sql
  canonical_capability_pricing_inventory_store.sql
  canonical_capability_snapshots_store.sql
  canonical_payment_links_store.sql
  canonical_session_current_store.sql
  canonical_status_hours_store.sql
  canonical_status_samples_recent_store.sql
  canonical_streaming_demand_hourly_store.sql
  canonical_streaming_gpu_metrics_hourly_store.sql
  canonical_streaming_sla_hourly_store.sql
  canonical_streaming_sla_input_hourly_store.sql
  normalized_session_attribution_input_latest_store.sql
```

Each file is a single `CREATE TABLE IF NOT EXISTS naap.<name> (...)
ENGINE = ... PARTITION BY ... ORDER BY ... TTL ... SETTINGS ...`.

## Workflow — adding a new store table

1. Add the declaration file: `warehouse/ddl/stores/<name>.sql`
2. Add an `infra/clickhouse/migrations/<NNN>_create_<name>.sql` that
   the bootstrap / migration runner will apply on fresh stacks. The
   migration and the declaration must produce the same schema.
3. `make apply-store-ddl` to ensure the table exists locally.
4. `make lint-store-ddl` to verify the live schema matches the file.
5. If the resolver needs to INSERT into it, add the writer in
   `api/internal/resolver/repo.go`.

## Workflow — changing an existing store table

1. Write an `infra/clickhouse/migrations/<NNN>_alter_<name>.sql`
   with the explicit `ALTER TABLE` / `MODIFY COLUMN` statements.
2. Apply it via `make migrate-up` against every target stack.
3. Update `warehouse/ddl/stores/<name>.sql` to reflect the new shape.
4. `make lint-store-ddl` — must pass. If it fails with
   `TYPE_DRIFT` or `MISSING_IN_LIVE`, the migration and the
   declaration have drifted; fix before merging.

## Workflow — detecting unintended drift

`make lint-store-ddl` is part of `make lint-medallion` and runs in
the `medallion-lints` workflow on every PR. A drift appears as:

```
TYPE_DRIFT canonical_session_current_store.attribution_status: \
  declared="LowCardinality(String)" live="String"
```

Read the mismatch; decide whether the declaration or the live schema
is correct; write the migration (or declaration update) that resolves
it.

## Why not just use `ALTER TABLE` in-line?

The lint does not apply `ALTER TABLE` itself. Two reasons:

- An automated schema-rewrite of a live table is a bigger change than
  a lint is entitled to.
- Some ALTERs on a ReplacingMergeTree with resolver-written data are
  unsafe without an operator's explicit consent (e.g. dropping a
  column the resolver still writes to).

The lint reports; the operator writes the migration.
