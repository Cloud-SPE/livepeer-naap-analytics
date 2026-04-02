# Run Modes And Recovery

For the architecture diagrams, see [`../design-docs/system-visuals.md`](../design-docs/system-visuals.md).
For the generated physical schema inventory, see [`../generated/schema.md`](../generated/schema.md).
For per-service Compose responsibilities, see [`compose-services.md`](compose-services.md).

## Standard Deployment Summary

A standard deployment runs ClickHouse, the resolver, the API, and the Grafana/Prometheus stack continuously. Kafka is the event source, ClickHouse owns physical ingest/runtime state, the resolver owns freshness and publication into current/serving stores, `dbt` owns the semantic `canonical_*` and `api_*` views, and the API only reads published `api_*` relations.

That split matters because it keeps responsibilities clear:

- ClickHouse handles ingestion, storage, and materialized-view fanout.
- The resolver handles bounded repair, dirty partition ownership, and current-store publication.
- `dbt` publishes semantic read models after SQL changes or during scheduled warehouse refresh runs.
- The API stays read-only against serving contracts.

## Local Development

Start the runtime local stack:

```bash
cp .env.example .env
make up
```

Useful commands:

```bash
make logs
make up-tooling
make warehouse-run
make test-validation-clean
make bootstrap-extract
```

`make up` does not start the optional long-lived `warehouse` tooling container or any validation-only services. Those stay behind dedicated Compose profiles so the default runtime only includes the always-on system components plus a one-shot `warehouse-init` publication step.

## Portainer / infra2 Mode

The Portainer deployment keeps stack files in [`../../deploy`](../../deploy).
Use [`../../deploy/README.md`](../../deploy/README.md) for the general stack layout and [`../../deploy/infra2/README.md`](../../deploy/infra2/README.md) for the split-stack `infra2` deployment.

Use the warehouse stack in [`../../deploy/infra1/warehouse/stack.yml`](../../deploy/infra1/warehouse/stack.yml) when you need scheduled or one-shot `dbt` publication. It is part of the supported solution, but it is not the freshness engine. Resolver state publication still belongs to the resolver service.

## Resolver Modes

`auto`

- Standard always-on production mode.
- Bootstraps visible backlog, repairs dirty historical partitions, then tails continuously.

`bootstrap`

- Bounded backlog catch-up mode.
- Use after cold start or after long resolver downtime before switching back to `auto`.

`tail`

- Steady-state live mode only.
- Use when backlog is already clean and only new arrivals need publication.

`backfill`

- Explicit historical replay for a requested window.
- Best for operator-driven rebuilds after a schema, attribution, or publication fix.

`repair-window`

- Repair a specific broken or stale window.
- Best when late raw arrivals or a failed run left a known gap.

`verify`

- Read-only parity/verification pass.
- Use after backfill or repair to confirm the expected state is published.

Examples:

```bash
make resolver-bootstrap FROM=2026-03-01T00:00:00Z TO=2026-03-02T00:00:00Z ORG=cloudspe
make resolver-backfill FROM=2026-03-01T00:00:00Z TO=2026-03-07T00:00:00Z ORG=daydream
make resolver-repair-window FROM=2026-03-04T12:00:00Z TO=2026-03-04T14:00:00Z ORG=cloudspe
make parity-verify FROM=2026-03-04T12:00:00Z TO=2026-03-04T14:00:00Z ORG=cloudspe
```

## dbt Publication Modes

Use local one-shot publication:

```bash
make warehouse-run
make warehouse-compile
make warehouse-test
```

Use the deployment stack in [`../../deploy/infra1/warehouse/stack.yml`](../../deploy/infra1/warehouse/stack.yml) for scheduled or operator-invoked publication in `infra2`.

Run `dbt` publication when:

- semantic SQL under [`../../warehouse/models`](../../warehouse/models) changes
- a fresh ClickHouse volume is bootstrapped
- you need to republish semantic views after a warehouse image or dependency change

## Fresh Bootstrap And Baseline Extraction

Generate the v1 bootstrap baseline from a clean validation database:

```bash
docker compose --profile validation up -d validation-clickhouse
docker compose --profile validation run --rm warehouse-validation
make bootstrap-extract
```

The extracted bootstrap is written to [`../../infra/clickhouse/bootstrap/v1.sql`](../../infra/clickhouse/bootstrap/v1.sql), and the corresponding inventory doc is written to [`../generated/schema.md`](../generated/schema.md).

## Failure Recovery

### Stalled Resolver Runs

Inspect resolver logs:

```bash
make resolver-logs
```

Inspect recent run state:

```sql
SELECT run_id, mode, status, owner_id, org, started_at, completed_at, last_error_summary
FROM naap.resolver_runs
ORDER BY started_at DESC
LIMIT 20;
```

If a run stalled and no active resolver still owns the work, restart the resolver and rerun the intended mode.

### Orphaned Window Claims

Inspect active claims:

```sql
SELECT window_key, claim_owner, lease_expires_at, released_at, updated_at
FROM naap.resolver_window_claims FINAL
WHERE released_at IS NULL
ORDER BY updated_at DESC;
```

Claims with an expired `lease_expires_at` are safe for a new resolver run to reclaim. If the owning process is gone, restarting the resolver is usually enough because the new run can reacquire expired claims.

### Dirty Partitions Blocking Progress

Inspect the dirty queue:

```sql
SELECT org, event_date, claim_owner, lease_expires_at, attempt_count, updated_at
FROM naap.resolver_dirty_partitions FINAL
ORDER BY updated_at DESC;
```

If partitions remain dirty after the quiet period:

1. confirm accepted raw data is still arriving
2. check resolver logs for repeated claim or publish errors
3. run `make resolver-repair-window` for the affected org/date window

### Warehouse Publication Drift

If semantic views are stale or missing after a fresh volume or SQL change:

```bash
make warehouse-compile
make warehouse-run
make warehouse-test
```

Then rerun the relevant validation or parity pass.

### Full Rebuild

Use this when the volume is disposable or the safest path is a clean rebuild:

1. recreate the ClickHouse volume
2. start ClickHouse with the extracted bootstrap
3. republish semantic views with `dbt`
4. run resolver bootstrap or bounded backfill
5. verify with validation and parity checks

## Validation And Post-Recovery Checks

```bash
make test-validation-clean
cd api && GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache go test ./...
```

Recommended SQL spot checks:

```sql
SELECT count() FROM naap.canonical_session_current;
SELECT count() FROM naap.api_network_demand_by_org;
SELECT count() FROM naap.api_active_stream_state WHERE last_seen > now() - INTERVAL 120 SECOND;
```
