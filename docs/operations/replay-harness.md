# Replay Harness

The replay harness drives a pinned raw-event fixture through every layer of
the medallion pipeline (raw → normalized → canonical → api) and records a
deterministic `artifact_checksum` rollup per table. A second run over the
same fixture must produce byte-identical rollups; any divergence localises
to the first layer whose rollup changed.

The harness is the verification substrate for [`serving-layer-v2`](../exec-plans/active/serving-layer-v2.md).
Every phase of that rewrite has an exit criterion of the form
`make replay-phase-N green`.

## Scope

**PR 1:** raw → normalized. The harness loads the fixture into
`naap.accepted_raw_events`, waits for the cascading MVs to settle, and
checksums every `normalized_*` table.

**PR 2:** resolver clock-freeze — `RunRequest.Now` is pinned so every
row-level timestamp the resolver writes (refreshed_at, materialized_at,
decision_id-embedded nonce) is deterministic.

**PR 3:** canonical layer. When `--layers` includes `canonical` the
harness also truncates `resolver_*` bookkeeping + `canonical_*_store`,
invokes the resolver in-process with `RunRequest.Mode = ModeBackfill` and
a pinned Now, and checksums the 17 canonical tables the resolver writes.

**PR 4 (current):** API layer. When `--layers` includes `api` the
harness invokes `dbt run --select api api_base` via the compose-based
`warehouse` service (view refresh; a no-op for row content but catches
schema-drift vs. the branch's view definitions), then checksums all 22
api/api_base view outputs. AggregateFunction columns (cohort-state
percentile sketches on `api_base_sla_quality_cohort_daily_state`) are
excluded from the tuple since sipHash64 cannot hash the opaque binary
state; scalar sibling columns preserve the signal.

### Why HTTP, not the native protocol

The loader uses ClickHouse's HTTP interface (`FORMAT JSONEachRow`) rather
than the native-protocol batch API, because each INSERT into
`accepted_raw_events` triggers a cascade of 16 materialized views.
Native batches stream client→server in lockstep — the client can't get
ahead of server MV processing, and the server's default 300s receive
timeout fires long before a large batch finishes. One streaming HTTP
POST keeps the whole pipeline on the server side, back-pressure and
all. Expect CH to peg 1–3 cores and hold ~5 GB RSS during a full load.

**Later PRs:** smaller dev fixtures for fast iteration (see performance
note below), lint scripts (`assert_layer_discipline`, `grafana-lint`,
core-logic recalc grep), dbt store-table ownership move (Phase 0
closeout), and CI workflow wiring.

## Repository layout

```text
scripts/fetch-golden-fixture.sh         fixture fetcher (bash + curl + zstd)
tests/fixtures/
  raw_events_golden.manifest.json       committed manifest (window, row count, SHA)
  raw_events_golden.ndjson.zst          fetched locally; git-ignored
api/cmd/replay/main.go                  CLI entry point
api/internal/replay/                    harness package
  doc.go, layers.go, manifest.go,
  checksum.go, loader.go, pipeline.go, diff.go
target/replay/                          run artefacts; git-ignored
  latest.json                           most recent report
  20260417-083015-<fixture>.json        per-run timestamped report
  first.json                            written by make replay-verify
```

### Prerequisites

- `make up` (local ClickHouse + Kafka + resolver)
- **Stop the resolver** before running the harness: `docker compose stop resolver`.
  The resolver writes to `canonical_*` on a schedule; if it's running
  during a replay it races with the checksum phase. The harness will
  grow an explicit guard for this in a later PR (tracked in the
  Phase 0 → Phase 1 handoff).
- 12 GB free RAM (ClickHouse `max_memory_usage` is 12 GB; peaks at ~7 GB
  during a full fixture load, and `FINAL` on `accepted_raw_events`
  peaks around 5 GB).
- `CLICKHOUSE_ADMIN_PASSWORD` set in `.env` (the writer user lacks
  `TRUNCATE` privileges).

### Kafka ingest pause

By default the harness **detaches the four ingest materialized views**
(`mv_ingest_{network,streaming}_events_{accepted,ignored}`) for the
duration of the run and reattaches them at the end. This prevents live
Kafka traffic from writing new rows into `accepted_raw_events` mid-run
— the deterministic failure we saw before this guard landed. Pass
`--pause-ingestion=false` on environments with no Kafka producer (CI).

The pause/resume pair uses a detached context for the resume so a
cancelled or failed run still reattaches the MVs before exiting.

**Caveat:** `SIGKILL` (including `kill -9` and `pkill -9`) bypasses Go
defers. If you force-kill a replay run, reattach manually:

```bash
for mv in mv_ingest_network_events_accepted mv_ingest_network_events_ignored \
         mv_ingest_streaming_events_accepted mv_ingest_streaming_events_ignored; do
  curl -sS -u "naap_admin:${CLICKHOUSE_ADMIN_PASSWORD}" \
    --data-binary "ATTACH TABLE IF NOT EXISTS naap.${mv}" \
    "http://localhost:8123/"
done
```

### API-phase caveats

The harness runs `dbt run --select api api_base` via
`docker compose --profile tooling run --rm warehouse ...`. This requires
the `warehouse` service image to be built (`make up` or
`docker compose --profile tooling up --build -d warehouse`) and
ClickHouse to be healthy. The `warehouse` service depends on
ClickHouse's healthcheck, so it waits for readiness automatically.

Determinism of api views depends on the canonical layer being stable.
Running `--layers api --skip-load --skip-resolver --skip-dbt` twice
back-to-back **will diverge** on any environment with a live Kafka
producer, because canonical_capability_* MV chains keep ingesting
between runs. Two mitigations:

- **Compose stack:** run with `make up` + stop the resolver; ingest
  MVs are paused automatically during each harness run, but the
  between-runs gap (while the shell advances from one `./bin/replay`
  invocation to the next) is not covered.
- **Ephemeral CI:** no Kafka producer is wired, so there is nothing
  to drift; `--pause-ingestion=false` is safe and slightly faster.

For bit-identical back-to-back runs on the compose stack, pause ingest
once manually before the test sequence:

```bash
for mv in mv_ingest_{network,streaming}_events_{accepted,ignored}; do
  curl -sS -u "naap_admin:${CLICKHOUSE_ADMIN_PASSWORD}" \
    --data-binary "DETACH TABLE IF EXISTS naap.${mv} SYNC" \
    "http://localhost:8123/"
done

./bin/replay --layers api --skip-load --skip-resolver --pause-ingestion=false ...
./bin/replay --layers api --skip-load --skip-resolver --pause-ingestion=false \
    --compare-to target/replay/first.json ...

# re-attach when done
for mv in mv_ingest_{network,streaming}_events_{accepted,ignored}; do
  curl -sS -u "naap_admin:${CLICKHOUSE_ADMIN_PASSWORD}" \
    --data-binary "ATTACH TABLE IF NOT EXISTS naap.${mv}" \
    "http://localhost:8123/"
done
```

### Canonical-phase performance

The canonical phase invokes the full resolver backfill over the fixture
window, partitioned by `--resolver-step` (default 24h). On the 8-day
golden fixture this is an intensive job:

- ClickHouse memory climbs to ~22 GB during the capability-interval
  reconstruction phase (the resolver fans out over the full
  `canonical_capability_snapshots` history per partition).
- Throughput on a local Docker stack runs at ~1–2k canonical-store
  rows/second, so a full 8-day replay takes **30–60 minutes**.
- This is expected for now — the serving-layer-v2 plan moves the
  capability-snapshot scan behind an index in Phase 4 (unified capability
  spine). Until then, prefer smaller fixtures for dev iteration.

**Dev iteration pattern:**

```bash
# Load once (12 min for full fixture):
./bin/replay --layers raw,normalized --output target/replay

# Iterate on canonical without reloading:
./bin/replay --layers raw,normalized,canonical --skip-load --output target/replay

# Checksum-only (30s) on an already-built canonical layer:
./bin/replay --layers raw,normalized,canonical --skip-load --skip-resolver \
  --output target/replay
```

## Usage

### 1. Fetch the fixture (one-time per developer)

```bash
# Configure staging ClickHouse credentials in .env first:
#   CLICKHOUSE_STAGING_URL=https://<staging-ch-host>
#   CLICKHOUSE_STAGING_USER=naap_reader
#   CLICKHOUSE_STAGING_PASSWORD=...
make replay-fetch-fixture
```

The fetcher pulls the pinned 8-day window (`2026-04-08 18:00 UTC` to
`2026-04-16 18:00 UTC`) as NDJSON, compresses it with zstd level 19, and
writes both the archive and an updated manifest. Re-running is idempotent
— it exits early unless `--force` is passed:

```bash
make replay-fetch-fixture-force
```

### 2. Run the harness

```bash
# Full stack (raw + normalized for PR 1):
make replay

# Fast determinism gate — loads once, re-checksums on the same state,
# and diffs. Catches regressions in the checksum itself and in the
# loader's determinism guard (ingest pause / merge completion).
make replay-verify

# Full determinism gate — loads twice and diffs. Catches regressions
# anywhere in the full pipeline including INSERT behaviour under MV
# backpressure. Slower; use in CI and at phase boundaries.
make replay-verify-full
```

Both gate targets fail with a formatted divergence report if any
table's rollup drifts, naming the first-divergent layer and table.

### 3. Direct CLI

```bash
./bin/replay \
  --fixture tests/fixtures/raw_events_golden.ndjson.zst \
  --manifest tests/fixtures/raw_events_golden.manifest.json \
  --layers raw,normalized,canonical,api \
  --output target/replay \
  --compare-to target/replay/first.json

# Dev-only: skip the load and re-checksum whatever is already in CH.
# Useful when iterating on the checksum function or adding tables to
# the layer map.
./bin/replay --skip-load ...

# Skip the canonical rebuild (reuse existing canonical_*_store state).
# Useful when iterating on the checksum function or comparing runs
# without paying the 30-60 min resolver cost each time.
./bin/replay --skip-resolver ...

# Skip the dbt view refresh (reuse existing api_* view definitions).
# Useful when iterating on the checksum function or comparing runs
# without paying the dbt subprocess cost (~5s cold, ~1s warm).
./bin/replay --skip-dbt ...

# Pin the resolver's Now to a specific instant (defaults to the
# fixture window_end).
./bin/replay --resolver-now 2026-04-16T18:00:00Z ...

# Disable ingest pause on ephemeral CI (no Kafka producer to contaminate).
./bin/replay --pause-ingestion=false ...

# Override the dbt selector (defaults to "api api_base").
./bin/replay --dbt-selector "api_hourly_streaming_sla" ...
```

All flags have env-var-friendly defaults (see `--help`).

## Interpreting divergence reports

When two runs disagree, the CLI prints:

```text
replay divergence: 3 table(s) differ
first-divergent layer: normalized
first-divergent table: naap.normalized_ai_stream_status
  [normalized] normalized_ai_stream_status     rows 123 -> 124 | checksum abc12345 -> def67890
  [normalized] normalized_session_event_rollup_latest ...
```

Read top-to-bottom in pipeline order. The first-divergent layer is the
likely root cause; divergences downstream are usually consequences of the
earliest one. Investigate the earliest table first.

Common causes:
- **Raw layer diverges** → fixture archive out of sync with manifest, or
  the loader introduced non-determinism (batch ordering, missing column).
- **Normalized layer diverges but raw does not** → MV logic changed, or
  an MV depends on `now()` / insertion order somewhere.
- **Canonical layer diverges** (once wired) → resolver used wall clock,
  hit a different dirty-partition ordering, or relied on insertion order.
- **API layer diverges** (once wired) → dbt model uses `{{ dbt_utils.current_timestamp() }}`
  or a non-deterministic function somewhere.

## How the checksum works

For each table, the harness emits:

```sql
SELECT
  count() AS rows,
  lower(hex(groupBitXor(sipHash128(col1, col2, ..., colN)))) AS checksum
FROM <db>.<table> [FINAL]
```

- `sipHash128` → per-row 128-bit fingerprint.
- `groupBitXor` → associative and commutative aggregation, so row storage
  order does not affect the result.
- `FINAL` on ReplacingMergeTree / AggregatingMergeTree / SummingMergeTree
  tables, so post-merge dedup is reflected.
- Columns are coalesced via `ifNull(toString(col), '\x00NULL')` so `NULL`
  and empty string hash distinctly but deterministically.
- `default_kind IN ('ALIAS','MATERIALIZED')` columns are excluded from the
  tuple so server-side derived values do not introduce flakiness.

The result is an order-independent, NULL-safe, schema-evolution-tolerant
fingerprint of the table's semantic content.

## Regenerating the fixture

Run `make replay-fetch-fixture-force` after any intentional change to the
fixture window, the `raw_events` schema, or the source environment. Commit
the resulting `raw_events_golden.manifest.json` change in the same PR
that justifies the regeneration, and update
[`serving-layer-v2.md`](../exec-plans/active/serving-layer-v2.md) if the
change affects downstream expected rollups.

The archive itself is never committed — `tests/fixtures/.gitignore` keeps
it local. Eventually it will be fetched from S3 at CI setup time (see
"CI plan" below).

## CI plan (not yet wired)

Stubbed at `.github/workflows/replay.yml` with `if: false` so it does not
run. Bring-up steps when ready:

1. **Trigger** — `on: pull_request` + nightly `on: schedule` against
   `refactor/medallion-v2`.
2. **Runner** — `ubuntu-latest`.
3. **Setup** — checkout, `actions/setup-go@v5` (Go 1.24.x),
   `actions/setup-python@v5` (only when the dbt phase is wired).
4. **Ephemeral ClickHouse** — start
   `clickhouse/clickhouse-server:24.3` as a service container with a
   tmpfs data dir and the bootstrap SQL mounted. No Kafka service — the
   harness inserts directly into `accepted_raw_events`.
5. **Fetch fixture** — pull `raw_events_golden.ndjson.zst` from S3
   by `archive_sha256` in the committed manifest. Cache by SHA.
6. **Migrations + dbt** — `make migrate-up && make warehouse-run` against
   the ephemeral ClickHouse.
7. **Harness** — `make replay-verify`. Artefact upload
   `target/replay/*.json` on failure.
8. **Caching** — cache Go modules by `go.sum`, dbt target by project
   hash, fixture by `archive_sha256`.
9. **Matrix (later)** — once more fixtures exist (`boundary-cases`,
   `week-rollup`), run them as a matrix.
10. **Required check** — mark `replay-verify` a required status check
    on PRs targeting `refactor/medallion-v2`.

## Extending coverage

Add tables to `api/internal/replay/layers.go` under the matching layer.
Keep each slice alphabetically sorted — the harness diffs tables in the
same order they are listed, and stable ordering makes divergence reports
comparable across commits.

Adding a new layer means:
1. Add the `Layer` constant to `layers.go`.
2. Extend `tablesByLayer` with the new tables.
3. Extend `pipeline.go` to materialise that layer (drive the resolver,
   run dbt, etc.) before checksumming it.
4. Update this document with the new layer's expectations.
