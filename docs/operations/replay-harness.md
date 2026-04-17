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

**PR 1 (current):** raw → normalized only. The harness loads the fixture
into `naap.accepted_raw_events`, waits for the cascading MVs to settle,
and checksums every `normalized_*` table.

### Why HTTP, not the native protocol

The loader uses ClickHouse's HTTP interface (`FORMAT JSONEachRow`) rather
than the native-protocol batch API, because each INSERT into
`accepted_raw_events` triggers a cascade of 16 materialized views.
Native batches stream client→server in lockstep — the client can't get
ahead of server MV processing, and the server's default 300s receive
timeout fires long before a large batch finishes. One streaming HTTP
POST keeps the whole pipeline on the server side, back-pressure and
all. Expect CH to peg 1–3 cores and hold ~5 GB RSS during a full load.

**Later PRs:** extend to the canonical layer (drive the resolver with a
frozen `RunRequest.Now`) and the api layer (run `dbt build --select tag:api`).

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
  --layers raw,normalized \
  --output target/replay \
  --compare-to target/replay/first.json

# Dev-only: skip the load and re-checksum whatever is already in CH.
# Useful when iterating on the checksum function or adding tables to
# the layer map.
./bin/replay --skip-load ...

# Disable ingest pause on ephemeral CI (no Kafka producer to contaminate).
./bin/replay --pause-ingestion=false ...
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
