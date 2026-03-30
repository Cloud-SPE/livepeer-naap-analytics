# Resolver Operations

The hard-cutover ingest boundary is ClickHouse-native:

- Kafka Engine tables read broker topics
- accepted analytics traffic lands in `naap.accepted_raw_events`
- unsupported families/subtypes land in `naap.ignored_raw_events`
- normalization and resolver logic read only `naap.accepted_raw_events`

## Commands

- `make resolver-logs`
- `make resolver-auto`
- `make resolver-auto DRY_RUN=1`
- `make resolver-bootstrap`
- `make resolver-bootstrap DRY_RUN=1`
- `make resolver-tail`
- `make resolver-tail DRY_RUN=1`
- `make resolver-backfill FROM=... TO=... ORG=...`
- `make resolver-backfill FROM=... TO=... ORG=... DRY_RUN=1`
- `make resolver-backfill FROM=... TO=... EXCLUDE_ORG_PREFIXES=vtest_`
- `make resolver-repair-window FROM=... TO=... ORG=...`
- `make resolver-repair-window FROM=... TO=... ORG=... DRY_RUN=1`
- `make parity-verify FROM=... TO=... ORG=...`
- `make parity-verify FROM=... TO=... EXCLUDE_ORG_PREFIXES=vtest_`

For wider slices, the resolver targets also accept:

- `CLICKHOUSE_TIMEOUT=120s`
- `CLICKHOUSE_TIMEOUT=300s`

Examples:

- `make resolver-backfill FROM=2026-03-21T00:00:00Z TO=2026-03-30T00:00:00Z EXCLUDE_ORG_PREFIXES=vtest_ CLICKHOUSE_TIMEOUT=300s`
- `make parity-verify FROM=2026-03-21T00:00:00Z TO=2026-03-30T00:00:00Z EXCLUDE_ORG_PREFIXES=vtest_ CLICKHOUSE_TIMEOUT=300s`

When the resolver code changes, rebuild the Docker image before trusting any
Docker-based benchmark, backfill, or verify result:

- `docker compose build resolver`

## Runtime modes

### `auto`

Recommended production mode for a single long-lived resolver service.

`auto` runs one scheduler loop with fixed priority:

1. visible closed historical backlog from `naap.accepted_raw_events`
2. closed historical days dirtied later by newly accepted raw arrivals
3. the live lateness-window `tail`

The service executes one owned slice at a time and keeps the existing
window-claim safety model. It does not run these lanes concurrently inside the
process.

Dirty historical repair is driven by:

- `naap.resolver_runtime_state` watermarking accepted-raw scan progress with
  `(last_ingested_at, last_event_id)`
- `naap.resolver_dirty_partitions` as the durable queue of closed historical
  `(org, event_date)` repairs

Automatic repair is intentionally stabilized during active replay:

- newly accepted late rows for a closed historical day update that day's dirty
  state rather than creating duplicate work
- if a dirty day is already `claimed`, new late arrivals are coalesced onto the
  in-flight claim instead of flipping the day back to `pending`
- a pending dirty day is only eligible for automatic replay once it has been
  quiet for `RESOLVER_DIRTY_QUIET_PERIOD`

This avoids repeatedly replaying the same historical day while accepted raw
backfill is still actively delivering more rows for that day.

Only resolver-relevant accepted raw families enqueue dirty historical work:

- `stream_trace`
- `ai_stream_status`
- `ai_stream_events`
- `network_capabilities`

`create_new_payment`, `discovery_results`, and `stream_ingest_metrics` do not
dirty resolver history.

If `DRY_RUN=1` is set, `auto` evaluates scheduler priority, backlog planning,
dirty accepted-raw detection, and live tail selection without mutating resolver
tables, queue state, runtime claims, or bookkeeping rows.

### `bootstrap`

Derives the historical backlog automatically from `naap.accepted_raw_events`,
replays closed `(org, event_date)` partitions that have not already succeeded,
and then hands off to `tail` automatically once replay has reached the current
lateness cutoff.

Bootstrap uses the same resolver image and environment as steady state. It is
now mainly an operator/debug mode because `RESOLVER_MODE=auto` is the normal
deployment shape.

If a bounded bootstrap is needed for debugging, `FROM`, `TO`, `ORG`, and
`EXCLUDE_ORG_PREFIXES` are still accepted, but production bootstrap should
usually derive its own range.

If `DRY_RUN=1` is set, bootstrap computes backlog partitions and runs the same
read path without mutating any resolver tables or handing off into `tail`.

### `tail`

Continuously processes the live lateness window and republishes:

- `canonical_selection_events`
- `canonical_selection_attribution_decisions`
- `canonical_selection_attribution_current`
- `canonical_session_current_store`
- `canonical_status_hours_store`
- `canonical_session_demand_input_current`

If `DRY_RUN=1` is set, `tail` executes one bounded lateness-window pass without
writing current-state, selection-state, or serving rows.

### `backfill`

Enumerates deterministic `(org, event_date)` partitions and replays them
through the same resolver logic used by tail.

Unlike `bootstrap`, `backfill` requires explicit bounds and does not continue
into `tail`.

For local wide-scope runs on a shared dev dataset, use
`EXCLUDE_ORG_PREFIXES=vtest_` to skip validation-org noise while keeping the
same resolver logic for the real org partitions.

If older local replay or validation data is already present in the retained
dataset, remove it before taking broad measurements so current-store counts and
serving rollups are not distorted.

### `repair-window`

Claims exclusive ownership for a bounded window and reruns resolver logic for
that slice only.

All mutating resolver modes now claim the owned window in
`naap.resolver_window_claims` before writing. The claim is lease-renewed while
the run is active. If another active overlapping mutating claim already owns the
window:

- `repair-window` exits without taking the window
- `bootstrap`, `backfill`, and `tail` skip that claimed slice and try again on
  the next eligible partition or tail interval

Use `DRY_RUN=1` to debug partition planning, attribution, and lifecycle output
without mutating resolver tables, creating runtime claims, or recording
`resolver_runs` / `resolver_backfill_runs` bookkeeping rows.

### `verify`

Runs the same extraction and attribution logic without writing current-state
tables. Use it to compare overlap bands and parity slices.

## Diagnostics

Use `naap.ignored_raw_event_diagnostics` and `naap.ignored_raw_events` to
inspect explicit `RULE-INGEST-003` noise that is being held out of the accepted
analytics path:

- unsupported top-level raw event families
- ignored non-core `stream_trace` subtypes such as `app_*`
- scope-client trace noise without canonical stream identity
- unsupported `stream_trace` subtypes that remain detectable but non-canonical

Use `naap.resolver_window_claims FINAL` to inspect current runtime ownership and
recent releases for overlapping mutating runs.

Use `naap.resolver_dirty_partitions FINAL` to inspect historical late-arrival
repair state:

- `pending`: closed historical day is waiting to be replayed
- `claimed`: the `auto` lane has taken that day for repair
- `success`: the dirty day has been repaired successfully
- `failed`: automatic repair hit an error and needs inspection

Use `naap.resolver_runtime_state FINAL` to inspect the accepted-raw dirty-scan
watermark for the active scheduler scope.

Lifecycle semantics are now split explicitly in current-state outputs:

- `requested_seen`: request entered the denominator
- `playable_seen`: startup/playable success evidence exists
- `selection_outcome`: `selected | no_orch | unknown`
- `startup_outcome`: `success | failed | unknown`
- `excusal_reason`: `none | no_orch | excusable_error`

Attribution diagnostics are also explicit in current-state outputs:

- `attribution_status`: `resolved | hardware_less | stale | ambiguous | unresolved`
- `attribution_reason`: concrete resolver reason within that status

Important current unresolved reasons:

- `missing_uri_snapshot_local_alias_present`
- `missing_uri_snapshot_address_match_present`
- `no_selection_no_orch_excused`

Important nuance:

- `missing_uri_snapshot_local_alias_present` remains `unresolved`
- `matched_without_hardware` remains `hardware_less`
- unresolved URI/local-alias cases still appear in session and status-hour
  facts, but they are not equivalent to hardware-less attribution and must not
  be treated as GPU-attributed matches

Resolver `/healthz` on the metrics port now includes:

- `mode`
- `phase`
- `dirty_queue_depth`
- `accepted_raw_scan_watermark`
- `tail_watermark`
- `active_claim`

Resolver scheduler phase may temporarily report `historical_repair_wait` when a
closed dirty day exists but has not yet satisfied the quiet-period gate.

Use heavy attribution-gap views only on bounded slices. Broad all-org runs can
be expensive enough to restart a local ClickHouse. For wide investigations,
prefer direct aggregates over:

- `canonical_session_current_store FINAL`
- `canonical_selection_events`
- `canonical_orch_capability_versions`

## Dead letters

The resolver writes non-recoverable work items to `resolver_dead_letters` when:

- stable identity cannot be derived
- required lineage is missing
- capability interval construction fails deterministically
- retry budget is exceeded
- a resolver invariant prevents safe materialization

Dead-lettered records block parity signoff for the target slice.
