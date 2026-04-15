#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

APPLY="${APPLY:-0}"
FROM_TS="${FROM:-}"
TO_TS="${TO:-}"
EXCLUDE_ORG_PREFIXES="${EXCLUDE_ORG_PREFIXES:-}"
CLICKHOUSE_TIMEOUT="${CLICKHOUSE_TIMEOUT:-300s}"
CH_USER="${CH_USER:-naap_admin}"
CH_PASSWORD="${CH_PASSWORD:-changeme}"
CH_DB="${CH_DB:-naap}"

usage() {
  cat <<'EOF'
Rebuild supported downstream state from retained ClickHouse raw history.

This workflow preserves:
- naap.accepted_raw_events
- naap.ignored_raw_events
- external metadata tables such as orch_metadata and gateway_metadata

This workflow purges and rebuilds:
- raw-derived normalized event-family tables
- raw-derived AI batch / BYOC normalized tables
- raw-fed dashboard rollups rebuilt from accepted_raw_events
- session rollups, attribution inputs, and capability rollups rebuilt from normalized_*
- resolver-owned canonical/current/serving stores
- legacy canonical_session_attribution_* compatibility tables rebuilt from the
  selection-centered attribution outputs
- resolver runtime bookkeeping and repair queues

Usage:
  scripts/rebuild_from_retained_raw.sh
  APPLY=1 scripts/rebuild_from_retained_raw.sh
  APPLY=1 FROM=2026-04-01T00:00:00Z TO=2026-04-08T00:00:00Z scripts/rebuild_from_retained_raw.sh
  APPLY=1 EXCLUDE_ORG_PREFIXES=vtest_ scripts/rebuild_from_retained_raw.sh

Environment overrides:
  APPLY=1                  Execute the destructive rebuild. Default is dry-run.
  FROM=<rfc3339>           Optional explicit replay start bound.
  TO=<rfc3339>             Optional explicit replay end bound.
  EXCLUDE_ORG_PREFIXES=... Optional resolver exclusion prefixes.
  CLICKHOUSE_TIMEOUT=300s  Timeout passed through to resolver backfill.
  CH_USER=naap_admin       ClickHouse admin user for purge / bound queries.
  CH_PASSWORD=changeme     ClickHouse admin password for purge / bound queries.
  CH_DB=naap               ClickHouse database name.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

clickhouse_query() {
  local query="$1"
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --database "$CH_DB" \
    --query "$query"
}

derive_bound() {
  local expr="$1"
  clickhouse_query "$expr"
}

if [[ -z "$FROM_TS" ]]; then
  FROM_TS="$(derive_bound "SELECT concat(toString(toDate(toStartOfDay(min(event_ts)))), 'T00:00:00Z') FROM accepted_raw_events FORMAT TSVRaw")"
fi

if [[ -z "$TO_TS" ]]; then
  TO_TS="$(derive_bound "SELECT concat(toString(toDate(toStartOfDay(max(event_ts) + INTERVAL 1 DAY))), 'T00:00:00Z') FROM accepted_raw_events FORMAT TSVRaw")"
fi

if [[ -z "$FROM_TS" || -z "$TO_TS" || "$FROM_TS" == "1970-01-01T00:00:00Z" || "$TO_TS" == "1970-01-02T00:00:00Z" ]]; then
  echo "accepted_raw_events does not contain usable retained history; aborting" >&2
  exit 1
fi

cat <<EOF
Retained-raw rebuild plan
=========================
Replay bounds:
  FROM=${FROM_TS}
  TO=${TO_TS}

Resolver options:
  EXCLUDE_ORG_PREFIXES=${EXCLUDE_ORG_PREFIXES:-<none>}
  CLICKHOUSE_TIMEOUT=${CLICKHOUSE_TIMEOUT}

Destructive steps:
  1. stop api + resolver
  2. truncate supported downstream physical tables
  3. rebuild raw-derived normalized and aggregate tables from accepted_raw_events
  4. rebuild normalized session rollups / capability rollups
  5. resolver backfill over retained raw bounds
  6. rebuild legacy attribution compatibility tables
  7. republish dbt semantic views
  8. restart api + resolver

Preserved tables:
  - accepted_raw_events / ignored_raw_events
  - external metadata tables populated by the enrichment worker
  - unsupported compatibility tables without a maintained retained-raw replay path
EOF

if [[ "$APPLY" != "1" ]]; then
  echo
  echo "Dry-run only. Re-run with APPLY=1 to execute."
  exit 0
fi

echo "==> stopping API and resolver"
docker compose stop api resolver

echo "==> truncating supported downstream state"
docker compose exec -T clickhouse clickhouse-client \
  --user "$CH_USER" \
  --password "$CH_PASSWORD" \
  --database "$CH_DB" \
  --multiquery <<'SQL'
TRUNCATE TABLE IF EXISTS naap.agg_stream_status_samples;
TRUNCATE TABLE IF EXISTS naap.agg_fps_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_discovery_latency_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_orch_reliability_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_orch_state;
TRUNCATE TABLE IF EXISTS naap.agg_payment_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_stream_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_stream_state;
TRUNCATE TABLE IF EXISTS naap.agg_webrtc_hourly;
TRUNCATE TABLE IF EXISTS naap.typed_stream_ingest_metrics;

TRUNCATE TABLE IF EXISTS naap.normalized_ai_batch_job;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_llm_request;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_stream_events;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_stream_status;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_auth;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_job;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_payment;
TRUNCATE TABLE IF EXISTS naap.normalized_network_capabilities;
TRUNCATE TABLE IF EXISTS naap.normalized_session_attribution_input_latest_store;
TRUNCATE TABLE IF EXISTS naap.normalized_session_trace_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_status_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_event_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_orchestrator_observation_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_status_hour_rollup;
TRUNCATE TABLE IF EXISTS naap.normalized_stream_trace;
TRUNCATE TABLE IF EXISTS naap.normalized_worker_lifecycle;
TRUNCATE TABLE IF EXISTS naap.canonical_capability_snapshot_latest;
TRUNCATE TABLE IF EXISTS naap.canonical_capability_hardware_inventory;
TRUNCATE TABLE IF EXISTS naap.canonical_capability_hardware_inventory_by_snapshot;
TRUNCATE TABLE IF EXISTS naap.canonical_capability_snapshots_by_address;
TRUNCATE TABLE IF EXISTS naap.canonical_capability_snapshots_by_uri;
TRUNCATE TABLE IF EXISTS naap.canonical_latest_orchestrator_pipeline_inventory_agg;

TRUNCATE TABLE IF EXISTS naap.canonical_selection_events;
TRUNCATE TABLE IF EXISTS naap.canonical_selection_attribution_decisions;
TRUNCATE TABLE IF EXISTS naap.canonical_selection_attribution_current;
TRUNCATE TABLE IF EXISTS naap.canonical_orch_capability_versions;
TRUNCATE TABLE IF EXISTS naap.canonical_orch_capability_intervals;
TRUNCATE TABLE IF EXISTS naap.canonical_session_attribution_audit;
TRUNCATE TABLE IF EXISTS naap.canonical_session_attribution_current;
TRUNCATE TABLE IF EXISTS naap.canonical_session_attribution_latest_store;
TRUNCATE TABLE IF EXISTS naap.canonical_session_current_store;
TRUNCATE TABLE IF EXISTS naap.canonical_session_demand_input_current;
TRUNCATE TABLE IF EXISTS naap.canonical_status_hours_store;
TRUNCATE TABLE IF EXISTS naap.canonical_status_samples_recent_store;
TRUNCATE TABLE IF EXISTS naap.canonical_active_stream_state_latest_store;
TRUNCATE TABLE IF EXISTS naap.canonical_payment_links_store;
TRUNCATE TABLE IF EXISTS naap.canonical_ai_batch_job_store;
TRUNCATE TABLE IF EXISTS naap.canonical_byoc_job_store;

TRUNCATE TABLE IF EXISTS naap.canonical_streaming_demand_hourly_store;
TRUNCATE TABLE IF EXISTS naap.canonical_streaming_sla_input_hourly_store;
TRUNCATE TABLE IF EXISTS naap.canonical_streaming_sla_hourly_store;
TRUNCATE TABLE IF EXISTS naap.canonical_streaming_gpu_metrics_hourly_store;

TRUNCATE TABLE IF EXISTS naap.selection_attribution_changes;
TRUNCATE TABLE IF EXISTS naap.session_current_changes;
TRUNCATE TABLE IF EXISTS naap.status_hour_changes;

TRUNCATE TABLE IF EXISTS naap.resolver_backfill_runs;
TRUNCATE TABLE IF EXISTS naap.resolver_dead_letters;
TRUNCATE TABLE IF EXISTS naap.resolver_dirty_partitions;
TRUNCATE TABLE IF EXISTS naap.resolver_dirty_orchestrators;
TRUNCATE TABLE IF EXISTS naap.resolver_dirty_selection_events;
TRUNCATE TABLE IF EXISTS naap.resolver_dirty_sessions;
TRUNCATE TABLE IF EXISTS naap.resolver_dirty_windows;
TRUNCATE TABLE IF EXISTS naap.resolver_repair_requests;
TRUNCATE TABLE IF EXISTS naap.resolver_runs;
TRUNCATE TABLE IF EXISTS naap.resolver_runtime_state;
TRUNCATE TABLE IF EXISTS naap.resolver_window_claims;
TRUNCATE TABLE IF EXISTS naap.resolver_query_event_ids;
TRUNCATE TABLE IF EXISTS naap.resolver_query_identities;
TRUNCATE TABLE IF EXISTS naap.resolver_query_selection_event_ids;
TRUNCATE TABLE IF EXISTS naap.resolver_query_session_keys;
TRUNCATE TABLE IF EXISTS naap.resolver_query_window_slices;
SQL

echo "==> rebuilding raw-derived normalized and aggregate tables from accepted_raw_events"
make backfill-normalized-from-retained-raw

echo "==> rebuilding normalized session rollups and capability rollups"
make backfill-rollups

echo "==> replaying resolver-owned history from retained raw"
if [[ -n "$EXCLUDE_ORG_PREFIXES" ]]; then
  make resolver-backfill \
    FROM="$FROM_TS" \
    TO="$TO_TS" \
    EXCLUDE_ORG_PREFIXES="$EXCLUDE_ORG_PREFIXES" \
    CLICKHOUSE_TIMEOUT="$CLICKHOUSE_TIMEOUT"
else
  make resolver-backfill \
    FROM="$FROM_TS" \
    TO="$TO_TS" \
    CLICKHOUSE_TIMEOUT="$CLICKHOUSE_TIMEOUT"
fi

echo "==> rebuilding legacy session attribution compatibility tables"
make backfill-legacy-session-attribution-compat

echo "==> republishing dbt semantic views"
make warehouse-run

echo "==> restarting API and resolver"
docker compose up -d api resolver

cat <<EOF
Rebuild complete.

Recommended follow-up:
  make test-validation-clean
  make parity-verify FROM=${FROM_TS} TO=${TO_TS}
EOF
