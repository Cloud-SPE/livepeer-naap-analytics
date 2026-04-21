#!/bin/bash
# Layer-by-layer job count audit for the "Jobs per Hour by Type" /
# "Success Rate per Hour by Type" dashboard panels. Counts how many jobs
# each medallion layer sees for a given [FROM, TO) window, grouped by
# hour and job_type. A drop between adjacent layers is where data is lost.
#
# Lineage:
#   raw_events
#     └── accepted_raw_events                   # dedupped MV target
#         ├── normalized_ai_batch_job           # completed subtype only
#         │   └── canonical_ai_batch_job_store  # resolver-attributed, 1/job
#         │       └── api_fact_ai_batch_job     # dbt dedup view
#         │           └── api_hourly_request_demand_store  # resolver-rolled hourly
#         │               └── api_hourly_request_demand    # ← panel reads here
#         └── normalized_byoc_job               # completed subtype only
#             └── canonical_byoc_job_store
#                 └── api_fact_byoc_job
#                     └── (same hourly path)
#
# Usage:
#   bash scripts/lineage-audit.sh                      # defaults: last 7 days
#   FROM='2026-04-10 00:00:00' TO='2026-04-17 00:00:00' bash scripts/lineage-audit.sh
#
# Reading the output:
#   * Zero at layer 1 (raw)       → upstream gateway never emitted the event
#   * Drop 2 → 3 (accepted→norm)  → normalized view's subtype filter
#   * Drop 3 → 4 (norm→canonical) → resolver never processed that window;
#                                    check resolver_dirty_partitions and
#                                    resolver_runs for the hour
#   * Drop 4 → 5 (canonical→api_fact) → dbt view rebuild lag
#   * Drop 5 → 6 (api_fact→api_hourly) → resolver's insertHourlyRequestDemand
#                                         missed the window; cross-reference
#                                         resolver_query_window_slices

set -u

# Default window: last 7 days (wall-clock). Override FROM/TO for a
# specific slice (e.g. a fixture window in replay mode).
DEFAULT_FROM=$(date -u -d '7 days ago' +'%Y-%m-%d %H:00:00' 2>/dev/null || date -u -v-7d +'%Y-%m-%d %H:00:00')
DEFAULT_TO=$(date -u +'%Y-%m-%d %H:00:00')
FROM="${FROM:-$DEFAULT_FROM}"
TO="${TO:-$DEFAULT_TO}"

CH=(docker compose exec -T clickhouse clickhouse-client --user naap_admin --password changeme --format PrettyCompact --query)

banner() { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }

banner "window: $FROM  →  $TO (UTC)"

banner "1 · raw_events (event_type in {ai_batch_request, job_gateway, job_orchestrator})"
"${CH[@]}" "
SELECT toStartOfHour(event_ts) AS hour,
       multiIf(event_type='ai_batch_request', 'ai-batch-raw',
               event_type='job_gateway',      'byoc-gw',
               event_type='job_orchestrator', 'byoc-orch',
               event_type) AS kind,
       count() AS rows
FROM naap.raw_events
WHERE event_ts >= toDateTime('$FROM') AND event_ts < toDateTime('$TO')
  AND event_type IN ('ai_batch_request','job_gateway','job_orchestrator')
GROUP BY hour, kind
ORDER BY hour, kind"

banner "2 · accepted_raw_events (post-dedup)"
"${CH[@]}" "
SELECT toStartOfHour(event_ts) AS hour,
       event_type,
       count() AS rows
FROM naap.accepted_raw_events
WHERE event_ts >= toDateTime('$FROM') AND event_ts < toDateTime('$TO')
  AND event_type IN ('ai_batch_request','job_gateway','job_orchestrator')
GROUP BY hour, event_type
ORDER BY hour, event_type"

banner "3 · normalized_{ai_batch_job,byoc_job} (completed events only)"
"${CH[@]}" "
SELECT hour, job_type, count() AS rows FROM (
    SELECT toStartOfHour(event_ts) AS hour, 'ai-batch' AS job_type
    FROM naap.normalized_ai_batch_job FINAL
    WHERE event_ts >= toDateTime('$FROM') AND event_ts < toDateTime('$TO')
      AND subtype = 'ai_batch_request_completed' AND request_id != ''
    UNION ALL
    SELECT toStartOfHour(event_ts), 'byoc'
    FROM naap.normalized_byoc_job FINAL
    WHERE event_ts >= toDateTime('$FROM') AND event_ts < toDateTime('$TO')
      AND subtype = 'job_gateway_completed'
)
GROUP BY hour, job_type
ORDER BY hour, job_type"

banner "4 · canonical_*_job_store (resolver-attributed, deduped by (org, request_id|event_id))"
"${CH[@]}" "
SELECT hour, job_type, count() AS rows FROM (
    SELECT toStartOfHour(completed_at) AS hour, 'ai-batch' AS job_type
    FROM (
        SELECT argMax(completed_at, materialized_at) AS completed_at
        FROM naap.canonical_ai_batch_job_store
        GROUP BY org, request_id
    )
    WHERE completed_at >= toDateTime('$FROM') AND completed_at < toDateTime('$TO')
    UNION ALL
    SELECT toStartOfHour(completed_at), 'byoc'
    FROM (
        SELECT argMax(completed_at, materialized_at) AS completed_at
        FROM naap.canonical_byoc_job_store
        GROUP BY org, event_id
    )
    WHERE completed_at >= toDateTime('$FROM') AND completed_at < toDateTime('$TO')
)
GROUP BY hour, job_type
ORDER BY hour, job_type"

banner "5 · api_fact_{ai_batch_job,byoc_job} (dbt dedup-then-pass-through views)"
"${CH[@]}" "
SELECT hour, job_type, count() AS rows FROM (
    SELECT toStartOfHour(completed_at) AS hour, 'ai-batch' AS job_type
    FROM naap.api_fact_ai_batch_job
    WHERE completed_at >= toDateTime('$FROM') AND completed_at < toDateTime('$TO')
    UNION ALL
    SELECT toStartOfHour(completed_at), 'byoc'
    FROM naap.api_fact_byoc_job
    WHERE completed_at >= toDateTime('$FROM') AND completed_at < toDateTime('$TO')
)
GROUP BY hour, job_type
ORDER BY hour, job_type"

banner "6 · api_hourly_request_demand (panel source — resolver-rolled hourly)"
"${CH[@]}" "
SELECT window_start AS hour,
       if(capability_family = 'byoc', 'byoc', 'ai-batch') AS job_type,
       sum(job_count)     AS jobs,
       sum(success_count) AS successes
FROM naap.api_hourly_request_demand
WHERE window_start >= toDateTime('$FROM') AND window_start < toDateTime('$TO')
GROUP BY hour, job_type
ORDER BY hour, job_type"

banner "Resolver windows actually published for this range"
"${CH[@]}" "
SELECT toStartOfHour(window_start) AS hour, countDistinct(org) AS orgs_published
FROM naap.resolver_query_window_slices
WHERE window_start >= toDateTime('$FROM') AND window_start < toDateTime('$TO')
GROUP BY hour ORDER BY hour"
