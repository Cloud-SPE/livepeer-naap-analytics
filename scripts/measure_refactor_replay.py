#!/usr/bin/env python3
"""Capture a repeatable snapshot of the refactored replay/catch-up path."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import urllib.parse
import urllib.request


UTC = timezone.utc


@dataclass
class Args:
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str
    database: str
    output_dir: str
    window_minutes: int
    run_limit: int
    stdout_only: bool


def parse_args() -> Args:
    dotenv = read_dotenv(Path(".env"))

    def dotenv_first(*keys: str, fallback: str | None = None) -> str | None:
        for key in keys:
            value = dotenv.get(key)
            if value:
                return value
        return fallback

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--clickhouse-url",
        default=dotenv_first("CLICKHOUSE_HTTP_URL", fallback="http://127.0.0.1:8123"),
        help="ClickHouse HTTP endpoint",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=dotenv_first(
            "CLICKHOUSE_ADMIN_USER", "CLICKHOUSE_USER", fallback="naap_admin"
        ),
        help="ClickHouse user",
    )
    parser.add_argument(
        "--clickhouse-password",
        default=dotenv_first(
            "CLICKHOUSE_ADMIN_PASSWORD", "CLICKHOUSE_PASSWORD", fallback="changeme"
        ),
        help="ClickHouse password",
    )
    parser.add_argument(
        "--database",
        default=dotenv_first("CLICKHOUSE_DATABASE", "CLICKHOUSE_DB", fallback="naap"),
        help="ClickHouse database",
    )
    parser.add_argument(
        "--output-dir",
        default=".local/baselines",
        help="Directory for JSON/Markdown snapshots",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=10,
        help="Recent query-log window for performance summary",
    )
    parser.add_argument(
        "--run-limit",
        type=int,
        default=10,
        help="How many recent completed refresh runs to include",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Print the snapshot JSON and skip writing files",
    )
    ns = parser.parse_args()
    return Args(
        clickhouse_url=ns.clickhouse_url,
        clickhouse_user=ns.clickhouse_user,
        clickhouse_password=ns.clickhouse_password,
        database=ns.database,
        output_dir=ns.output_dir,
        window_minutes=ns.window_minutes,
        run_limit=ns.run_limit,
        stdout_only=ns.stdout_only,
    )


def read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def run_clickhouse(args: Args, sql: str) -> list[dict[str, Any]]:
    query = " ".join(sql.strip().split())
    if not query.endswith("FORMAT JSONEachRow"):
        query = f"{query} FORMAT JSONEachRow"
    params = urllib.parse.urlencode(
        {
            "user": args.clickhouse_user,
            "password": args.clickhouse_password,
            "database": args.database,
        }
    )
    request = urllib.request.Request(
        f"{args.clickhouse_url.rstrip('/')}/?{params}",
        data=query.encode(),
        method="POST",
    )
    with urllib.request.urlopen(request) as resp:
        body = resp.read().decode()
    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def one(args: Args, sql: str) -> dict[str, Any]:
    rows = run_clickhouse(args, sql)
    return rows[0] if rows else {}


def many(args: Args, sql: str) -> list[dict[str, Any]]:
    return run_clickhouse(args, sql)


def build_snapshot(args: Args) -> dict[str, Any]:
    measured_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    recent_runs = many(
        args,
        f"""
        SELECT
            run_id,
            mode,
            org,
            status,
            started_at,
            finished_at,
            dateDiff('second', started_at, finished_at) AS duration_seconds,
            rows_processed,
            mismatch_count,
            error_summary
        FROM resolver_runs
        ORDER BY started_at DESC
        LIMIT {args.run_limit}
        """,
    )

    ingest_window = one(
        args,
        """
        SELECT
            min(ingested_at) AS ingest_start,
            max(ingested_at) AS ingest_end,
            dateDiff('second', min(ingested_at), max(ingested_at)) AS ingest_span_seconds,
            count() AS raw_event_rows
        FROM accepted_raw_events
        """,
    )

    watermark = one(
        args,
        """
        SELECT
            max(event_ts) AS latest_event_ts,
            max(ingested_at) AS latest_ingested_at,
            (SELECT max(last_seen) FROM canonical_session_current) AS latest_session_last_seen,
            dateDiff(
                'second',
                max(event_ts),
                max(ingested_at)
            ) AS watermark_lag_seconds
        FROM accepted_raw_events
        """,
    )

    stage_activity = one(
        args,
        """
        SELECT
            countIf(released_at IS NULL) AS active_claims,
            max(updated_at) AS last_claim_update
        FROM resolver_window_claims FINAL
        """,
    )

    repair_state = one(
        args,
        """
        SELECT
            (SELECT count() FROM canonical_selection_attribution_current FINAL) AS selection_attribution_current_rows,
            (SELECT count() FROM canonical_session_current FINAL) AS session_current_rows,
            (SELECT count() FROM canonical_session_current_store) AS session_current_store_rows,
            (SELECT count() FROM canonical_session_demand_input_current FINAL) AS demand_input_rows,
            (SELECT count() FROM canonical_status_hours_store) AS status_hours_rows,
            (SELECT count() FROM api_network_demand_by_org_store) AS demand_store_rows,
            (SELECT count() FROM api_sla_compliance_by_org_store) AS sla_store_rows,
            (SELECT count() FROM api_gpu_metrics_by_org_store) AS gpu_metrics_store_rows
        """,
    )

    store_counts = many(
        args,
        """
        SELECT 'accepted_raw_events' AS table, count() AS rows FROM accepted_raw_events
        UNION ALL
        SELECT 'canonical_selection_attribution_current' AS table, count() AS rows FROM canonical_selection_attribution_current FINAL
        UNION ALL
        SELECT 'canonical_session_current' AS table, count() AS rows FROM canonical_session_current FINAL
        UNION ALL
        SELECT 'canonical_session_current_store' AS table, count() AS rows FROM canonical_session_current_store
        UNION ALL
        SELECT 'canonical_status_hours_store' AS table, count() AS rows FROM canonical_status_hours_store
        UNION ALL
        SELECT 'canonical_status_samples_recent_store' AS table, count() AS rows FROM canonical_status_samples_recent_store
        UNION ALL
        SELECT 'canonical_active_stream_state_latest_store' AS table, count() AS rows FROM canonical_active_stream_state_latest_store
        UNION ALL
        SELECT 'canonical_session_demand_input_current' AS table, count() AS rows FROM canonical_session_demand_input_current FINAL
        UNION ALL
        SELECT 'api_network_demand_by_org_store' AS table, count() AS rows FROM api_network_demand_by_org_store
        UNION ALL
        SELECT 'api_sla_compliance_by_org_store' AS table, count() AS rows FROM api_sla_compliance_by_org_store
        UNION ALL
        SELECT 'api_gpu_metrics_by_org_store' AS table, count() AS rows FROM api_gpu_metrics_by_org_store
        ORDER BY table
        """,
    )

    canonical_quality = one(
        args,
        """
        SELECT
            count() AS total_sessions,
            countIf(selection_outcome = 'selected') AS selected_sessions,
            countIf(attribution_status = 'resolved') AS resolved,
            countIf(attribution_status = 'hardware_less') AS hardware_less,
            countIf(attribution_status = 'stale') AS stale,
            countIf(attribution_status = 'ambiguous') AS ambiguous,
            countIf(attribution_status = 'unresolved') AS unresolved,
            countIf(selection_outcome = 'selected' AND attribution_status = 'resolved') AS selected_resolved,
            countIf(selection_outcome = 'selected' AND attribution_status = 'unresolved') AS selected_unresolved,
            countIf(notEmpty(ifNull(attributed_orch_address, ''))) AS with_orch,
            countIf(notEmpty(ifNull(canonical_model, ''))) AS with_model,
            countIf(notEmpty(ifNull(canonical_pipeline, ''))) AS with_pipeline,
            countIf(attribution_status = 'unresolved' AND notEmpty(ifNull(attributed_orch_address, ''))) AS unresolved_with_orch
        FROM canonical_session_current
        """,
    )

    api_gpu_quality = one(
        args,
        """
        SELECT
            count() AS total_rows,
            countIf(notEmpty(orchestrator_address)) AS rows_with_orchestrator,
            countIf(notEmpty(ifNull(model_id, ''))) AS rows_with_model,
            countIf(notEmpty(ifNull(gpu_id, ''))) AS rows_with_gpu,
            sum(known_sessions_count) AS known_sessions_total
        FROM api_gpu_metrics_by_org
        """,
    )

    api_sla_quality = one(
        args,
        """
        SELECT
            count() AS total_rows,
            countIf(notEmpty(orchestrator_address)) AS rows_with_orchestrator,
            countIf(notEmpty(ifNull(model_id, ''))) AS rows_with_model,
            countIf(notEmpty(ifNull(gpu_id, ''))) AS rows_with_gpu,
            sum(known_sessions_count) AS known_sessions_total
        FROM api_sla_compliance_by_org
        """,
    )

    active_processes = many(
        args,
        """
        SELECT
            elapsed,
            read_rows,
            read_bytes,
            memory_usage,
            left(replaceRegexpAll(query, '\\\\s+', ' '), 320) AS query
        FROM system.processes
        WHERE query LIKE '%INSERT INTO naap.canonical_selection_attribution_current%'
           OR query LIKE '%INSERT INTO naap.canonical_session_current_store%'
           OR query LIKE '%INSERT INTO naap.canonical_session_demand_input_current%'
           OR query LIKE '%INSERT INTO naap.canonical_status_hours_store%'
           OR query LIKE '%INSERT INTO naap.canonical_status_samples_recent_store%'
           OR query LIKE '%INSERT INTO naap.canonical_active_stream_state_latest_store%'
           OR query LIKE '%INSERT INTO naap.api_%_store%'
        ORDER BY elapsed DESC
        LIMIT 5
        """,
    )

    performance = many(
        args,
        f"""
        SELECT
            target,
            count() AS queries,
            round(quantileExact(0.5)(query_duration_ms), 1) AS p50_ms,
            round(quantileExact(0.9)(query_duration_ms), 1) AS p90_ms,
            round(quantileExact(0.99)(query_duration_ms), 1) AS p99_ms,
            round(avg(read_rows), 1) AS avg_read_rows,
            max(read_rows) AS max_read_rows,
            round(avg(memory_usage) / 1048576, 2) AS avg_mem_mb,
            round(max(memory_usage) / 1048576, 2) AS max_mem_mb
        FROM (
            SELECT
                multiIf(
                    query LIKE '%INSERT INTO naap.canonical_selection_attribution_current%', 'canonical_selection_attribution_current',
                    query LIKE '%INSERT INTO naap.canonical_session_current_store%', 'canonical_session_current_store',
                    query LIKE '%INSERT INTO naap.canonical_session_demand_input_current%', 'canonical_session_demand_input_current',
                    query LIKE '%INSERT INTO naap.canonical_status_hours_store%', 'canonical_status_hours_store',
                    query LIKE '%INSERT INTO naap.canonical_status_samples_recent_store%', 'canonical_status_samples_recent_store',
                    query LIKE '%INSERT INTO naap.canonical_active_stream_state_latest_store%', 'canonical_active_stream_state_latest_store',
                    query LIKE '%INSERT INTO naap.api_network_demand_by_org_store%', 'api_network_demand_by_org_store',
                    query LIKE '%INSERT INTO naap.api_gpu_network_demand_by_org_store%', 'api_gpu_network_demand_by_org_store',
                    query LIKE '%INSERT INTO naap.api_sla_compliance_by_org_store%', 'api_sla_compliance_by_org_store',
                    query LIKE '%INSERT INTO naap.api_gpu_metrics_by_org_store%', 'api_gpu_metrics_by_org_store',
                    'other'
                ) AS target,
                query_duration_ms,
                read_rows,
                memory_usage
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND event_time >= now() - INTERVAL {args.window_minutes} MINUTE
              AND (
                    query LIKE '%INSERT INTO naap.canonical_selection_attribution_current%'
                 OR query LIKE '%INSERT INTO naap.canonical_session_current_store%'
                 OR query LIKE '%INSERT INTO naap.canonical_session_demand_input_current%'
                 OR query LIKE '%INSERT INTO naap.canonical_status_hours_store%'
                 OR query LIKE '%INSERT INTO naap.canonical_status_samples_recent_store%'
                 OR query LIKE '%INSERT INTO naap.canonical_active_stream_state_latest_store%'
                 OR query LIKE '%INSERT INTO naap.api_network_demand_by_org_store%'
                 OR query LIKE '%INSERT INTO naap.api_gpu_network_demand_by_org_store%'
                 OR query LIKE '%INSERT INTO naap.api_sla_compliance_by_org_store%'
                 OR query LIKE '%INSERT INTO naap.api_gpu_metrics_by_org_store%'
              )
        )
        GROUP BY target
        ORDER BY queries DESC, target
        """,
    )

    return {
        "measured_at_utc": measured_at,
        "args": asdict(args),
        "ingest_window": ingest_window,
        "watermark": watermark,
        "stage_activity": stage_activity,
        "repair_state": repair_state,
        "recent_runs": recent_runs,
        "store_counts": store_counts,
        "canonical_quality": canonical_quality,
        "api_gpu_quality": api_gpu_quality,
        "api_sla_quality": api_sla_quality,
        "performance_last_window": performance,
        "active_store_processes": active_processes,
    }


def pct(numerator: int | float, denominator: int | float) -> str:
    if not denominator:
        return "0.0%"
    return f"{(100.0 * float(numerator) / float(denominator)):.1f}%"


def render_markdown(snapshot: dict[str, Any]) -> str:
    canonical = snapshot["canonical_quality"]
    gpu = snapshot["api_gpu_quality"]
    sla = snapshot["api_sla_quality"]
    watermark = snapshot["watermark"]
    ingest = snapshot["ingest_window"]
    repair = snapshot["repair_state"]
    lines: list[str] = []
    lines.append("# Refactor Replay Snapshot")
    lines.append("")
    lines.append(f"- Measured at: `{snapshot['measured_at_utc']} UTC`")
    lines.append(
        f"- Raw ingest window: `{ingest.get('ingest_start')}` to `{ingest.get('ingest_end')}` "
        f"(`{ingest.get('ingest_span_seconds')}s`, `{ingest.get('raw_event_rows')}` rows)"
    )
    lines.append(
        f"- Latest raw event: `{watermark.get('latest_event_ts')}`; "
        f"`ingested_at={watermark.get('latest_ingested_at')}`, "
        f"`latest_session_last_seen={watermark.get('latest_session_last_seen')}`, "
        f"`event_to_ingest_lag={watermark.get('watermark_lag_seconds')}`s"
    )
    lines.append("")
    lines.append("## Repair State")
    lines.append("")
    lines.append(
        f"- Selection attribution rows: `current={repair.get('selection_attribution_current_rows')}`, "
        f"`session_current={repair.get('session_current_rows')}`, "
        f"`session_current_store={repair.get('session_current_store_rows')}`"
    )
    lines.append(
        f"- Serving inputs: `session_demand_input={repair.get('demand_input_rows')}`, "
        f"`status_hours={repair.get('status_hours_rows')}`, "
        f"`demand_store={repair.get('demand_store_rows')}`, "
        f"`sla_store={repair.get('sla_store_rows')}`, "
        f"`gpu_metrics_store={repair.get('gpu_metrics_store_rows')}`"
    )
    lines.append("")
    lines.append("## Canonical Quality")
    lines.append("")
    lines.append(
        f"- Sessions: `{canonical.get('total_sessions')}`; "
        f"`resolved={canonical.get('resolved')}` ({pct(canonical.get('resolved', 0), canonical.get('total_sessions', 0))}), "
        f"`unresolved={canonical.get('unresolved')}` ({pct(canonical.get('unresolved', 0), canonical.get('total_sessions', 0))}), "
        f"`ambiguous={canonical.get('ambiguous')}`, `stale={canonical.get('stale')}`, `hardware_less={canonical.get('hardware_less')}`"
    )
    lines.append(
        f"- Selected-session slice: `selected={canonical.get('selected_sessions')}`, "
        f"`selected_resolved={canonical.get('selected_resolved')}` ({pct(canonical.get('selected_resolved', 0), canonical.get('selected_sessions', 0))}), "
        f"`selected_unresolved={canonical.get('selected_unresolved')}` ({pct(canonical.get('selected_unresolved', 0), canonical.get('selected_sessions', 0))})"
    )
    lines.append(
        f"- Coverage: `with_orch={canonical.get('with_orch')}` ({pct(canonical.get('with_orch', 0), canonical.get('total_sessions', 0))}), "
        f"`with_model={canonical.get('with_model')}` ({pct(canonical.get('with_model', 0), canonical.get('total_sessions', 0))}), "
        f"`with_pipeline={canonical.get('with_pipeline')}` ({pct(canonical.get('with_pipeline', 0), canonical.get('total_sessions', 0))})"
    )
    lines.append(
        f"- Guardrail: `unresolved_with_orch={canonical.get('unresolved_with_orch')}`"
    )
    lines.append("")
    lines.append("## API Quality")
    lines.append("")
    lines.append(
        f"- GPU metrics rows: `{gpu.get('total_rows')}`; "
        f"`with_orchestrator={gpu.get('rows_with_orchestrator')}` ({pct(gpu.get('rows_with_orchestrator', 0), gpu.get('total_rows', 0))}), "
        f"`with_model={gpu.get('rows_with_model')}` ({pct(gpu.get('rows_with_model', 0), gpu.get('total_rows', 0))}), "
        f"`with_gpu={gpu.get('rows_with_gpu')}` ({pct(gpu.get('rows_with_gpu', 0), gpu.get('total_rows', 0))})"
    )
    lines.append(
        f"- SLA rows: `{sla.get('total_rows')}`; "
        f"`with_orchestrator={sla.get('rows_with_orchestrator')}` ({pct(sla.get('rows_with_orchestrator', 0), sla.get('total_rows', 0))}), "
        f"`with_model={sla.get('rows_with_model')}` ({pct(sla.get('rows_with_model', 0), sla.get('total_rows', 0))}), "
        f"`with_gpu={sla.get('rows_with_gpu')}` ({pct(sla.get('rows_with_gpu', 0), sla.get('total_rows', 0))})"
    )
    lines.append("")
    lines.append("## Recent Runs")
    lines.append("")
    for run in snapshot["recent_runs"][:5]:
        lines.append(
            f"- `{run['run_id']}` `{run['mode']}` `{run['status']}` "
            f"`org={run['org']}`, `{run['duration_seconds']}s`, "
            f"`rows_processed={run['rows_processed']}`, "
            f"`mismatches={run['mismatch_count']}`"
        )
    lines.append("")
    lines.append("## Performance Window")
    lines.append("")
    for row in snapshot["performance_last_window"]:
        lines.append(
            f"- `{row['target']}`: `queries={row['queries']}`, "
            f"`p50={row['p50_ms']}ms`, `p90={row['p90_ms']}ms`, `p99={row['p99_ms']}ms`, "
            f"`avg_read_rows={row['avg_read_rows']}`, `max_read_rows={row['max_read_rows']}`, "
            f"`avg_mem={row['avg_mem_mb']}MB`, `max_mem={row['max_mem_mb']}MB`"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    snapshot = build_snapshot(args)

    if args.stdout_only:
        json.dump(snapshot, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H%M%S")
    base = output_dir / f"{stamp}-refactor-replay-snapshot"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")

    json_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    md_path.write_text(render_markdown(snapshot))

    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
