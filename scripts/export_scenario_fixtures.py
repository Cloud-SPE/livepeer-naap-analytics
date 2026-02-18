#!/usr/bin/env python3
"""Export production ClickHouse rows for scenario-based integration fixtures.

Workflow:
1) Find scenario candidate sessions using tests/integration/sql/scenario_candidates.sql.
2) Export related rows across typed + fact tables.
3) Write JSONL fixtures and manifest metadata.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover
    clickhouse_connect = None


DEFAULT_SCENARIO_SQL = "tests/integration/sql/scenario_candidates.sql"
DEFAULT_OUTPUT_DIR = "tests/integration/fixtures"

SESSION_TABLES = [
    ("fact_workflow_sessions FINAL", "workflow_session_id"),
    ("fact_workflow_session_segments FINAL", "workflow_session_id"),
    ("fact_workflow_param_updates FINAL", "workflow_session_id"),
    # ("fact_lifecycle_edge_coverage FINAL", "workflow_session_id"),
    ("fact_stream_status_samples", "workflow_session_id"),
    ("fact_stream_trace_edges", "workflow_session_id"),
    ("fact_stream_ingest_samples", "workflow_session_id"),
]

EVENT_TABLES = [
    ("ai_stream_status", "event_timestamp"),
    ("stream_trace_events", "event_timestamp"),
    ("ai_stream_events", "event_timestamp"),
    ("stream_ingest_metrics", "event_timestamp"),
]


@dataclass
class SessionRef:
    scenario_name: str
    workflow_session_id: str
    stream_id: str
    request_id: str
    session_start_ts: datetime
    session_end_ts: datetime | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export scenario fixtures from ClickHouse")
    parser.add_argument("--scenario-sql", default=DEFAULT_SCENARIO_SQL)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--database", default=os.getenv("CH_DATABASE", "livepeer_analytics"))
    parser.add_argument("--host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "analytics_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "analytics_password"))
    parser.add_argument(
        "--secure",
        action="store_true",
        default=os.getenv("CH_SECURE", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--from-ts", required=True, help="UTC timestamp, e.g. 2026-01-01T00:00:00Z")
    parser.add_argument("--to-ts", required=True, help="UTC timestamp, e.g. 2026-02-16T00:00:00Z")
    parser.add_argument("--limit-per-scenario", type=int, default=3)
    parser.add_argument("--session-padding-minutes", type=int, default=5)
    parser.add_argument("--open-session-duration-minutes", type=int, default=30)
    parser.add_argument("--require-all-scenarios", dest="require_all_scenarios", action="store_true")
    parser.add_argument("--allow-missing-scenarios", dest="require_all_scenarios", action="store_false")
    parser.set_defaults(require_all_scenarios=True)
    return parser.parse_args()


def parse_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def require_client() -> None:
    if clickhouse_connect is None:
        print("Missing dependency: clickhouse_connect. Install with: pip install clickhouse-connect", file=sys.stderr)
        sys.exit(1)


def get_client(args: argparse.Namespace):
    return clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        secure=args.secure,
    )


def parse_query_blocks(path: Path) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []

    with path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if line.startswith("-- QUERY:"):
                if current_name and current_lines:
                    sql = "\n".join(current_lines).strip().rstrip(";")
                    if sql:
                        blocks.append((current_name, sql))
                current_name = line.split(":", 1)[1].strip()
                current_lines = []
                continue
            if current_name is not None:
                current_lines.append(line)

    if current_name and current_lines:
        sql = "\n".join(current_lines).strip().rstrip(";")
        if sql:
            blocks.append((current_name, sql))

    return blocks


def normalize_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        return parse_utc(value)
    return None


def to_json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


def sanitize_name(name: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9_.-]+", "_", name).strip("_")
    return out or "scenario"


def query_rows(client, sql: str, params: dict[str, Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
    result = client.query(sql, parameters=params)
    return result.column_names, result.result_rows


def discover_sessions(client, scenario_sql_path: Path, params: dict[str, Any]) -> dict[str, list[SessionRef]]:
    scenarios: dict[str, list[SessionRef]] = {}
    for block_name, sql in parse_query_blocks(scenario_sql_path):
        col_names, rows = query_rows(client, sql, params)
        idx = {name: i for i, name in enumerate(col_names)}
        refs: list[SessionRef] = []
        for row in rows:
            workflow_session_id = str(row[idx["workflow_session_id"]])
            stream_id = str(row[idx.get("stream_id", -1)]) if "stream_id" in idx and row[idx["stream_id"]] is not None else ""
            request_id = str(row[idx.get("request_id", -1)]) if "request_id" in idx and row[idx["request_id"]] is not None else ""
            session_start_ts = normalize_dt(row[idx["session_start_ts"]])
            session_end_ts = normalize_dt(row[idx["session_end_ts"]]) if "session_end_ts" in idx else None
            if not workflow_session_id or session_start_ts is None:
                continue
            refs.append(
                SessionRef(
                    scenario_name=block_name,
                    workflow_session_id=workflow_session_id,
                    stream_id=stream_id,
                    request_id=request_id,
                    session_start_ts=session_start_ts,
                    session_end_ts=session_end_ts,
                )
            )
        scenarios[block_name] = refs
    return scenarios


def export_session_rows(
    client,
    out_file: Path,
    session: SessionRef,
    padding_minutes: int,
    open_session_minutes: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    start_bound = session.session_start_ts - timedelta(minutes=padding_minutes)
    raw_end = session.session_end_ts or (session.session_start_ts + timedelta(minutes=open_session_minutes))
    end_bound = raw_end + timedelta(minutes=padding_minutes)

    with out_file.open("w", encoding="utf-8") as out:
        for table_expr, key_col in SESSION_TABLES:
            table_name = table_expr.split()[0]
            sql = f"SELECT * FROM livepeer_analytics.{table_expr} WHERE {key_col} = %(workflow_session_id)s"
            col_names, rows = query_rows(client, sql, {"workflow_session_id": session.workflow_session_id})
            counts[table_name] = counts.get(table_name, 0) + len(rows)
            for row in rows:
                payload = {
                    "__table": table_name,
                    "__scenario": session.scenario_name,
                    "__workflow_session_id": session.workflow_session_id,
                }
                for i, col in enumerate(col_names):
                    payload[col] = to_json_value(row[i])
                out.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")

        for table_name, ts_col in EVENT_TABLES:
            id_predicates: list[str] = []
            params: dict[str, Any] = {
                "window_start": start_bound.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "window_end": end_bound.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            }
            if session.request_id:
                id_predicates.append("request_id = %(request_id)s")
                params["request_id"] = session.request_id
            if session.stream_id:
                id_predicates.append("stream_id = %(stream_id)s")
                params["stream_id"] = session.stream_id

            if not id_predicates:
                counts[table_name] = counts.get(table_name, 0)
                continue

            sql = (
                f"SELECT * FROM livepeer_analytics.{table_name} "
                f"WHERE {ts_col} >= %(window_start)s AND {ts_col} < %(window_end)s "
                f"AND ({' OR '.join(id_predicates)})"
            )
            col_names, rows = query_rows(client, sql, params)
            counts[table_name] = counts.get(table_name, 0) + len(rows)
            for row in rows:
                payload = {
                    "__table": table_name,
                    "__scenario": session.scenario_name,
                    "__workflow_session_id": session.workflow_session_id,
                }
                for i, col in enumerate(col_names):
                    payload[col] = to_json_value(row[i])
                out.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")

    return counts


def main() -> None:
    args = parse_args()

    require_client()
    client = get_client(args)

    from_ts = parse_utc(args.from_ts)
    to_ts = parse_utc(args.to_ts)
    if from_ts >= to_ts:
        print("from-ts must be earlier than to-ts", file=sys.stderr)
        sys.exit(2)

    scenario_sql_path = Path(args.scenario_sql)
    if not scenario_sql_path.exists():
        print(f"Scenario SQL file not found: {scenario_sql_path}", file=sys.stderr)
        sys.exit(2)

    params = {
        "from_ts": from_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "to_ts": to_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "limit_per_scenario": int(args.limit_per_scenario),
    }

    scenarios = discover_sessions(client, scenario_sql_path, params)
    missing = [name for name, refs in scenarios.items() if not refs]
    if missing:
        print("Missing scenario candidates:")
        for m in missing:
            print(f"  - {m}")
        if args.require_all_scenarios:
            print("Aborting because --require-all-scenarios is active.", file=sys.stderr)
            sys.exit(1)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_out = Path(args.output_dir) / f"prod_snapshot_{stamp}"
    base_out.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {"from_ts": from_ts.isoformat(), "to_ts": to_ts.isoformat()},
        "source": {
            "host": args.host,
            "port": args.port,
            "database": args.database,
            "scenario_sql": str(scenario_sql_path),
        },
        "scenarios": {},
    }

    for scenario_name, sessions in scenarios.items():
        scenario_dir = base_out / sanitize_name(scenario_name)
        scenario_dir.mkdir(parents=True, exist_ok=True)
        scenario_manifest: dict[str, Any] = {
            "candidate_count": len(sessions),
            "sessions": [],
        }

        for session in sessions:
            out_file = scenario_dir / f"{sanitize_name(session.workflow_session_id)}.jsonl"
            table_counts = export_session_rows(
                client=client,
                out_file=out_file,
                session=session,
                padding_minutes=args.session_padding_minutes,
                open_session_minutes=args.open_session_duration_minutes,
            )

            scenario_manifest["sessions"].append(
                {
                    "workflow_session_id": session.workflow_session_id,
                    "stream_id": session.stream_id,
                    "request_id": session.request_id,
                    "session_start_ts": session.session_start_ts.isoformat(),
                    "session_end_ts": session.session_end_ts.isoformat() if session.session_end_ts else None,
                    "file": str(out_file),
                    "table_counts": table_counts,
                }
            )

        manifest["scenarios"][scenario_name] = scenario_manifest

    manifest_path = base_out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Wrote fixture snapshot: {base_out}")
    print(f"Manifest: {manifest_path}")
    for scenario_name, info in manifest["scenarios"].items():
        print(f"{scenario_name}: {info['candidate_count']} sessions")


if __name__ == "__main__":
    main()
