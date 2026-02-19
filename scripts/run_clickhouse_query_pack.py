#!/usr/bin/env python3
"""Run ordered ClickHouse query packs marked with -- QUERY: blocks."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover
    clickhouse_connect = None


DEFAULT_SQL_FILE = "tests/integration/sql/trace_pipeline_flow.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ClickHouse query pack with -- QUERY: markers")
    parser.add_argument("--sql-file", default=DEFAULT_SQL_FILE)
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
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--from-ts", help="UTC timestamp, e.g. 2026-02-15T00:00:00Z")
    parser.add_argument("--to-ts", help="UTC timestamp, e.g. 2026-02-16T00:00:00Z")
    parser.add_argument("--max-rows", type=int, default=50)
    return parser.parse_args()


def require_client() -> None:
    if clickhouse_connect is None:
        print(
            "Missing dependency: clickhouse_connect. Run via uv project: "
            "uv run --project tools/python python scripts/run_clickhouse_query_pack.py ...",
            file=sys.stderr,
        )
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


def parse_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def resolve_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    to_ts = parse_utc(args.to_ts) if args.to_ts else datetime.now(timezone.utc)
    from_ts = parse_utc(args.from_ts) if args.from_ts else to_ts - timedelta(hours=args.lookback_hours)
    if from_ts >= to_ts:
        raise ValueError("from_ts must be earlier than to_ts")
    return from_ts, to_ts


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


def format_row(row: tuple[Any, ...]) -> str:
    vals = []
    for v in row:
        if isinstance(v, datetime):
            vals.append(v.isoformat())
        else:
            vals.append(str(v))
    return " | ".join(vals)


def main() -> None:
    args = parse_args()
    require_client()

    sql_path = Path(args.sql_file)
    if not sql_path.exists():
        print(f"SQL file not found: {sql_path}", file=sys.stderr)
        sys.exit(2)

    blocks = parse_query_blocks(sql_path)
    if not blocks:
        print(f"No -- QUERY: blocks found in {sql_path}", file=sys.stderr)
        sys.exit(2)

    from_ts, to_ts = resolve_window(args)
    params = {
        "from_ts": from_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "to_ts": to_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }

    print(f"Window UTC: {from_ts.isoformat()} -> {to_ts.isoformat()}")
    print(f"SQL file: {sql_path}")
    print("")

    client = get_client(args)
    for name, sql in blocks:
        print(f"=== {name} ===")
        result = client.query(sql, parameters=params)
        print("Columns:", ", ".join(result.column_names))
        rows = result.result_rows[: args.max_rows]
        for row in rows:
            print(format_row(row))
        if len(result.result_rows) > args.max_rows:
            print(f"... ({len(result.result_rows) - args.max_rows} more rows)")
        print("")


if __name__ == "__main__":
    main()
