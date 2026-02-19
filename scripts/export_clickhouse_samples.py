#!/usr/bin/env python3
"""Export sampled ClickHouse event rows to JSONL for metric validation."""

import argparse
import json
import os
import sys
from datetime import date, datetime

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None


DEFAULT_TABLES = [
    "ai_stream_status",
    "stream_ingest_metrics",
    "stream_trace_events",
    "ai_stream_events",
    "payment_events",
    "network_capabilities",
    "discovery_results",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Sample ClickHouse event tables to JSONL")
    parser.add_argument("--output", required=True, help="Output JSONL file path")
    parser.add_argument("--tables", nargs="*", default=None, help="Tables to sample")
    parser.add_argument("--per-sample", type=int, default=50, help="Rows per sample slice")
    parser.add_argument("--lookback-hours", type=int, default=24, help="Recent window lookback hours")
    parser.add_argument("--older-hours", type=int, default=168, help="Older window lookback hours (from now)")
    parser.add_argument("--database", default=os.getenv("CH_DATABASE", "livepeer_analytics"))
    # parser.add_argument("--host", default=os.getenv("CH_HOST", "clickhouse.livepeer.cloud"))
    parser.add_argument("--host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "analytics_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "analytics_password"))
    parser.add_argument("--secure", action="store_true", default=os.getenv("CH_SECURE", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--random-sample", action="store_true", help="Also include random sample")
    return parser.parse_args()


def require_client():
    if clickhouse_connect is None:
        print(
            "Missing dependency: clickhouse_connect. Run via uv project: "
            "uv run --project tools/python python scripts/export_clickhouse_samples.py ...",
            file=sys.stderr,
        )
        sys.exit(1)


def get_client(args):
    return clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        secure=args.secure,
    )


def query(client, sql, params=None):
    result = client.query(sql, parameters=params or {})
    return result.column_names, result.result_rows


def list_tables(client, database):
    sql = "SELECT name FROM system.tables WHERE database = %(db)s"
    _, rows = query(client, sql, {"db": database})
    return {r[0] for r in rows}


def get_columns(client, database, table):
    sql = """
        SELECT name, type
        FROM system.columns
        WHERE database = %(db)s AND table = %(table)s
        ORDER BY position
    """
    _, rows = query(client, sql, {"db": database, "table": table})
    return [(r[0], r[1]) for r in rows]


def pick_time_column(columns):
    col_names = {c[0] for c in columns}
    for candidate in ("event_timestamp", "timestamp", "event_date"):
        if candidate in col_names:
            return candidate
    return None


def build_missing_condition(columns):
    conditions = []
    for name, ctype in columns:
        if name in {"stream_id", "request_id", "pipeline_id", "gateway", "orchestrator_address", "orchestrator_url"}:
            if "String" in ctype:
                conditions.append(f"({name} = '' OR {name} IS NULL)")
    if not conditions:
        return None
    return " OR ".join(conditions)


def select_rows(client, table, columns, where_clause, limit, order_by=None):
    col_list = ", ".join(columns)
    sql = f"SELECT {col_list} FROM {table}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    sql += f" LIMIT {limit}"
    return query(client, sql)


def write_rows(out, table, columns, rows, sample_label):
    for row in rows:
        payload = {"__table": table, "__sample": sample_label}
        for idx, col in enumerate(columns):
            val = row[idx]
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            payload[col] = val
        out.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main():
    args = parse_args()
    require_client()
    client = get_client(args)

    available = list_tables(client, args.database)
    tables = args.tables or DEFAULT_TABLES
    tables = [t for t in tables if t in available]
    if not tables:
        print("No matching tables found in database", file=sys.stderr)
        sys.exit(1)

    with open(args.output, "w", encoding="utf-8") as out:
        for table in tables:
            cols = get_columns(client, args.database, table)
            if not cols:
                continue
            col_names = [c[0] for c in cols]
            time_col = pick_time_column(cols)
            missing_cond = build_missing_condition(cols)

            if time_col:
                recent_where = f"{time_col} >= now() - INTERVAL {args.lookback_hours} HOUR"
                _, rows = select_rows(client, table, col_names, recent_where, args.per_sample, f"{time_col} DESC")
                write_rows(out, table, col_names, rows, "recent")

                older_where = (
                    f"{time_col} >= now() - INTERVAL {args.older_hours} HOUR "
                    f"AND {time_col} < now() - INTERVAL {args.lookback_hours} HOUR"
                )
                _, rows = select_rows(client, table, col_names, older_where, args.per_sample, f"{time_col} ASC")
                write_rows(out, table, col_names, rows, "older")

            if missing_cond:
                _, rows = select_rows(client, table, col_names, missing_cond, args.per_sample, None)
                write_rows(out, table, col_names, rows, "missing")

            if args.random_sample:
                _, rows = select_rows(client, table, col_names, None, args.per_sample, "rand()")
                write_rows(out, table, col_names, rows, "random")

    print(f"Wrote samples to {args.output}")


if __name__ == "__main__":
    main()
