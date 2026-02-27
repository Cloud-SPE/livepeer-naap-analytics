#!/usr/bin/env python3
"""Load exported scenario fixture JSONL files into ClickHouse tables."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover
    clickhouse_connect = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load exported scenario fixtures into ClickHouse")
    parser.add_argument("--manifest", required=True, help="Path to fixture manifest.json")
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
    parser.add_argument("--truncate", action="store_true", help="Truncate touched tables before loading")
    return parser.parse_args()


def require_client() -> None:
    if clickhouse_connect is None:
        print(
            "Missing dependency: clickhouse_connect. Run via uv project: "
            "uv run --project tests/python python tests/python/scripts/load_scenario_fixtures.py ...",
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


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_fixture_files(manifest: dict[str, Any], manifest_path: Path) -> list[Path]:
    files: list[Path] = []
    base_dir = manifest_path.parent
    for scenario in manifest.get("scenarios", {}).values():
        for session in scenario.get("sessions", []):
            p = Path(session["file"])
            # Session file paths are stored relative to manifest by default.
            if not p.is_absolute():
                p = (base_dir / p).resolve()
            files.append(p)
    return files


def get_insertable_columns(client, database: str, table: str) -> list[str]:
    sql = """
        SELECT name
        FROM system.columns
        WHERE database = %(db)s
          AND table = %(table)s
          AND default_kind = ''
        ORDER BY position
    """
    result = client.query(sql, parameters={"db": database, "table": table})
    return [row[0] for row in result.result_rows]


def load_jsonl_rows(files: list[Path]) -> dict[str, list[dict[str, Any]]]:
    rows_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for file in files:
        if not file.exists():
            print(f"Warning: fixture file missing, skipped: {file}", file=sys.stderr)
            continue
        with file.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                table = obj.get("__table")
                if not table:
                    continue
                # Strip fixture metadata helper columns before insertion.
                obj.pop("__table", None)
                obj.pop("__scenario", None)
                obj.pop("__workflow_session_id", None)
                rows_by_table[table].append(obj)
    return rows_by_table


def truncate_tables(client, database: str, tables: list[str]) -> None:
    for table in tables:
        client.command(f"TRUNCATE TABLE {database}.{table}")


def insert_rows(client, database: str, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    insertable_columns = get_insertable_columns(client, database, table)
    if not insertable_columns:
        return 0

    # Keep inserts tolerant to schema drift: send only columns that exist in
    # both fixture rows and current table definition.
    available_columns = [c for c in insertable_columns if any(c in r for r in rows)]
    if not available_columns:
        return 0

    data = []
    for row in rows:
        data.append([row.get(c) for c in available_columns])

    client.insert(
        table=f"{database}.{table}",
        data=data,
        column_names=available_columns,
    )
    return len(data)


def main() -> None:
    args = parse_args()
    require_client()
    client = get_client(args)

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(2)

    manifest = load_manifest(manifest_path)
    files = collect_fixture_files(manifest, manifest_path)
    if not files:
        print("No fixture files found in manifest", file=sys.stderr)
        sys.exit(2)

    rows_by_table = load_jsonl_rows(files)
    tables = sorted(rows_by_table.keys())

    if args.truncate and tables:
        truncate_tables(client, args.database, tables)

    inserted_total = 0
    for table in tables:
        inserted = insert_rows(client, args.database, table, rows_by_table[table])
        inserted_total += inserted
        print(f"{table}: inserted {inserted} rows")

    print(f"Total inserted rows: {inserted_total}")


if __name__ == "__main__":
    main()
