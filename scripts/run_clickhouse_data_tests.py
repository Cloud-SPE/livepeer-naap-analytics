#!/usr/bin/env python3
"""Run ClickHouse SQL integration assertions.

SQL files are parsed by blocks marked with:
  -- TEST: test_name

Each block must return one row and include a `failed_rows` column.
A test passes when failed_rows == 0.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover
    clickhouse_connect = None


DEFAULT_SQL_FILES = [
    "tests/integration/sql/assertions_pipeline.sql",
]


@dataclass
class SqlTest:
    name: str
    sql: str
    source_file: str


@dataclass
class TestResult:
    name: str
    source_file: str
    passed: bool
    failed_rows: int
    diagnostics: dict[str, Any]
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ClickHouse data-contract integration assertions")
    parser.add_argument("--sql-file", action="append", dest="sql_files", help="SQL file with -- TEST: blocks")
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
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help="Used when --from-ts is omitted",
    )
    parser.add_argument("--from-ts", help="UTC timestamp, e.g. 2026-02-15T00:00:00Z")
    parser.add_argument("--to-ts", help="UTC timestamp, e.g. 2026-02-16T00:00:00Z")
    parser.add_argument("--json-out", help="Optional JSON report output path")
    return parser.parse_args()


def require_client() -> None:
    if clickhouse_connect is None:
        print(
            "Missing dependency: clickhouse_connect. Run via uv project: "
            "uv run --project tools/python python scripts/run_clickhouse_data_tests.py ...",
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


def parse_sql_tests(path: Path) -> list[SqlTest]:
    # Parser contract: each `-- TEST:` marker starts a standalone query block.
    # The harness relies on stable test names and one-row outputs per block.
    tests: list[SqlTest] = []
    current_name: str | None = None
    current_lines: list[str] = []

    with path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if line.startswith("-- TEST:"):
                if current_name and current_lines:
                    tests.append(
                        SqlTest(
                            name=current_name,
                            sql="\n".join(current_lines).strip().rstrip(";"),
                            source_file=str(path),
                        )
                    )
                current_name = line.split(":", 1)[1].strip()
                current_lines = []
                continue

            if current_name is not None:
                current_lines.append(line)

    if current_name and current_lines:
        tests.append(
            SqlTest(
                name=current_name,
                sql="\n".join(current_lines).strip().rstrip(";"),
                source_file=str(path),
            )
        )

    return [t for t in tests if t.sql]


def row_to_dict(column_names: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return {column_names[i]: row[i] for i in range(len(column_names))}


def run_test(client, test: SqlTest, parameters: dict[str, Any]) -> TestResult:
    try:
        result = client.query(test.sql, parameters=parameters)
    except Exception as exc:  # pragma: no cover
        return TestResult(
            name=test.name,
            source_file=test.source_file,
            passed=False,
            failed_rows=1,
            diagnostics={},
            error=str(exc),
        )

    if not result.result_rows:
        return TestResult(
            name=test.name,
            source_file=test.source_file,
            passed=False,
            failed_rows=1,
            diagnostics={},
            error="Query returned no rows",
        )

    # Only the first row is evaluated by contract; tests must aggregate to one
    # row with `failed_rows` and optional diagnostics columns.
    first = row_to_dict(result.column_names, result.result_rows[0])
    if "failed_rows" not in first:
        return TestResult(
            name=test.name,
            source_file=test.source_file,
            passed=False,
            failed_rows=1,
            diagnostics=first,
            error="Missing required column: failed_rows",
        )

    try:
        failed_rows = int(first["failed_rows"])
    except (TypeError, ValueError):
        return TestResult(
            name=test.name,
            source_file=test.source_file,
            passed=False,
            failed_rows=1,
            diagnostics=first,
            error="failed_rows is not numeric",
        )

    diagnostics = {k: v for k, v in first.items() if k != "failed_rows"}
    return TestResult(
        name=test.name,
        source_file=test.source_file,
        passed=failed_rows == 0,
        failed_rows=failed_rows,
        diagnostics=diagnostics,
    )


def print_results(results: list[TestResult], from_ts: datetime, to_ts: datetime) -> None:
    print(f"Window UTC: {from_ts.isoformat()} -> {to_ts.isoformat()}")
    print(f"Executed tests: {len(results)}")
    print("")

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.name} ({r.source_file})")
        if r.error:
            print(f"  error: {r.error}")
        if r.failed_rows != 0:
            print(f"  failed_rows: {r.failed_rows}")
        if r.diagnostics:
            print(f"  diagnostics: {json.dumps(r.diagnostics, default=str)}")

    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    print("")
    print(f"Summary: {passed} passed, {failed} failed")


def main() -> None:
    args = parse_args()
    require_client()

    sql_files = args.sql_files or DEFAULT_SQL_FILES
    tests: list[SqlTest] = []
    for sql_file in sql_files:
        path = Path(sql_file)
        if not path.exists():
            print(f"SQL file not found: {sql_file}", file=sys.stderr)
            sys.exit(2)
        parsed = parse_sql_tests(path)
        if not parsed:
            print(f"No tests found in file: {sql_file}", file=sys.stderr)
            sys.exit(2)
        tests.extend(parsed)

    from_ts, to_ts = resolve_window(args)
    parameters = {
        "from_ts": from_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "to_ts": to_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }

    client = get_client(args)
    # Execute all tests even if one fails so the report contains full context.
    results = [run_test(client, test, parameters) for test in tests]
    print_results(results, from_ts, to_ts)

    if args.json_out:
        payload = {
            "from_ts": from_ts.isoformat(),
            "to_ts": to_ts.isoformat(),
            "results": [
                {
                    "name": r.name,
                    "source_file": r.source_file,
                    "passed": r.passed,
                    "failed_rows": r.failed_rows,
                    "diagnostics": r.diagnostics,
                    "error": r.error,
                }
                for r in results
            ],
        }
        Path(args.json_out).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    any_fail = any(not r.passed for r in results)
    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
