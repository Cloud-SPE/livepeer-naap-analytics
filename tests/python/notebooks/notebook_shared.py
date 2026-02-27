from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import pandas as pd


def resolve_repo_root(start: Path | None = None) -> Path:
    env_root = os.getenv("REPO_ROOT", "").strip()
    if env_root:
        p = Path(env_root).expanduser().resolve()
        if (p / "tests" / "integration" / "sql").is_dir():
            return p

    base = (start or Path.cwd()).resolve()
    for candidate in [base, *base.parents]:
        if (candidate / "tests" / "integration" / "sql").is_dir():
            return candidate
    raise FileNotFoundError("Could not locate repo root containing tests/integration/sql")


def resolve_sql_dir(start: Path | None = None) -> Path:
    return resolve_repo_root(start) / "tests" / "integration" / "sql"


def parse_utc(ts: str) -> datetime:
    normalized = ts.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def resolve_validation_window(
    repo_root: Path,
    lookback_hours: int,
    env_from_var: str = "FIXTURE_FROM_TS",
    env_to_var: str = "FIXTURE_TO_TS",
    manifest_relpath: str = "tests/integration/fixtures/manifest.json",
) -> tuple[datetime, datetime, str]:
    env_from = os.getenv(env_from_var, "").strip()
    env_to = os.getenv(env_to_var, "").strip()
    if env_from and env_to:
        return parse_utc(env_from), parse_utc(env_to), f"env {env_from_var}/{env_to_var}"

    manifest_path = Path(manifest_relpath)
    if not manifest_path.is_absolute():
        manifest_path = (repo_root / manifest_path).resolve()
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            window = payload.get("window", {})
            from_ts = window.get("from_ts")
            to_ts = window.get("to_ts")
            if from_ts and to_ts:
                return parse_utc(from_ts), parse_utc(to_ts), f"manifest {manifest_path}"
        except Exception as exc:
            print(f"WARNING: Failed reading fixture manifest window: {exc}")

    to_ts = datetime.now(timezone.utc)
    from_ts = to_ts - timedelta(hours=lookback_hours)
    return from_ts, to_ts, f"lookback {lookback_hours}h"


def style_status_table(df: pd.DataFrame, status_col: str = "status"):
    if status_col not in df.columns:
        return df

    palette = {
        "PASS": "#dcfce7",
        "FAIL": "#fee2e2",
        "WARN": "#fef9c3",
        "INFO": "#e0f2fe",
    }

    def _color_status(col: pd.Series):
        return [f"background-color: {palette.get(str(v), '#ffffff')}" for v in col]

    return df.style.apply(_color_status, subset=[status_col])


def parse_blocks(path: str | Path, marker: str) -> list[tuple[str, str]]:
    tests: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []

    with Path(path).open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if line.startswith(marker):
                if current_name and current_lines:
                    sql = "\n".join(current_lines).strip().rstrip(";")
                    if sql:
                        tests.append((current_name, sql))
                current_name = line.split(":", 1)[1].strip()
                current_lines = []
                continue

            if current_name is not None:
                current_lines.append(line)

    if current_name and current_lines:
        sql = "\n".join(current_lines).strip().rstrip(";")
        if sql:
            tests.append((current_name, sql))

    return tests


def run_assertion_file(
    path: str | Path,
    params: dict,
    query_fn: Callable[[str, dict], pd.DataFrame],
) -> pd.DataFrame:
    rows = []
    tests = parse_blocks(path, "-- TEST:")

    for name, sql in tests:
        try:
            df = query_fn(sql, params)
        except Exception as exc:
            rows.append({"test_name": name, "failed_rows": 1, "status": "FAIL", "error": str(exc)})
            continue

        if df.empty:
            rows.append({"test_name": name, "failed_rows": 1, "status": "FAIL", "error": "No rows"})
            continue

        row = df.iloc[0].to_dict()
        failed = int(row.get("failed_rows", 1))
        row["test_name"] = name
        row["status"] = "PASS" if failed == 0 else "FAIL"
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    cols = ["test_name", "status", "failed_rows"] + [
        c for c in out.columns if c not in {"test_name", "status", "failed_rows"}
    ]
    return out[cols]
