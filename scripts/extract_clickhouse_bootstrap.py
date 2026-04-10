#!/usr/bin/env python3
"""Regenerate docs/generated/schema.md from infra/clickhouse/bootstrap/v1.sql.

This helper intentionally keeps the checked-in bootstrap inventory synchronized
with the checked-in bootstrap SQL. Refreshing v1.sql from a live migrated
ClickHouse instance still remains a separate operator step.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
BOOTSTRAP_PATH = REPO_ROOT / "infra" / "clickhouse" / "bootstrap" / "v1.sql"
SCHEMA_DOC_PATH = REPO_ROOT / "docs" / "generated" / "schema.md"

TABLE_RE = re.compile(
    r"CREATE TABLE(?: IF NOT EXISTS)? naap\.([A-Za-z0-9_]+).*?ENGINE = ([A-Za-z0-9]+)",
    re.DOTALL,
)
MAT_VIEW_RE = re.compile(
    r"CREATE MATERIALIZED VIEW(?: IF NOT EXISTS)? naap\.([A-Za-z0-9_]+).*?TO naap\.([A-Za-z0-9_]+)",
    re.DOTALL,
)
VIEW_RE = re.compile(
    r"CREATE VIEW(?: IF NOT EXISTS)? naap\.([A-Za-z0-9_]+)",
    re.DOTALL,
)


EXCLUDED_NOTES = [
    "`schema_migrations` stays migration-path bookkeeping only.",
    "`events`, `typed_*`, and `mv_typed_*` are removed; dbt staging now parses directly from `accepted_raw_events`.",
    "`canonical_refresh_*` is removed from the supported schema surface.",
    "`canonical_session_latest_store` is removed with the hard cutover to `canonical_session_current`.",
    "`api_status_samples_store` and `api_active_stream_state_store` are not part of the supported serving spine.",
    "`raw_events` and `ignored_raw_event_diagnostics` stay in the bootstrap as supported operational compatibility views.",
    "`v_api_*` compatibility views are not part of the supported semantic contract.",
]


def parse_objects(sql_text: str) -> list[tuple[str, str]]:
    objects: list[tuple[str, str]] = []
    seen: set[str] = set()

    for match in TABLE_RE.finditer(sql_text):
        name, engine = match.groups()
        if name not in seen:
            objects.append((name, engine))
            seen.add(name)

    for match in MAT_VIEW_RE.finditer(sql_text):
        name = match.group(1)
        if name not in seen:
            objects.append((name, "MaterializedView"))
            seen.add(name)

    for match in VIEW_RE.finditer(sql_text):
        name = match.group(1)
        if name not in seen:
            objects.append((name, "View"))
            seen.add(name)

    return objects


def render_schema_doc(objects: list[tuple[str, str]]) -> str:
    lines = [
        "# Generated Schema",
        "",
        "This reference is generated from the checked-in bootstrap baseline in [`v1.sql`](../../infra/clickhouse/bootstrap/v1.sql).",
        "",
        "Regenerate this inventory with `make bootstrap-extract`. If the physical bootstrap changes, update [`v1.sql`](../../infra/clickhouse/bootstrap/v1.sql) first and rerun the generator in the same change.",
        "",
        "The documented `raw_*`, `normalized_*`, `canonical_*`, `operational_*`, `api_base_*`, and `api_*` tiers are semantic/modeling guidance. This inventory is the supported physical schema, so it also includes infrastructure/runtime objects such as `accepted_raw_events`, `ignored_raw_events`, `kafka_*`, `resolver_*`, `agg_*`, metadata tables, and change/audit tables.",
        "",
        "## Included Objects",
        "",
        "| Object | Engine |",
        "| --- | --- |",
    ]
    lines.extend(f"| `{name}` | `{engine}` |" for name, engine in objects)
    lines.extend(
        [
            "",
            "## Excluded From The V1 Bootstrap",
            "",
        ]
    )
    lines.extend(f"- {note}" for note in EXCLUDED_NOTES)
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    sql_text = BOOTSTRAP_PATH.read_text(encoding="utf-8")
    objects = parse_objects(sql_text)
    SCHEMA_DOC_PATH.write_text(render_schema_doc(objects), encoding="utf-8")
    print(f"updated {SCHEMA_DOC_PATH.relative_to(REPO_ROOT)} from {BOOTSTRAP_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
