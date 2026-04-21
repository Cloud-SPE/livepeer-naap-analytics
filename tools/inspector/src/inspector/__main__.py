"""Entry point for the NaaP Kafka inspector.

Usage:
    uv run naap-inspect [OPTIONS]

Options:
    --topics        Comma-separated topic names (default: network_events,streaming_events)
    --lookback      Hours to look back (default: 3)
    --max-messages  Max messages per topic (default: 10000)
    --output-dir    Directory for report output (default: reports/)
    --schema-out    Path for schema.md output (default: ../../docs/generated/schema.md)
    --no-schema     Skip writing schema.md
    --json          Also write a JSON report
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from inspector.analyzer import analyze
from inspector.consumer import BROKER, TOPICS, sample_topic
from inspector.report import print_report, write_json_report, write_schema_md

console = Console()

# Paths are resolved relative to CWD so `make inspect` (run from tools/inspector/) works.
# CWD when invoked via `make inspect` is tools/inspector/.
# The repo root is two levels up from there.
_CWD = Path.cwd()
_REPO_ROOT = _CWD.parent.parent if _CWD.name == "inspector" and _CWD.parent.name == "tools" else _CWD
_DEFAULT_SCHEMA_OUT = _REPO_ROOT / "docs" / "generated" / "schema.md"
_DEFAULT_REPORTS_DIR = _CWD / "reports"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NaaP Kafka Inspector")
    p.add_argument("--broker", default=BROKER, help=f"Kafka broker (default: {BROKER})")
    p.add_argument("--topics", default=",".join(TOPICS), help="Comma-separated topic names")
    p.add_argument("--lookback", type=float, default=3.0, help="Hours to look back")
    p.add_argument("--max-messages", type=int, default=10_000, help="Max messages per topic")
    p.add_argument("--output-dir", type=Path, default=_DEFAULT_REPORTS_DIR)
    p.add_argument("--schema-out", type=Path, default=_DEFAULT_SCHEMA_OUT)
    p.add_argument("--no-schema", action="store_true", help="Skip writing schema.md")
    p.add_argument("--json", action="store_true", help="Write a JSON report")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]

    console.print(f"[bold cyan]NaaP Kafka Inspector[/]")
    console.print(f"Broker : [yellow]{args.broker}[/]")
    console.print(f"Topics : [yellow]{', '.join(topics)}[/]")
    console.print(f"Lookback: [yellow]{args.lookback}h[/]  Max msgs: [yellow]{args.max_messages:,}[/]")
    console.print()

    results = []
    start = time.time()

    for topic in topics:
        with Progress(
            SpinnerColumn(),
            TextColumn(f"[cyan]Sampling {topic}...[/]"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            progress.add_task("", total=None)
            sample = sample_topic(
                topic=topic,
                broker=args.broker,
                lookback_hours=args.lookback,
                max_messages=args.max_messages,
            )

        console.print(
            f"[green]✓[/] {topic}: {len(sample.messages):,} messages "
            f"({sample.parse_errors} parse errors, "
            f"{sample.partitions_sampled} partitions)"
        )

        result = analyze(topic, sample.messages, parse_errors=sample.parse_errors)
        results.append(result)

    elapsed = time.time() - start
    console.print(f"\n[dim]Sampling complete in {elapsed:.1f}s[/]\n")

    if not results:
        console.print("[red]No results — check broker connectivity.[/]")
        sys.exit(1)

    print_report(results)

    # Write schema.md
    if not args.no_schema:
        write_schema_md(results, args.schema_out)

    # Write JSON report
    if args.json:
        ts = time.strftime("%Y%m%d-%H%M%S")
        json_path = args.output_dir / f"inspector-{ts}.json"
        write_json_report(results, json_path)

    console.print("\n[bold green]Done.[/]")


if __name__ == "__main__":
    main()
