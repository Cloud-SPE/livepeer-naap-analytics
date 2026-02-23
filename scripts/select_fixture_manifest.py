#!/usr/bin/env python3
"""Create a stable fixture manifest file for local integration runs.

The export script writes timestamped snapshots under:
  tests/integration/fixtures/prod_snapshot_<stamp>/manifest.json

This helper promotes one snapshot manifest to a stable location (by default
`tests/integration/fixtures/manifest.json`) and rewrites session file paths
as relative paths so manifests are portable across local and CI environments.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote snapshot fixture manifest to stable path")
    parser.add_argument(
        "--source",
        help="Source snapshot manifest path. If omitted, newest prod_snapshot_* manifest is used.",
    )
    parser.add_argument(
        "--output",
        default="tests/integration/fixtures/manifest.json",
        help="Stable manifest output path",
    )
    return parser.parse_args()


def find_latest_snapshot_manifest(fixtures_root: Path) -> Path:
    candidates = sorted(fixtures_root.glob("prod_snapshot_*/manifest.json"))
    if not candidates:
        raise FileNotFoundError(f"No snapshot manifests found under {fixtures_root}")
    return candidates[-1]


def relativize_session_files(
    manifest: dict, source_manifest: Path, output_manifest: Path, repo_root: Path
) -> dict:
    out = dict(manifest)
    output_base = output_manifest.parent.resolve()
    scenarios = out.get("scenarios", {})
    for scenario in scenarios.values():
        for session in scenario.get("sessions", []):
            file_path = Path(session.get("file", ""))
            if not file_path:
                continue
            if file_path.is_absolute():
                resolved = file_path.resolve()
            else:
                source_relative = (source_manifest.parent / file_path).resolve()
                repo_relative = (repo_root / file_path).resolve()
                if source_relative.exists():
                    resolved = source_relative
                elif repo_relative.exists():
                    resolved = repo_relative
                elif file_path.parts[:3] == ("tests", "integration", "fixtures"):
                    resolved = repo_relative
                else:
                    resolved = source_relative
            session["file"] = str(resolved.relative_to(output_base))
    out.setdefault("metadata", {})
    out["metadata"]["promoted_from"] = str(source_manifest.resolve().relative_to(output_base))
    return out


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    fixtures_root = (repo_root / "tests/integration/fixtures").resolve()

    source = Path(args.source).resolve() if args.source else find_latest_snapshot_manifest(fixtures_root)
    if not source.exists():
        raise FileNotFoundError(f"Source manifest not found: {source}")

    output = Path(args.output)
    if not output.is_absolute():
        output = (repo_root / output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(source.read_text(encoding="utf-8"))
    promoted = relativize_session_files(manifest, source, output, repo_root)
    output.write_text(json.dumps(promoted, indent=2), encoding="utf-8")

    print(f"Source manifest: {source}")
    print(f"Wrote stable manifest: {output}")


if __name__ == "__main__":
    main()
