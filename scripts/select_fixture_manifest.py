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
from datetime import datetime, timedelta, timezone


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
    parser.add_argument(
        "--manifest-window-padding-minutes",
        type=int,
        default=60,
        help="Padding added around derived fixture session window in promoted manifest.",
    )
    parser.add_argument(
        "--enforce-capability-overlap",
        action="store_true",
        help="Drop sessions that fail typed/capability wallet overlap validation.",
    )
    return parser.parse_args()


def _parse_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _derive_window_from_sessions(manifest: dict, padding_minutes: int) -> tuple[str, str] | None:
    starts: list[datetime] = []
    ends: list[datetime] = []
    scenarios = manifest.get("scenarios", {})
    for scenario in scenarios.values():
        for session in scenario.get("sessions", []):
            start_raw = session.get("session_start_ts")
            end_raw = session.get("session_end_ts")
            if not start_raw:
                continue
            start = _parse_utc(start_raw)
            end = _parse_utc(end_raw) if end_raw else start
            starts.append(start)
            ends.append(end)
    if not starts:
        return None
    pad = timedelta(minutes=max(int(padding_minutes), 0))
    return (min(starts) - pad).isoformat(), (max(ends) + pad).isoformat()


def _extract_capability_event_bounds(session_file: Path) -> tuple[datetime, datetime] | None:
    starts: list[datetime] = []
    for line in session_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("__table") != "streaming_events":
            continue
        if str(row.get("type", "")).strip() != "network_capabilities":
            continue
        try:
            ts = int(str(row.get("timestamp", "0")))
        except (TypeError, ValueError):
            continue
        starts.append(datetime.fromtimestamp(ts / 1000, tz=timezone.utc))
    if not starts:
        return None
    return min(starts), max(starts)


def find_latest_snapshot_manifest(fixtures_root: Path) -> Path:
    candidates = sorted(fixtures_root.glob("prod_snapshot_*/manifest.json"))
    if not candidates:
        raise FileNotFoundError(f"No snapshot manifests found under {fixtures_root}")
    return candidates[-1]


def _wallet_set(rows: list[dict], table: str, field: str) -> set[str]:
    # Normalized wallet extractor used by overlap validation.
    wallets: set[str] = set()
    for row in rows:
        if row.get("__table") != table:
            continue
        raw = str(row.get(field, "")).strip().lower()
        if raw:
            wallets.add(raw)
    return wallets


def _raw_data_obj(row: dict) -> dict | None:
    data = row.get("data")
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _raw_network_cap_wallets(rows: list[dict]) -> set[str]:
    wallets: set[str] = set()
    for row in rows:
        if row.get("__table") != "streaming_events":
            continue
        if str(row.get("type", "")).strip() != "network_capabilities":
            continue
        data = row.get("data")
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if isinstance(item, str):
                try:
                    item = json.loads(item)
                except json.JSONDecodeError:
                    continue
            if not isinstance(item, dict):
                continue
            addr = str(item.get("address", "")).strip().lower()
            if addr:
                wallets.add(addr)
    return wallets


def _session_has_capability_overlap(session_file: Path) -> tuple[bool, dict]:
    # Defensive attribution guard:
    # if typed rows expose orchestrator wallets, require at least one matching
    # canonical capabilities identity row in the same fixture file.
    # Keep this strict (no fallback to local/hot wallet) so normalization
    # regressions remain visible in fixture validation outputs.
    # This catches snapshots where traces/status and capabilities come from
    # different wallet populations and would produce blank canonical attribution.
    rows: list[dict] = []
    for line in session_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    typed_wallets: set[str] = set()
    for row in rows:
        if row.get("__table") != "streaming_events":
            continue
        event_type = str(row.get("type", "")).strip()
        if event_type not in {"ai_stream_status", "stream_trace"}:
            continue
        data_obj = _raw_data_obj(row)
        if not data_obj:
            continue
        wallet = str(data_obj.get("orchestrator_address", "")).strip().lower()
        if wallet:
            typed_wallets.add(wallet)

    cap_wallets = _raw_network_cap_wallets(rows)
    overlap = sorted(typed_wallets & cap_wallets)
    details = {
        "typed_wallet_count": len(typed_wallets),
        "capability_canonical_wallet_count": len(cap_wallets),
        "overlap_wallet_count": len(overlap),
        "sample_overlap_wallets": overlap[:5],
    }
    # If no typed wallets are present (e.g. no-orchestrator scenario), keep session.
    if not typed_wallets:
        return True, details
    return bool(overlap), details


def relativize_session_files(
    manifest: dict,
    source_manifest: Path,
    output_manifest: Path,
    repo_root: Path,
    padding_minutes: int,
    enforce_capability_overlap: bool,
) -> dict:
    out = dict(manifest)
    output_base = output_manifest.parent.resolve()
    scenarios = out.get("scenarios", {})
    dropped_sessions: list[dict] = []
    capability_starts: list[datetime] = []
    capability_ends: list[datetime] = []
    for scenario_name, scenario in scenarios.items():
        # Normalize and dedupe by relative fixture file path so downstream
        # replay does not publish the same JSONL twice.
        deduped_sessions = []
        seen_files: set[str] = set()
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
            rel_file = str(resolved.relative_to(output_base))
            if rel_file in seen_files:
                continue
            seen_files.add(rel_file)

            # Enforce attribution quality: sessions with typed orchestrator wallets
            # must include at least one matching canonical capabilities wallet.
            valid, details = _session_has_capability_overlap(resolved)
            if not valid:
                dropped_sessions.append(
                    {
                        "scenario": session.get("scenario_name", scenario_name),
                        "workflow_session_id": session.get("workflow_session_id", ""),
                        "file": rel_file,
                        **details,
                    }
                )
                if enforce_capability_overlap:
                    continue

            session["file"] = rel_file
            cap_bounds = _extract_capability_event_bounds(resolved)
            if cap_bounds is not None:
                cap_start, cap_end = cap_bounds
                capability_starts.append(cap_start)
                capability_ends.append(cap_end)
            deduped_sessions.append(session)
        scenario["sessions"] = deduped_sessions
    out.setdefault("metadata", {})
    out["metadata"]["promoted_from"] = str(source_manifest.resolve().relative_to(output_base))
    out["metadata"]["dropped_sessions_missing_capability_wallet_overlap"] = dropped_sessions
    out["metadata"]["capability_overlap_enforced"] = bool(enforce_capability_overlap)
    derived_window = _derive_window_from_sessions(out, padding_minutes)
    if derived_window is not None:
        from_ts, to_ts = derived_window
        if capability_starts and capability_ends:
            from_dt = min(_parse_utc(from_ts), min(capability_starts))
            to_dt = max(_parse_utc(to_ts), max(capability_ends))
            from_ts = from_dt.isoformat()
            to_ts = to_dt.isoformat()
        out["window"] = {"from_ts": from_ts, "to_ts": to_ts}
        out["metadata"]["manifest_window_padding_minutes"] = int(padding_minutes)
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
    promoted = relativize_session_files(
        manifest,
        source,
        output,
        repo_root,
        padding_minutes=args.manifest_window_padding_minutes,
        enforce_capability_overlap=args.enforce_capability_overlap,
    )
    output.write_text(json.dumps(promoted, indent=2), encoding="utf-8")

    print(f"Source manifest: {source}")
    print(f"Wrote stable manifest: {output}")
    dropped = promoted.get("metadata", {}).get("dropped_sessions_missing_capability_wallet_overlap", [])
    if dropped:
        label = (
            "Dropped sessions with typed/capability wallet mismatch"
            if args.enforce_capability_overlap
            else "Sessions with typed/capability wallet mismatch (advisory only)"
        )
        print(f"{label}: {len(dropped)}")
        for item in dropped[:10]:
            print(
                "  - "
                f"{item.get('workflow_session_id', '<unknown>')} "
                f"({item.get('scenario', '<unknown>')}) "
                f"typed_wallets={item.get('typed_wallet_count', 0)} "
                f"cap_wallets={item.get('capability_canonical_wallet_count', 0)} "
                f"overlap={item.get('overlap_wallet_count', 0)}"
            )


if __name__ == "__main__":
    main()
