#!/usr/bin/env python3
"""Validate metric feasibility from sampled ClickHouse JSONL exports.

The script is intentionally stdlib-only for easy reruns.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


DEFAULT_INPUT = "scripts/livepeer_samples.jsonl"
DEFAULT_REPORT = "documentation/reports/METRICS_VALIDATION_REPORT.md"


@dataclass
class TimeSpan:
    start: datetime | None = None
    end: datetime | None = None

    def update(self, value: datetime | None) -> None:
        if value is None:
            return
        if self.start is None or value < self.start:
            self.start = value
        if self.end is None or value > self.end:
            self.end = value

    def render(self) -> str:
        if not self.start or not self.end:
            return "n/a"
        return f"{self.start.isoformat()} to {self.end.isoformat()} ({self.end - self.start})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate metric feasibility from JSONL samples")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input JSONL file")
    parser.add_argument("--report", default=DEFAULT_REPORT, help="Output Markdown report")
    return parser.parse_args()


def is_nonempty(value: Any) -> bool:
    return value not in (None, "", [])


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        # Values may be epoch milliseconds in some exports.
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            raw = int(text)
            if raw > 10_000_000_000:
                return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    sorted_vals = sorted(values)
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    weight = pos - lo
    return sorted_vals[lo] * (1 - weight) + sorted_vals[hi] * weight


def fmt_num(value: float | None, digits: int = 3) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:.{digits}f}"


def render_field_coverage(rows: list[dict[str, Any]], fields: list[str]) -> str:
    parts = []
    for field in fields:
        present = sum(1 for r in rows if field in r)
        nonempty = sum(1 for r in rows if field in r and is_nonempty(r.get(field)))
        parts.append(f"`{field}` {nonempty}/{len(rows)} nonempty ({present}/{len(rows)} present)")
    return ", ".join(parts)


def load_rows(path: Path) -> dict[str, list[dict[str, Any]]]:
    by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            by_table[obj.get("__table", "unknown")].append(obj)
    return by_table


def metric_output_fps(rows: list[dict[str, Any]]) -> list[str]:
    fps_vals: list[float] = []
    ratio_vals: list[float] = []
    by_orch: dict[str, list[float]] = defaultdict(list)

    for row in rows:
        fps = as_float(row.get("output_fps"))
        in_fps = as_float(row.get("input_fps"))
        orch = row.get("orchestrator_address") or "__missing__"
        if fps is not None:
            fps_vals.append(fps)
            by_orch[orch].append(fps)
        if fps is not None and in_fps not in (None, 0.0):
            ratio_vals.append(fps / in_fps)

    top_orch = sorted(
        ((k, len(v), mean(v), percentile(v, 0.95)) for k, v in by_orch.items() if v),
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    lines = [
        "### Output FPS",
        f"- rows with `output_fps`: {len(fps_vals)}/{len(rows)}",
        f"- avg `output_fps`: {fmt_num(mean(fps_vals) if fps_vals else None)}",
        f"- p95 `output_fps`: {fmt_num(percentile(fps_vals, 0.95))}",
        f"- avg `output_fps / input_fps`: {fmt_num(mean(ratio_vals) if ratio_vals else None)}",
        "- top orchestrators by sample volume (count, avg_fps, p95_fps):",
    ]
    if not top_orch:
        lines.append("- n/a")
    else:
        for orch, count, avg_fps, p95_fps in top_orch:
            lines.append(f"- `{orch}`: {count}, {fmt_num(avg_fps)}, {fmt_num(p95_fps)}")
    return lines


def metric_jitter(status_rows: list[dict[str, Any]], ingest_rows: list[dict[str, Any]]) -> list[str]:
    fps_vals = [as_float(r.get("output_fps")) for r in status_rows]
    fps_vals = [v for v in fps_vals if v is not None]
    fps_jitter = None
    if fps_vals and mean(fps_vals) != 0:
        fps_jitter = pstdev(fps_vals) / mean(fps_vals)

    video_vals = [as_float(r.get("video_jitter")) for r in ingest_rows]
    audio_vals = [as_float(r.get("audio_jitter")) for r in ingest_rows]
    video_vals = [v for v in video_vals if v is not None]
    audio_vals = [v for v in audio_vals if v is not None]

    return [
        "### Jitter",
        f"- FPS jitter coefficient (`stddevPop(output_fps)/avg(output_fps)`): {fmt_num(fps_jitter)}",
        f"- avg network `video_jitter`: {fmt_num(mean(video_vals) if video_vals else None)}",
        f"- avg network `audio_jitter`: {fmt_num(mean(audio_vals) if audio_vals else None)}",
        "- note: FPS jitter and network jitter are different KPIs and should be reported separately.",
    ]


def metric_bandwidth(ingest_rows: list[dict[str, Any]]) -> list[str]:
    by_stream: dict[str, list[tuple[datetime, float, float]]] = defaultdict(list)
    for row in ingest_rows:
        key = row.get("stream_id") or row.get("request_id") or "__missing__"
        ts = parse_ts(row.get("event_timestamp"))
        br = as_float(row.get("bytes_received"))
        bs = as_float(row.get("bytes_sent"))
        if ts is None or br is None or bs is None:
            continue
        by_stream[key].append((ts, br, bs))

    down_mbps: list[float] = []
    up_mbps: list[float] = []
    negative_delta = 0
    valid_pairs = 0

    for points in by_stream.values():
        points.sort(key=lambda x: x[0])
        for i in range(1, len(points)):
            prev = points[i - 1]
            curr = points[i]
            dt = (curr[0] - prev[0]).total_seconds()
            if dt <= 0:
                continue
            dbr = curr[1] - prev[1]
            dbs = curr[2] - prev[2]
            if dbr < 0 or dbs < 0:
                negative_delta += 1
                continue
            valid_pairs += 1
            down_mbps.append((dbr * 8.0) / dt / 1_000_000.0)
            up_mbps.append((dbs * 8.0) / dt / 1_000_000.0)

    return [
        "### Up/Down Bandwidth (Inferred)",
        f"- streams sampled: {len(by_stream)}",
        f"- valid delta pairs: {valid_pairs}",
        f"- negative delta pairs skipped: {negative_delta}",
        f"- avg down Mbps: {fmt_num(mean(down_mbps) if down_mbps else None)}",
        f"- p95 down Mbps: {fmt_num(percentile(down_mbps, 0.95))}",
        f"- avg up Mbps: {fmt_num(mean(up_mbps) if up_mbps else None)}",
        f"- p95 up Mbps: {fmt_num(percentile(up_mbps, 0.95))}",
        "- formula: `((delta_bytes * 8) / delta_seconds) / 1_000_000`.",
    ]


def metric_trace_latency(trace_rows: list[dict[str, Any]], status_rows: list[dict[str, Any]]) -> list[str]:
    by_req_trace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        req = row.get("request_id")
        if is_nonempty(req):
            by_req_trace[str(req)].append(row)

    by_req_start_time: dict[str, datetime] = {}
    for row in status_rows:
        req = row.get("request_id")
        st = parse_ts(row.get("start_time"))
        if not is_nonempty(req) or st is None:
            continue
        req_key = str(req)
        current = by_req_start_time.get(req_key)
        if current is None or st < current:
            by_req_start_time[req_key] = st

    startup_vals: list[float] = []
    e2e_vals: list[float] = []
    prompt_vals: list[float] = []
    startup_missing = 0
    e2e_missing = 0
    prompt_missing = 0
    negative = 0

    for req, rows in by_req_trace.items():
        rows_sorted = sorted(rows, key=lambda r: parse_ts(r.get("data_timestamp")) or parse_ts(r.get("event_timestamp")) or datetime.max.replace(tzinfo=timezone.utc))
        edges: dict[str, datetime] = {}
        for row in rows_sorted:
            t = parse_ts(row.get("data_timestamp")) or parse_ts(row.get("event_timestamp"))
            if t is None:
                continue
            edge = row.get("trace_type") or ""
            if edge and edge not in edges:
                edges[edge] = t

        # Startup proxy.
        start = edges.get("gateway_receive_stream_request")
        first_processed = edges.get("gateway_receive_first_processed_segment")
        if start and first_processed:
            value = (first_processed - start).total_seconds() * 1000.0
            if value >= 0:
                startup_vals.append(value)
            else:
                negative += 1
        else:
            startup_missing += 1

        # E2E proxy.
        sent = edges.get("gateway_send_first_ingest_segment")
        returned = edges.get("runner_send_first_processed_segment")
        if sent and returned:
            value = (returned - sent).total_seconds() * 1000.0
            if value >= 0:
                e2e_vals.append(value)
            else:
                negative += 1
        else:
            e2e_missing += 1

        # Prompt-to-playable proxy.
        few_processed = edges.get("gateway_receive_few_processed_segments")
        req_start = by_req_start_time.get(req)
        if req_start and few_processed:
            value = (few_processed - req_start).total_seconds() * 1000.0
            if value >= 0:
                prompt_vals.append(value)
            else:
                negative += 1
        else:
            prompt_missing += 1

    total_reqs = len(by_req_trace)
    return [
        "### Startup / E2E / Prompt Latency (Trace Proxies)",
        f"- trace requests analyzed: {total_reqs}",
        f"- startup proxy pairs found: {len(startup_vals)} (missing: {startup_missing})",
        f"- startup proxy avg ms: {fmt_num(mean(startup_vals) if startup_vals else None)}",
        f"- e2e proxy pairs found: {len(e2e_vals)} (missing: {e2e_missing})",
        f"- e2e proxy avg ms: {fmt_num(mean(e2e_vals) if e2e_vals else None)}",
        f"- prompt-to-playable proxy pairs found: {len(prompt_vals)} (missing: {prompt_missing})",
        f"- prompt-to-playable proxy avg ms: {fmt_num(mean(prompt_vals) if prompt_vals else None)}",
        f"- negative-latency pairs rejected: {negative}",
        "- required before finalization: lock canonical edge dictionary and authoritative timestamp field.",
    ]


def metric_failure_and_swap(
    ai_event_rows: list[dict[str, Any]],
    status_rows: list[dict[str, Any]],
    trace_rows: list[dict[str, Any]],
) -> list[str]:
    error_requests = {
        str(r.get("request_id"))
        for r in ai_event_rows
        if is_nonempty(r.get("request_id")) and str(r.get("event_type", "")).lower() == "error"
    }
    status_requests = {str(r.get("request_id")) for r in status_rows if is_nonempty(r.get("request_id"))}
    trace_requests = {str(r.get("request_id")) for r in trace_rows if is_nonempty(r.get("request_id"))}

    # State-based failures: any non-ONLINE status seen for request.
    state_fail_requests: set[str] = set()
    for r in status_rows:
        req = r.get("request_id")
        state = str(r.get("state") or "")
        if is_nonempty(req) and state and state != "ONLINE":
            state_fail_requests.add(str(req))

    # Swap proxy: request has >1 distinct orchestrator.
    orch_by_req: dict[str, set[str]] = defaultdict(set)
    for r in trace_rows:
        req = r.get("request_id")
        orch = r.get("orchestrator_address")
        if is_nonempty(req) and is_nonempty(orch):
            orch_by_req[str(req)].add(str(orch))
    requests_with_orch = {k for k, v in orch_by_req.items() if v}
    swap_requests = {k for k, v in orch_by_req.items() if len(v) > 1}

    matched_status_errors = error_requests.intersection(status_requests)
    matched_trace_errors = error_requests.intersection(trace_requests)
    failure_rate_status_den = (len(matched_status_errors) / len(status_requests)) if status_requests else None
    failure_rate_trace_den = (len(matched_trace_errors) / len(trace_requests)) if trace_requests else None
    state_failure_rate = (len(state_fail_requests) / len(status_requests)) if status_requests else None
    swap_rate = (len(swap_requests) / len(requests_with_orch)) if requests_with_orch else None

    return [
        "### Failure Rate / Swap Rate (Current Proxies)",
        f"- distinct `ai_stream_events` error requests: {len(error_requests)}",
        f"- distinct requests in `ai_stream_status`: {len(status_requests)}",
        f"- distinct requests in `stream_trace_events`: {len(trace_requests)}",
        f"- error requests overlapping status denominator: {len(matched_status_errors)}",
        f"- error requests overlapping trace denominator: {len(matched_trace_errors)}",
        f"- failure rate vs status denominator: {fmt_num(failure_rate_status_den * 100 if failure_rate_status_den is not None else None)}%",
        f"- failure rate vs trace denominator: {fmt_num(failure_rate_trace_den * 100 if failure_rate_trace_den is not None else None)}%",
        f"- state-based failure requests (non-ONLINE seen): {len(state_fail_requests)}",
        f"- state-based failure rate: {fmt_num(state_failure_rate * 100 if state_failure_rate is not None else None)}%",
        f"- requests with non-empty orchestrator in trace: {len(requests_with_orch)}",
        f"- swap requests (>1 orchestrator): {len(swap_requests)}",
        f"- swap rate proxy: {fmt_num(swap_rate * 100 if swap_rate is not None else None)}%",
        "- required before finalization: classify error taxonomy and lock denominator contract (session vs request).",
    ]


def build_report(path: Path, rows_by_table: dict[str, list[dict[str, Any]]]) -> str:
    table_order = [
        "ai_stream_status",
        "stream_ingest_metrics",
        "stream_trace_events",
        "ai_stream_events",
        "payment_events",
        "network_capabilities",
        "discovery_results",
    ]
    fields_by_table = {
        "ai_stream_status": [
            "stream_id",
            "request_id",
            "pipeline",
            "pipeline_id",
            "gateway",
            "orchestrator_address",
            "orchestrator_url",
            "output_fps",
            "input_fps",
            "start_time",
            "state",
        ],
        "stream_ingest_metrics": [
            "stream_id",
            "request_id",
            "pipeline_id",
            "connection_quality",
            "bytes_received",
            "bytes_sent",
            "video_jitter",
            "audio_jitter",
            "video_latency",
            "audio_latency",
        ],
        "stream_trace_events": [
            "stream_id",
            "request_id",
            "pipeline_id",
            "orchestrator_address",
            "orchestrator_url",
            "trace_type",
            "data_timestamp",
        ],
        "ai_stream_events": [
            "stream_id",
            "request_id",
            "pipeline",
            "pipeline_id",
            "event_type",
            "message",
            "capability",
        ],
        "payment_events": ["request_id", "session_id", "manifest_id", "sender", "recipient", "orchestrator", "capability"],
        "network_capabilities": ["orchestrator_address", "orch_uri", "gpu_id", "gpu_name", "pipeline", "model_id", "runner_version"],
        "discovery_results": ["orchestrator_address", "orchestrator_url", "latency_ms"],
    }

    lines: list[str] = []
    lines.append("# Metrics Validation Report (JSONL)")
    lines.append("")
    lines.append(f"- input file: `{path}`")
    lines.append(f"- generated at: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append("")
    lines.append("## Table Coverage")
    lines.append("")

    for table in table_order:
        rows = rows_by_table.get(table, [])
        span = TimeSpan()
        for row in rows:
            span.update(parse_ts(row.get("event_timestamp")))
        lines.append(f"### `{table}`")
        lines.append(f"- row count: {len(rows)}")
        lines.append(f"- time span: {span.render()}")
        if rows and table in fields_by_table:
            lines.append(f"- field coverage: {render_field_coverage(rows, fields_by_table[table])}")
        lines.append("")

    lines.append("## Metric Feasibility")
    lines.append("")
    lines.extend(metric_output_fps(rows_by_table.get("ai_stream_status", [])))
    lines.append("")
    lines.extend(metric_jitter(rows_by_table.get("ai_stream_status", []), rows_by_table.get("stream_ingest_metrics", [])))
    lines.append("")
    lines.extend(metric_bandwidth(rows_by_table.get("stream_ingest_metrics", [])))
    lines.append("")
    lines.extend(
        metric_trace_latency(
            rows_by_table.get("stream_trace_events", []),
            rows_by_table.get("ai_stream_status", []),
        )
    )
    lines.append("")
    lines.extend(
        metric_failure_and_swap(
            rows_by_table.get("ai_stream_events", []),
            rows_by_table.get("ai_stream_status", []),
            rows_by_table.get("stream_trace_events", []),
        )
    )
    lines.append("")
    lines.append("## Edge Mapping Gaps")
    lines.append("")
    lines.append("- Lock canonical edge pairs for startup/e2e/prompt metrics.")
    lines.append("- Decide authoritative time field for trace math: `data_timestamp` vs `event_timestamp`.")
    lines.append("- Finalize failure denominator contract (`session` or `request`).")
    lines.append("- Improve orchestrator coverage in trace to stabilize swap metric.")
    lines.append("- Populate `pipeline_id` in curated tables for inference workflow segmentation.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    out_path = Path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_by_table = load_rows(in_path)
    report = build_report(in_path, rows_by_table)
    out_path.write_text(report, encoding="utf-8")
    print(f"Wrote report: {out_path}")


if __name__ == "__main__":
    main()
