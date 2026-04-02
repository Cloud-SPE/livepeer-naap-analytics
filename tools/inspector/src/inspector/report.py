"""Report generation — terminal output (Rich) and schema.md file output."""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from inspector.analyzer import AnalysisResult, OrchRecord, StreamRecord

console = Console()

WEI_TO_ETH = 1e-18


def _ts(ms: int) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _eth(wei: int) -> str:
    return f"{wei * WEI_TO_ETH:.8f} ETH ({wei:,} WEI)"


def _avg(vals: list[float]) -> str:
    if not vals:
        return "—"
    return f"{statistics.mean(vals):.2f}"


def _pct(num: int, denom: int) -> str:
    if not denom:
        return "—"
    return f"{100 * num / denom:.1f}%"


# ── Terminal report ───────────────────────────────────────────────────────────

def print_report(results: list[AnalysisResult]) -> None:
    console.rule("[bold cyan]NAAP Analytics — Kafka Inspector Report[/]")
    console.print()

    for r in results:
        _print_topic_report(r)

    if len(results) > 1:
        _print_cross_topic_summary(results)


def _print_topic_report(r: AnalysisResult) -> None:
    console.rule(f"[bold yellow]Topic: {r.topic}[/]")

    # ── Overview ──────────────────────────────────────────────────────────
    t = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
    t.add_column("key", style="dim", width=30)
    t.add_column("value")
    t.add_row("Messages sampled", f"{r.total_messages:,}")
    t.add_row("Parse errors", str(r.parse_errors))
    t.add_row("Time range", f"{_ts(r.time_range_ms[0])} → {_ts(r.time_range_ms[1])}")
    t.add_row("Unique event types", str(len(r.event_type_counts)))
    t.add_row("Unique streams", str(len(r.streams)))
    t.add_row("Unique orchestrators", str(len(r.orchs)))
    console.print(Panel(t, title="Overview", border_style="blue"))

    # ── Event type distribution ────────────────────────────────────────────
    et = Table(title="Event Types", box=box.SIMPLE_HEAD)
    et.add_column("Type", style="cyan")
    et.add_column("Count", justify="right")
    et.add_column("% of total", justify="right")
    for etype, count in sorted(r.event_type_counts.items(), key=lambda x: -x[1]):
        et.add_row(etype, f"{count:,}", _pct(count, r.total_messages))
    console.print(et)

    # ── Stream trace subtypes ──────────────────────────────────────────────
    if r.stream_trace_subtypes:
        st = Table(title="Stream Trace Sub-types", box=box.SIMPLE_HEAD)
        st.add_column("Sub-type", style="cyan")
        st.add_column("Count", justify="right")
        for sub, cnt in sorted(r.stream_trace_subtypes.items(), key=lambda x: -x[1]):
            st.add_row(sub, f"{cnt:,}")
        console.print(st)

    # ── AI stream states ───────────────────────────────────────────────────
    if r.ai_stream_states:
        ss = Table(title="AI Stream States Observed", box=box.SIMPLE_HEAD)
        ss.add_column("State", style="cyan")
        ss.add_column("Count", justify="right")
        for state, cnt in sorted(r.ai_stream_states.items(), key=lambda x: -x[1]):
            ss.add_row(state, f"{cnt:,}")
        console.print(ss)

    # ── Stream summary ─────────────────────────────────────────────────────
    streams = list(r.streams.values())
    active = r.active_streams
    failed = r.failed_streams
    swapped = r.swapped_streams

    s_table = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
    s_table.add_column("key", style="dim", width=30)
    s_table.add_column("value")
    s_table.add_row("Total streams seen", str(len(streams)))
    s_table.add_row("Apparently active (no OFFLINE state)", str(len(active)))
    s_table.add_row("Had failures (restart > 0 or error)", str(len(failed)))
    s_table.add_row("Had orch swaps", str(len(swapped)))

    durations = [s.duration_sec for s in streams if s.duration_sec]
    if durations:
        s_table.add_row("Avg stream duration", f"{statistics.mean(durations):.1f}s")
        s_table.add_row("Max stream duration", f"{max(durations):.1f}s")

    inf_fps_all = [f for s in streams for f in s.inference_fps_samples]
    if inf_fps_all:
        s_table.add_row("Avg inference FPS", _avg(inf_fps_all))

    console.print(Panel(s_table, title="Streams", border_style="green"))

    # ── Top streams (by payment) ───────────────────────────────────────────
    top_streams = sorted(streams, key=lambda s: s.total_payments_wei, reverse=True)[:10]
    if any(s.total_payments_wei for s in top_streams):
        ps = Table(title="Top Streams by Payment", box=box.SIMPLE_HEAD)
        ps.add_column("Stream / Request ID", style="cyan", no_wrap=True)
        ps.add_column("Pipeline")
        ps.add_column("Orch swaps", justify="right")
        ps.add_column("Restarts", justify="right")
        ps.add_column("Inf FPS avg", justify="right")
        ps.add_column("Total paid (WEI)", justify="right")
        for s in top_streams:
            if not s.total_payments_wei:
                continue
            sid = s.stream_id or s.request_id or "unknown"
            ps.add_row(
                sid[:32],
                s.pipeline or "—",
                str(s.orch_swaps),
                str(s.restart_count),
                _avg(s.inference_fps_samples),
                f"{s.total_payments_wei:,}",
            )
        console.print(ps)

    # ── Payment summary ────────────────────────────────────────────────────
    if r.payment_count:
        pt = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
        pt.add_column("key", style="dim", width=30)
        pt.add_column("value")
        pt.add_row("Payment events", f"{r.payment_count:,}")
        pt.add_row("Total paid", _eth(r.total_payments_wei))
        pt.add_row("Unique senders (gateways)", str(len(r.unique_senders)))
        pt.add_row("Unique recipients (orchs)", str(len(r.unique_recipients)))
        console.print(Panel(pt, title="Payments", border_style="magenta"))

        if r.pipeline_payment_totals:
            ppt = Table(title="Payments by Pipeline", box=box.SIMPLE_HEAD)
            ppt.add_column("Pipeline", style="cyan")
            ppt.add_column("Total WEI", justify="right")
            ppt.add_column("ETH", justify="right")
            for pipeline, total in sorted(r.pipeline_payment_totals.items(), key=lambda x: -x[1]):
                ppt.add_row(pipeline, f"{total:,}", f"{total * WEI_TO_ETH:.8f}")
            console.print(ppt)

    # ── Orchestrators ──────────────────────────────────────────────────────
    orchs = list(r.orchs.values())
    if orchs:
        ot = Table(title="Orchestrators Observed", box=box.SIMPLE_HEAD)
        ot.add_column("Address (truncated)", style="cyan", no_wrap=True)
        ot.add_column("Name / URI")
        ot.add_column("Version")
        ot.add_column("GPUs")
        ot.add_column("GPU Model")
        ot.add_column("GPU VRAM (GB)", justify="right")
        ot.add_column("Models")
        ot.add_column("Streams", justify="right")
        ot.add_column("Total paid (WEI)", justify="right")
        for o in sorted(orchs, key=lambda x: -x.total_payments_wei):
            gpu_names = ", ".join(set(o.gpu_names)) if o.gpus else "—"
            model_list = ", ".join(m for models in o.models.values() for m in models[:2])
            ot.add_row(
                o.address[:10] + "…",
                o.name or o.uri[:30] or "—",
                o.version or "—",
                str(len(o.gpus)),
                gpu_names[:40],
                f"{o.total_gpu_memory_gb:.1f}" if o.gpus else "—",
                model_list[:40] or "—",
                str(o.streams_handled),
                f"{o.total_payments_wei:,}" if o.total_payments_wei else "—",
            )
        console.print(ot)

    # ── Network capabilities snapshot ──────────────────────────────────────
    if r.capabilities_snapshot_count:
        console.print(f"[dim]Network capabilities snapshots in sample: {r.capabilities_snapshot_count}[/]")

    # ── AI Batch jobs ──────────────────────────────────────────────────────
    if r.ai_batch_by_pipeline:
        abt = Table(title="AI Batch Jobs by Pipeline", box=box.SIMPLE_HEAD)
        abt.add_column("Pipeline", style="cyan")
        abt.add_column("Jobs", justify="right")
        abt.add_column("Success%", justify="right")
        abt.add_column("Avg duration (ms)", justify="right")
        abt.add_column("Avg latency score", justify="right")
        for pipeline, jobs in sorted(r.ai_batch_by_pipeline.items(), key=lambda x: -len(x[1])):
            total = len(jobs)
            successes = sum(1 for j in jobs if j["success"] is True)
            durations = [j["duration_ms"] for j in jobs if j["duration_ms"] is not None]
            latencies = [j["latency_score"] for j in jobs if j["latency_score"] is not None]
            abt.add_row(
                pipeline,
                str(total),
                _pct(successes, total),
                _avg(durations),
                _avg(latencies),
            )
        console.print(abt)

    # ── AI Batch LLM metrics ───────────────────────────────────────────────
    if r.ai_batch_llm_by_model:
        llmt = Table(title="AI Batch — LLM Metrics by Model", box=box.SIMPLE_HEAD)
        llmt.add_column("Model", style="cyan")
        llmt.add_column("Requests", justify="right")
        llmt.add_column("Success%", justify="right")
        llmt.add_column("Avg TPS", justify="right")
        llmt.add_column("Avg TTFT (ms)", justify="right")
        llmt.add_column("Avg tokens", justify="right")
        for model, reqs in sorted(r.ai_batch_llm_by_model.items(), key=lambda x: -len(x[1])):
            total = len(reqs)
            successes = sum(1 for req in reqs if not req["has_error"])
            tps_vals = [req["tps"] for req in reqs if req["tps"] is not None]
            ttft_vals = [req["ttft_ms"] for req in reqs if req["ttft_ms"] is not None]
            token_vals = [float(req["total_tokens"]) for req in reqs if req["total_tokens"] is not None]
            llmt.add_row(
                model,
                str(total),
                _pct(successes, total),
                _avg(tps_vals),
                _avg(ttft_vals),
                _avg(token_vals),
            )
        console.print(llmt)

    # ── BYOC jobs ──────────────────────────────────────────────────────────
    if r.byoc_jobs_by_capability:
        bct = Table(title="BYOC Jobs by Capability", box=box.SIMPLE_HEAD)
        bct.add_column("Capability", style="cyan")
        bct.add_column("Jobs", justify="right")
        bct.add_column("Success%", justify="right")
        bct.add_column("Avg duration (ms)", justify="right")
        for cap, jobs in sorted(r.byoc_jobs_by_capability.items(), key=lambda x: -len(x[1])):
            total = len(jobs)
            successes = sum(1 for j in jobs if j["success"] is True)
            durations = [j["duration_ms"] for j in jobs if j["duration_ms"] is not None]
            bct.add_row(cap, str(total), _pct(successes, total), _avg(durations))
        console.print(bct)

    # ── BYOC workers ───────────────────────────────────────────────────────
    if r.byoc_workers_by_capability:
        bwt = Table(title="BYOC Worker Inventory by Capability", box=box.SIMPLE_HEAD)
        bwt.add_column("Capability", style="cyan")
        bwt.add_column("Workers seen", justify="right")
        bwt.add_column("Models")
        bwt.add_column("Avg price/unit", justify="right")
        for cap, w in sorted(r.byoc_workers_by_capability.items(), key=lambda x: -x[1]["worker_count"]):
            prices = w["prices"]
            avg_price = _avg(prices)
            models_str = ", ".join(sorted(w["models"])) or "—"
            bwt.add_row(cap, str(w["worker_count"]), models_str, avg_price)
        console.print(bwt)

    # ── BYOC auth ──────────────────────────────────────────────────────────
    if r.byoc_auth_by_capability:
        bat = Table(title="BYOC Auth Events by Capability", box=box.SIMPLE_HEAD)
        bat.add_column("Capability", style="cyan")
        bat.add_column("Total events", justify="right")
        bat.add_column("Success%", justify="right")
        bat.add_column("Failures", justify="right")
        for cap, events in sorted(r.byoc_auth_by_capability.items(), key=lambda x: -len(x[1])):
            total = len(events)
            successes = sum(1 for e in events if e["success"] is True)
            failures = sum(1 for e in events if e["success"] is False)
            bat.add_row(cap, str(total), _pct(successes, total), str(failures))
        console.print(bat)

    console.print()


def _print_cross_topic_summary(results: list[AnalysisResult]) -> None:
    console.rule("[bold white]Cross-topic Summary[/]")

    all_orch_addrs: set[str] = set()
    total_wei = 0
    all_gpu_count = 0
    gpu_models: dict[str, int] = {}

    for r in results:
        all_orch_addrs.update(r.orchs.keys())
        total_wei += r.total_payments_wei
        for orch in r.orchs.values():
            all_gpu_count += len(orch.gpus)
            for name in orch.gpu_names:
                gpu_models[name] = gpu_models.get(name, 0) + 1

    t = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
    t.add_column("key", style="dim", width=35)
    t.add_column("value")
    t.add_row("Unique orchs across all topics", str(len(all_orch_addrs)))
    t.add_row("Total GPU count (from hw data)", str(all_gpu_count))
    t.add_row("Total payments (all topics)", _eth(total_wei))
    console.print(Panel(t, title="Network Summary", border_style="white"))

    if gpu_models:
        gt = Table(title="GPU Models on Network", box=box.SIMPLE_HEAD)
        gt.add_column("GPU Model", style="cyan")
        gt.add_column("Count", justify="right")
        for name, cnt in sorted(gpu_models.items(), key=lambda x: -x[1]):
            gt.add_row(name, str(cnt))
        console.print(gt)

    console.print()


# ── Schema.md output ──────────────────────────────────────────────────────────

def write_schema_md(results: list[AnalysisResult], output_path: Path) -> None:
    lines: list[str] = [
        "# Schema Reference",
        "",
        "> **Auto-generated by `tools/inspector`.** Do not edit manually.",
        f"> Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    for r in results:
        lines += _schema_for_topic(r)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    console.print(f"Schema written to {output_path}", style="green")


def _schema_for_topic(r: AnalysisResult) -> list[str]:
    lines = [
        f"## Topic: `{r.topic}`",
        "",
        f"Sample size: {r.total_messages:,} messages  ",
        f"Time range: {_ts(r.time_range_ms[0])} → {_ts(r.time_range_ms[1])}  ",
        f"Partitions sampled: inferred from messages",
        "",
        "### Envelope",
        "",
        "All messages share this outer envelope:",
        "",
        "```json",
        '{',
        '  "id":        "string (UUID) — unique message identifier",',
        '  "type":      "string (enum) — event type, see table below",',
        '  "timestamp": "string (epoch ms as string)",',
        '  "gateway":   "string — gateway ETH address or hostname (may be empty)",',
        '  "data":      "object|array — event-type specific payload"',
        '}',
        "```",
        "",
        "### Event types observed",
        "",
        "| Event Type | Count | % |",
        "|-----------|------:|--:|",
    ]

    total = r.total_messages or 1
    for etype, count in sorted(r.event_type_counts.items(), key=lambda x: -x[1]):
        pct = 100 * count / total
        lines.append(f"| `{etype}` | {count:,} | {pct:.1f}% |")

    lines += ["", "### Event type schemas", ""]

    for etype in sorted(r.event_type_counts.keys()):
        count = r.event_type_counts[etype]
        lines += _event_type_schema(etype, r, count)

    # Orchestrator identity
    lines += [
        "### Orchestrator identity",
        "",
        "Orchestrators are uniquely identified by their **ETH address** (`address` field).",
        "The `local_address` is the hot-wallet address and is informational.",
        "",
        "| Field | Description |",
        "|-------|-------------|",
        "| `address` | Primary key. ETH address (checksummed). |",
        "| `local_address` | Hot wallet / signing address. |",
        "| `orch_uri` | HTTPS endpoint for the orchestrator. |",
        "| `capabilities.capacities` | Map of capability_id → concurrent_capacity. |",
        "| `capabilities_prices[].capability` | Numeric capability ID (see capability map). |",
        "| `capabilities_prices[].pricePerUnit` | Price in WEI per unit. |",
        "| `hardware[].gpu_info` | Map of index → GPU details (name, memory, UUID). |",
        "",
        "### Capability ID map",
        "",
        "| ID | Name |",
        "|----|------|",
    ]

    # Add capability names if available
    known_caps = {
        "0": "H.264", "1": "MPEGTS", "2": "MP4", "3": "Fractional framerates",
        "4": "Storage direct", "5": "Storage S3", "6": "Storage GCS",
        "7": "H264 Baseline", "8": "H264 Main", "9": "H264 High",
        "10": "H264 Constrained High", "11": "GOP", "12": "Auth token",
        "14": "MPEG7 signature", "15": "HEVC decode", "16": "HEVC encode",
        "17": "VP8 decode", "18": "VP9 decode", "19": "VP8 encode", "20": "VP9 encode",
        "26": "Segment slicing", "27": "Text to image", "28": "Image to image",
        "29": "Image to video", "30": "Upscale", "31": "Audio to text",
        "32": "Segment anything 2", "33": "LLM", "34": "Image to text",
        "35": "Live video to video", "36": "Text to speech",
    }
    for cap_id, name in sorted(known_caps.items(), key=lambda x: int(x[0])):
        lines.append(f"| {cap_id} | {name} |")

    lines += [
        "",
        "### Composite keys",
        "",
        "| Concept | Primary key | Secondary keys |",
        "|---------|------------|----------------|",
        "| Stream lifecycle | `stream_id` | `request_id`, `session_id` |",
        "| Orchestrator | `address` (ETH) | `local_address`, `orch_uri` |",
        "| Payment | `sessionID` + `requestID` | `recipient`, `sender` |",
        "| GPU | `gpu_info[n].id` (UUID) | `name`, orch `address` |",
        "",
    ]

    return lines


def _event_type_schema(etype: str, r: AnalysisResult, count: int) -> list[str]:
    lines = [f"#### `{etype}` ({count:,} events)", ""]

    # Curated descriptions for known types
    descriptions: dict[str, str] = {
        "stream_trace": "Stream lifecycle milestone. The `data.type` sub-field identifies the specific milestone.",
        "ai_stream_status": "Periodic AI inference health report. Emitted ~every 10s per active stream.",
        "create_new_payment": "Probabilistic payment ticket created for an orchestrator. Uses Livepeer's probabilistic micropayment scheme.",
        "stream_ingest_metrics": "WebRTC ingest stats (jitter, packet loss, RTT) per stream.",
        "discovery_results": "Orchestrator discovery response — list of available orchs with latency.",
        "network_capabilities": "Full network capabilities snapshot. Contains all orchestrators with GPU info, models, and prices.",
    }
    if etype in descriptions:
        lines += [descriptions[etype], ""]

    # Field inventory
    fields = r.field_inventory.get(etype, {})
    if fields:
        lines += [
            "**Observed fields:**",
            "",
            "| Field path | Types seen |",
            "|-----------|-----------|",
        ]
        for path in sorted(fields.keys()):
            types = ", ".join(sorted(fields[path]))
            lines.append(f"| `{path}` | {types} |")

    # Sub-type notes
    if etype == "stream_trace":
        lines += [
            "",
            "**`data.type` sub-types observed:**",
            "",
        ]
        for sub in sorted(r.stream_trace_subtypes.keys()):
            lines.append(f"- `{sub}`")

    if etype == "ai_stream_status":
        lines += [
            "",
            "**`data.state` values observed:**",
            "",
        ]
        for state in sorted(r.ai_stream_states.keys()):
            lines.append(f"- `{state}`")

    lines += [""]
    return lines


# ── JSON report ───────────────────────────────────────────────────────────────

def write_json_report(results: list[AnalysisResult], output_path: Path) -> None:
    """Write a machine-readable JSON summary of findings."""

    def _orch_dict(o: OrchRecord) -> dict[str, Any]:
        return {
            "address": o.address,
            "local_address": o.local_address,
            "uri": o.uri,
            "name": o.name,
            "version": o.version,
            "gpu_count": len(o.gpus),
            "gpu_names": list(set(o.gpu_names)),
            "total_gpu_vram_gb": round(o.total_gpu_memory_gb, 2),
            "models": o.models,
            "prices": o.prices,
            "streams_handled": o.streams_handled,
            "total_payments_wei": o.total_payments_wei,
            "discovery_latency_ms_avg": round(statistics.mean(o.discovery_latencies_ms), 2)
            if o.discovery_latencies_ms else None,
        }

    def _stream_dict(s: StreamRecord) -> dict[str, Any]:
        return {
            "stream_id": s.stream_id,
            "request_id": s.request_id,
            "pipeline": s.pipeline,
            "orchs_seen": s.orchs_seen,
            "orch_swaps": s.orch_swaps,
            "states_seen": s.states_seen,
            "restart_count": s.restart_count,
            "had_failure": s.had_failure,
            "last_error": s.last_error,
            "duration_sec": s.duration_sec,
            "total_payments_wei": s.total_payments_wei,
            "payment_events": s.payment_events,
            "avg_inference_fps": round(statistics.mean(s.inference_fps_samples), 2)
            if s.inference_fps_samples else None,
        }

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "topics": {},
    }

    for r in results:
        report["topics"][r.topic] = {
            "total_messages": r.total_messages,
            "parse_errors": r.parse_errors,
            "time_range": {
                "start": _ts(r.time_range_ms[0]),
                "end": _ts(r.time_range_ms[1]),
            },
            "event_type_counts": dict(r.event_type_counts),
            "streams": {k: _stream_dict(v) for k, v in r.streams.items()},
            "orchestrators": {k: _orch_dict(v) for k, v in r.orchs.items()},
            "payments": {
                "total_events": r.payment_count,
                "total_wei": r.total_payments_wei,
                "total_eth": r.total_payments_wei * WEI_TO_ETH,
                "unique_senders": len(r.unique_senders),
                "unique_recipients": len(r.unique_recipients),
                "by_pipeline": dict(r.pipeline_payment_totals),
            },
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    console.print(f"JSON report written to {output_path}", style="green")
