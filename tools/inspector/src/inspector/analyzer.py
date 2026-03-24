"""Analytics — derives insights from sampled Kafka messages.

Produces structured findings used by both the terminal report and schema.md.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from inspector.consumer import RawMessage


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract(msg: dict[str, Any], *keys: str) -> Any:
    """Safely walk a nested dict."""
    cur: Any = msg
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _parse_wei(value: str | None) -> int | None:
    """Parse a WEI string like '2402400000000000 WEI' → int."""
    if not value:
        return None
    m = re.match(r"(\d+)", str(value).strip())
    return int(m.group(1)) if m else None


def _parse_wei_per_pixel(value: str | None) -> float | None:
    """Parse '1635.021 wei/pixel' → float."""
    if not value:
        return None
    m = re.match(r"([\d.]+)", str(value).strip())
    return float(m.group(1)) if m else None


def _norm_addr(addr: str | None) -> str:
    return (addr or "").lower().strip()


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class StreamRecord:
    stream_id: str
    request_id: str
    session_ids: set[str] = field(default_factory=set)
    pipeline: str = ""
    gateway: str = ""
    orchs_seen: list[str] = field(default_factory=list)  # ordered, may repeat = swap
    states_seen: list[str] = field(default_factory=list)
    restart_count: int = 0
    last_error: str | None = None
    first_ts: int = 0
    last_ts: int = 0
    total_payments_wei: int = 0
    payment_events: int = 0
    price_wei_per_pixel: float | None = None
    inference_fps_samples: list[float] = field(default_factory=list)
    input_fps_samples: list[float] = field(default_factory=list)
    video_packets_lost: int = 0
    conn_quality_samples: list[str] = field(default_factory=list)

    @property
    def orch_swaps(self) -> int:
        """Count orch changes within this stream (swap = new orch after first)."""
        if not self.orchs_seen:
            return 0
        changes = 0
        prev = self.orchs_seen[0]
        for addr in self.orchs_seen[1:]:
            if addr and addr != prev:
                changes += 1
                prev = addr
        return changes

    @property
    def had_failure(self) -> bool:
        return self.restart_count > 0 or bool(self.last_error)

    @property
    def duration_sec(self) -> float | None:
        if self.first_ts and self.last_ts:
            return (self.last_ts - self.first_ts) / 1000.0
        return None


@dataclass
class OrchRecord:
    address: str
    local_address: str = ""
    uri: str = ""
    name: str = ""          # derived from URI hostname
    version: str = ""
    capabilities: list[int] = field(default_factory=list)
    models: dict[str, list[str]] = field(default_factory=dict)  # capability → [model_ids]
    gpus: list[dict[str, Any]] = field(default_factory=list)
    prices: list[dict[str, Any]] = field(default_factory=list)
    streams_handled: int = 0
    total_payments_wei: int = 0
    discovery_latencies_ms: list[float] = field(default_factory=list)

    @property
    def total_gpu_memory_gb(self) -> float:
        return sum(
            g.get("memory_total", 0) / (1024 ** 3)
            for g in self.gpus
        )

    @property
    def gpu_names(self) -> list[str]:
        return [g.get("name", "unknown") for g in self.gpus]


@dataclass
class AnalysisResult:
    topic: str
    total_messages: int = 0
    parse_errors: int = 0
    time_range_ms: tuple[int, int] = (0, 0)

    event_type_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    stream_trace_subtypes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    ai_stream_states: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    streams: dict[str, StreamRecord] = field(default_factory=dict)
    orchs: dict[str, OrchRecord] = field(default_factory=dict)

    # Payment summary
    total_payments_wei: int = 0
    payment_count: int = 0
    unique_senders: set[str] = field(default_factory=set)
    unique_recipients: set[str] = field(default_factory=set)
    pipeline_payment_totals: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Network capabilities snapshot
    capabilities_snapshot_count: int = 0
    known_capability_names: dict[str, str] = field(default_factory=dict)

    # Field inventory for schema.md
    field_inventory: dict[str, dict[str, set[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )

    @property
    def active_streams(self) -> list[StreamRecord]:
        """Streams that appear to be active (no terminal state, recent events)."""
        return [s for s in self.streams.values() if "OFFLINE" not in s.states_seen]

    @property
    def failed_streams(self) -> list[StreamRecord]:
        return [s for s in self.streams.values() if s.had_failure]

    @property
    def swapped_streams(self) -> list[StreamRecord]:
        return [s for s in self.streams.values() if s.orch_swaps > 0]


# ── Field inventory ──────────────────────────────────────────────────────────

def _walk_fields(obj: Any, path: str, inventory: dict[str, set[str]], max_depth: int = 6) -> None:
    """Recursively catalogue field paths and value types."""
    if max_depth <= 0:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            child = f"{path}.{k}"
            inventory[child].add(type(v).__name__ if not isinstance(v, (dict, list)) else ("object" if isinstance(v, dict) else "array"))
            _walk_fields(v, child, inventory, max_depth - 1)
    elif isinstance(obj, list) and obj:
        _walk_fields(obj[0], f"{path}[]", inventory, max_depth - 1)


# ── Main analyzer ─────────────────────────────────────────────────────────────

def analyze(topic: str, messages: list[RawMessage], parse_errors: int = 0) -> AnalysisResult:
    result = AnalysisResult(topic=topic, total_messages=len(messages), parse_errors=parse_errors)

    timestamps = [m.timestamp_ms for m in messages if m.timestamp_ms]
    if timestamps:
        result.time_range_ms = (min(timestamps), max(timestamps))

    for msg in messages:
        v = msg.value
        event_type = v.get("type", "unknown")
        result.event_type_counts[event_type] += 1

        # Field inventory
        _walk_fields(v, event_type, result.field_inventory[event_type])

        ts = int(v.get("timestamp", msg.timestamp_ms) or 0)
        # Strip string timestamps
        if isinstance(v.get("timestamp"), str):
            try:
                ts = int(v["timestamp"])
            except (ValueError, TypeError):
                ts = msg.timestamp_ms

        data = v.get("data", {})

        # ── Dispatch by event type ────────────────────────────────────────

        if event_type == "stream_trace":
            _handle_stream_trace(v, data, ts, result)

        elif event_type == "ai_stream_status":
            _handle_ai_stream_status(v, data, ts, result)

        elif event_type == "create_new_payment":
            _handle_payment(v, data, ts, result)

        elif event_type == "stream_ingest_metrics":
            _handle_ingest_metrics(v, data, ts, result)

        elif event_type == "discovery_results":
            _handle_discovery(v, data, ts, result)

        elif event_type in ("network_capabilities", "get_network_capabilities"):
            _handle_network_capabilities(v, data, ts, result)

        elif event_type == "capabilities_update":
            _handle_network_capabilities(v, data, ts, result)

    return result


def _get_or_create_stream(result: AnalysisResult, stream_id: str, request_id: str) -> StreamRecord:
    key = stream_id or request_id or "unknown"
    if key not in result.streams:
        result.streams[key] = StreamRecord(stream_id=stream_id or "", request_id=request_id or "")
    s = result.streams[key]
    if request_id:
        s.request_id = request_id
    return s


def _get_or_create_orch(result: AnalysisResult, address: str, uri: str = "") -> OrchRecord:
    key = _norm_addr(address)
    if not key:
        return OrchRecord(address="unknown")
    if key not in result.orchs:
        hostname = ""
        if uri:
            m = re.search(r"https?://([^:/]+)", uri)
            hostname = m.group(1) if m else uri
        result.orchs[key] = OrchRecord(address=key, uri=uri, name=hostname)
    o = result.orchs[key]
    if uri and not o.uri:
        o.uri = uri
    return o


def _handle_stream_trace(v: dict, data: dict, ts: int, result: AnalysisResult) -> None:
    sub_type = data.get("type", "")
    result.stream_trace_subtypes[sub_type] += 1

    stream_id = data.get("stream_id", "")
    request_id = data.get("request_id", "")
    s = _get_or_create_stream(result, stream_id, request_id)

    if not s.first_ts or ts < s.first_ts:
        s.first_ts = ts
    if ts > s.last_ts:
        s.last_ts = ts

    orch_info = data.get("orchestrator_info", {})
    orch_addr = _norm_addr(orch_info.get("address"))
    orch_uri = orch_info.get("url", "")
    if orch_addr:
        if not s.orchs_seen or s.orchs_seen[-1] != orch_addr:
            s.orchs_seen.append(orch_addr)
        _get_or_create_orch(result, orch_addr, orch_uri).streams_handled += (
            1 if sub_type == "gateway_receive_stream_request" else 0
        )

    gateway = v.get("gateway", "")
    if gateway and not s.gateway:
        s.gateway = gateway


def _handle_ai_stream_status(v: dict, data: dict, ts: int, result: AnalysisResult) -> None:
    stream_id = data.get("stream_id", "")
    request_id = data.get("request_id", "")
    s = _get_or_create_stream(result, stream_id, request_id)

    if ts > s.last_ts:
        s.last_ts = ts

    state = data.get("state", "")
    if state:
        result.ai_stream_states[state] += 1
        if not s.states_seen or s.states_seen[-1] != state:
            s.states_seen.append(state)

    pipeline = data.get("pipeline", "")
    if pipeline and not s.pipeline:
        s.pipeline = pipeline

    inf = data.get("inference_status", {})
    if isinstance(inf, dict):
        fps = inf.get("fps")
        if fps is not None:
            s.inference_fps_samples.append(float(fps))
        restart = inf.get("restart_count")
        if restart is not None:
            s.restart_count = max(s.restart_count, int(restart))
        err = inf.get("last_error")
        if err:
            s.last_error = str(err)

    inp = data.get("input_status", {})
    if isinstance(inp, dict):
        fps = inp.get("fps")
        if fps is not None:
            s.input_fps_samples.append(float(fps))

    orch_info = data.get("orchestrator_info", {})
    orch_addr = _norm_addr(orch_info.get("address"))
    if orch_addr:
        if not s.orchs_seen or s.orchs_seen[-1] != orch_addr:
            s.orchs_seen.append(orch_addr)


def _handle_payment(v: dict, data: dict, ts: int, result: AnalysisResult) -> None:
    face_value = _parse_wei(data.get("faceValue"))
    price = _parse_wei_per_pixel(data.get("price"))
    session_id = data.get("sessionID", "")
    request_id = data.get("requestID", "")
    manifest_id = data.get("manifestID", "")
    recipient = _norm_addr(data.get("recipient"))
    sender = _norm_addr(data.get("sender"))
    orch_url = data.get("orchestrator", "")

    result.payment_count += 1
    if face_value:
        result.total_payments_wei += face_value
    if sender:
        result.unique_senders.add(sender)
    if recipient:
        result.unique_recipients.add(recipient)

    # Attribute to pipeline via manifestID prefix (e.g. "35_streamdiffusion-sdxl-faceid")
    pipeline_key = manifest_id.split("_", 1)[-1] if "_" in manifest_id else manifest_id
    if pipeline_key and face_value:
        result.pipeline_payment_totals[pipeline_key] += face_value

    # Attribute to stream
    stream_id = ""  # payments don't always have stream_id directly
    s = _get_or_create_stream(result, stream_id, request_id or session_id)
    if session_id:
        s.session_ids.add(session_id)
    if face_value:
        s.total_payments_wei += face_value
        s.payment_events += 1
    if price is not None and s.price_wei_per_pixel is None:
        s.price_wei_per_pixel = price

    if recipient:
        orch = _get_or_create_orch(result, recipient, orch_url)
        if face_value:
            orch.total_payments_wei += face_value


def _handle_ingest_metrics(v: dict, data: dict, ts: int, result: AnalysisResult) -> None:
    stream_id = data.get("stream_id", "")
    request_id = data.get("request_id", "")
    s = _get_or_create_stream(result, stream_id, request_id)

    if ts > s.last_ts:
        s.last_ts = ts

    stats = data.get("stats", {})
    if not isinstance(stats, dict):
        return

    quality = stats.get("conn_quality")
    if quality:
        s.conn_quality_samples.append(quality)

    for track in stats.get("track_stats", []):
        if isinstance(track, dict) and track.get("type") == "video":
            lost = track.get("packets_lost", 0)
            if lost:
                s.video_packets_lost += int(lost)


def _handle_discovery(v: dict, data: Any, ts: int, result: AnalysisResult) -> None:
    if not isinstance(data, list):
        return
    for entry in data:
        if not isinstance(entry, dict):
            continue
        addr = _norm_addr(entry.get("address"))
        uri = entry.get("url", "")
        latency = entry.get("latency_ms")
        if addr:
            orch = _get_or_create_orch(result, addr, uri)
            if latency is not None:
                try:
                    orch.discovery_latencies_ms.append(float(latency))
                except (ValueError, TypeError):
                    pass


def _handle_network_capabilities(v: dict, data: Any, ts: int, result: AnalysisResult) -> None:
    result.capabilities_snapshot_count += 1
    orch_list = data if isinstance(data, list) else []

    for orch_data in orch_list:
        if not isinstance(orch_data, dict):
            continue
        addr = _norm_addr(orch_data.get("address"))
        local_addr = _norm_addr(orch_data.get("local_address"))
        uri = orch_data.get("orch_uri", "")

        orch = _get_or_create_orch(result, addr, uri)
        if local_addr:
            orch.local_address = local_addr

        caps = orch_data.get("capabilities", {})
        if isinstance(caps, dict):
            orch.version = caps.get("version", orch.version)
            capacities = caps.get("capacities", {})
            orch.capabilities = [int(k) for k in capacities.keys()]

            # Extract per-capability model constraints
            per_cap = caps.get("constraints", {}).get("PerCapability", {})
            for cap_id, cap_data in per_cap.items():
                models = list((cap_data.get("models") or {}).keys())
                if models:
                    orch.models[cap_id] = models

        prices = orch_data.get("capabilities_prices") or []
        if prices:
            orch.prices = prices

        hardware = orch_data.get("hardware") or []
        for hw in hardware:
            if not isinstance(hw, dict):
                continue
            gpu_info = hw.get("gpu_info", {})
            if isinstance(gpu_info, dict):
                for gpu in gpu_info.values():
                    if isinstance(gpu, dict) and gpu not in orch.gpus:
                        orch.gpus.append(gpu)
