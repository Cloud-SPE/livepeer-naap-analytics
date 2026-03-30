#!/usr/bin/env python3
"""Measure a Kafka/ClickHouse processing baseline and write report artifacts.

The script reads existing repository settings from `.env` where available.
Kafka UI connection details are supplied as flags because they are not part of
the repository `.env` contract.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any


UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="docs/baselines")
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--session-window-minutes", type=int, default=60)
    parser.add_argument("--status-window-minutes", type=int, default=15)
    parser.add_argument("--kafka-ui-url")
    parser.add_argument("--kafka-ui-username")
    parser.add_argument("--kafka-ui-password")
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--clickhouse-user", default="naap_admin")
    parser.add_argument("--clickhouse-password")
    parser.add_argument("--clickhouse-db", default="naap")
    parser.add_argument("--kafka-cluster", default="naap")
    parser.add_argument("--topic-network", default="network_events")
    parser.add_argument("--topic-streaming", default="streaming_events")
    parser.add_argument("--docker-compose-cmd", default="docker compose")
    return parser.parse_args()


def read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def value_or_raise(name: str, cli_value: str | None, dotenv: dict[str, str]) -> str:
    value = cli_value or dotenv.get(name)
    if not value:
        raise SystemExit(f"missing required setting: {name}")
    return value


def flag_or_raise(flag_name: str, value: str | None) -> str:
    if value:
        return value
    raise SystemExit(f"missing required flag: --{flag_name}")


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ValueError(f"unsupported datetime format: {value}")


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def json_dumps(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


def run_command(cmd: str) -> str:
    proc = subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc.stdout


class KafkaUIClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.cookies = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies)
        )

    def login(self) -> None:
        self.opener.open(f"{self.base_url}/auth").read()
        payload = urllib.parse.urlencode(
            {"username": self.username, "password": self.password}
        ).encode()
        req = urllib.request.Request(
            f"{self.base_url}/auth",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        self.opener.open(req).read()

    def get_json(self, path: str) -> Any:
        with self.opener.open(f"{self.base_url}{path}") as resp:
            return json.loads(resp.read().decode())


class ClickHouseHTTPClient:
    def __init__(self, base_url: str, user: str, password: str, database: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.database = database

    def query(self, sql: str, fmt: str = "JSON") -> Any:
        query = sql.strip()
        if not query.endswith(f"FORMAT {fmt}"):
            query = f"{query}\nFORMAT {fmt}"
        params = urllib.parse.urlencode(
            {
                "user": self.user,
                "password": self.password,
                "database": self.database,
            }
        )
        req = urllib.request.Request(
            f"{self.base_url}/?{params}",
            data=query.encode(),
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode()
        if fmt == "JSON":
            return json.loads(body)
        if fmt == "JSONEachRow":
            return [json.loads(line) for line in body.splitlines() if line.strip()]
        raise ValueError(f"unsupported format: {fmt}")


@dataclass
class SamplePoint:
    remote_network_offsets: dict[int, int]
    remote_streaming_offsets: dict[int, int]
    local_consumers: list[dict[str, Any]]
    event_counts: dict[str, Any]


def topic_offsets(topic_json: dict[str, Any]) -> dict[int, int]:
    return {
        int(partition["partition"]): int(partition["offsetMax"])
        for partition in topic_json["partitions"]
    }


def consumer_partition_offsets(
    consumers: list[dict[str, Any]], table_name: str
) -> dict[int, int]:
    offsets: dict[int, int] = {}
    for row in consumers:
        if row["table"] != table_name:
            continue
        parts = [int(x) for x in row["assignments.partition_id"]]
        values = [int(x) for x in row["assignments.current_offset"]]
        for partition_id, offset in zip(parts, values, strict=True):
            offsets[partition_id] = offset
    return offsets


def consumer_message_reads(
    consumers: list[dict[str, Any]], table_name: str
) -> int:
    return sum(int(row["num_messages_read"]) for row in consumers if row["table"] == table_name)


def sample_state(
    kafka_ui: KafkaUIClient,
    ch: ClickHouseHTTPClient,
    cluster: str,
    topic_network: str,
    topic_streaming: str,
) -> SamplePoint:
    network_topic = kafka_ui.get_json(f"/api/clusters/{cluster}/topics/{topic_network}")
    streaming_topic = kafka_ui.get_json(
        f"/api/clusters/{cluster}/topics/{topic_streaming}"
    )
    consumers = ch.query(
        """
        SELECT
            table,
            consumer_id,
            `assignments.topic`,
            `assignments.partition_id`,
            `assignments.current_offset`,
            num_messages_read,
            num_commits,
            last_poll_time,
            last_commit_time,
            `exceptions.text`
        FROM system.kafka_consumers
        """,
        fmt="JSON",
    )["data"]
    event_counts = ch.query(
        """
        SELECT
            now64(3) AS observed_at,
            count() AS total_events,
            countIf(org = 'daydream') AS daydream_events,
            countIf(org = 'cloudspe') AS cloudspe_events,
            max(event_ts) AS max_event_ts
        FROM naap.events
        """,
        fmt="JSON",
    )["data"][0]
    return SamplePoint(
        remote_network_offsets=topic_offsets(network_topic),
        remote_streaming_offsets=topic_offsets(streaming_topic),
        local_consumers=consumers,
        event_counts=event_counts,
    )


def percent(part: int | float, whole: int | float) -> float | None:
    if not whole:
        return None
    return round(100.0 * float(part) / float(whole), 2)


def rate(count: int | float, seconds: int) -> float:
    return round(float(count) / float(seconds), 2)


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * q))))
    return ordered[index]


def choose_recent_session_org(ch: ClickHouseHTTPClient, minutes: int) -> str | None:
    rows = ch.query(
        f"""
        SELECT org, count() AS c
        FROM naap.events
        WHERE event_type = 'stream_trace'
          AND event_ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY org
        ORDER BY c DESC
        """,
        fmt="JSON",
    )["data"]
    return rows[0]["org"] if rows else None


def fetch_recent_session_rows(
    ch: ClickHouseHTTPClient, org: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    trace_rows = ch.query(
        f"""
        SELECT
            canonical_session_key, org, event_ts, stream_id, request_id,
            trace_type, raw_pipeline_hint, pipeline_id, orch_raw_address, orch_url
        FROM naap.stg_stream_trace
        WHERE org = '{org}' AND event_ts >= now() - INTERVAL 24 HOUR
        """,
        fmt="JSONEachRow",
    )
    status_rows = ch.query(
        f"""
        SELECT
            canonical_session_key, org, event_ts, stream_id, request_id,
            raw_pipeline_hint, state, orch_raw_address, orch_url,
            output_fps, e2e_latency_ms, restart_count, last_error
        FROM naap.stg_ai_stream_status
        WHERE org = '{org}' AND event_ts >= now() - INTERVAL 24 HOUR
        """,
        fmt="JSONEachRow",
    )
    event_rows = ch.query(
        f"""
        SELECT
            canonical_session_key, org, event_ts, stream_id, request_id,
            raw_pipeline_hint, event_name, orch_raw_address, orch_url, message
        FROM naap.stg_ai_stream_events
        WHERE org = '{org}' AND event_ts >= now() - INTERVAL 24 HOUR
        """,
        fmt="JSONEachRow",
    )
    capability_rows = ch.query(
        """
        SELECT snapshot_ts, orch_address, orch_uri, raw_capabilities
        FROM naap.capability_snapshots
        WHERE snapshot_ts >= now() - INTERVAL 24 HOUR
        """,
        fmt="JSONEachRow",
    )
    return trace_rows, status_rows, event_rows, capability_rows


def reconstruct_recent_sessions(
    trace_rows: list[dict[str, Any]],
    status_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    capability_rows: list[dict[str, Any]],
    session_window_minutes: int,
    status_window_minutes: int,
) -> dict[str, Any]:
    for row in trace_rows:
        row["event_dt"] = parse_dt(row["event_ts"])
    for row in status_rows:
        row["event_dt"] = parse_dt(row["event_ts"])
        row["output_fps"] = float(row.get("output_fps") or 0)
        row["e2e_latency_ms"] = float(row.get("e2e_latency_ms") or 0)
        row["restart_count"] = int(row.get("restart_count") or 0)
    for row in event_rows:
        row["event_dt"] = parse_dt(row["event_ts"])
    for row in capability_rows:
        row["snapshot_dt"] = parse_dt(row["snapshot_ts"])
        row["orch_uri_norm"] = (row.get("orch_uri") or "").lower()
        try:
            raw_caps = json.loads(row.get("raw_capabilities") or "{}")
        except json.JSONDecodeError:
            raw_caps = {}
        row["hardware_count"] = len(raw_caps.get("hardware") or [])

    all_events = trace_rows + status_rows + event_rows
    if not all_events:
        return {
            "analysis_now_ts": None,
            "recent_window_minutes": session_window_minutes,
            "recent_sessions": 0,
            "attribution_buckets": {},
            "startup_outcomes": {},
            "no_orch_sessions": 0,
            "swapped_sessions": 0,
            "avg_health_signal_coverage_ratio": None,
            "avg_status_samples_per_session": None,
            "phase_session_buckets": {},
            "phase_observation_buckets": {},
            "status_recent": {},
        }

    analysis_now = max(row["event_dt"] for row in all_events if row.get("event_dt"))
    recent_start = analysis_now - timedelta(minutes=session_window_minutes)
    status_start = analysis_now - timedelta(minutes=status_window_minutes)

    recent_keys = sorted(
        {
            row["canonical_session_key"]
            for row in all_events
            if row.get("canonical_session_key") and row["event_dt"] >= recent_start
        }
    )
    recent_key_set = set(recent_keys)

    trace_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    status_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    events_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        if row["canonical_session_key"] in recent_key_set:
            trace_by[row["canonical_session_key"]].append(row)
    for row in status_rows:
        if row["canonical_session_key"] in recent_key_set:
            status_by[row["canonical_session_key"]].append(row)
    for row in event_rows:
        if row["canonical_session_key"] in recent_key_set:
            events_by[row["canonical_session_key"]].append(row)
    for mapping in (trace_by, status_by, events_by):
        for rows in mapping.values():
            rows.sort(key=lambda item: item["event_dt"])

    caps_by_addr: dict[str, list[dict[str, Any]]] = defaultdict(list)
    caps_by_uri: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in capability_rows:
        if row.get("orch_address"):
            caps_by_addr[row["orch_address"]].append(row)
        if row.get("orch_uri_norm"):
            caps_by_uri[row["orch_uri_norm"]].append(row)
    for mapping in (caps_by_addr, caps_by_uri):
        for rows in mapping.values():
            rows.sort(key=lambda item: item["snapshot_dt"])

    session_meta: dict[str, dict[str, Any]] = {}
    for key in recent_keys:
        trace = trace_by.get(key, [])
        status = status_by.get(key, [])
        events = events_by.get(key, [])
        observations = [
            row
            for row in trace + status + events
            if (row.get("orch_raw_address") or "") != "" or (row.get("orch_url") or "") != ""
        ]
        observations.sort(key=lambda item: item["event_dt"])

        observed_orch_address = ""
        observed_orch_url = ""
        for row in reversed(observations):
            if not observed_orch_address and (row.get("orch_raw_address") or "") != "":
                observed_orch_address = row["orch_raw_address"]
            if not observed_orch_url and (row.get("orch_url") or "") != "":
                observed_orch_url = row["orch_url"]
            if observed_orch_address and observed_orch_url:
                break

        observed_orch_address_count = len(
            {row["orch_raw_address"] for row in observations if (row.get("orch_raw_address") or "") != ""}
        )
        observed_orch_url_count = len(
            {(row.get("orch_url") or "").lower() for row in observations if (row.get("orch_url") or "") != ""}
        )

        last_seen = max([row["event_dt"] for row in trace + status + events], default=None)
        started = any(row.get("trace_type") == "gateway_receive_stream_request" for row in trace)
        playable_seen = any(
            row.get("trace_type") == "gateway_receive_few_processed_segments" for row in trace
        )
        no_orch = any(
            row.get("trace_type") == "gateway_no_orchestrators_available" for row in trace
        )
        swap_count = sum(1 for row in trace if row.get("trace_type") == "orchestrator_swap")
        status_sample_count = len(status)
        running_state_samples = sum(
            1
            for row in status
            if row.get("state") in ("ONLINE", "DEGRADED_INPUT", "DEGRADED_INFERENCE")
        )
        online_seen = any(row.get("state") == "ONLINE" for row in status)
        positive_output_seen = any(float(row.get("output_fps") or 0) > 0 for row in status)
        loading_only_session = status_sample_count > 0 and running_state_samples == 0 and not online_seen
        zero_output_fps_session = status_sample_count > 0 and not positive_output_seen

        candidates: list[dict[str, Any]] = []
        if last_seen is not None:
            lower_bound = last_seen - timedelta(minutes=15)
            upper_bound = last_seen + timedelta(seconds=60)
            if observed_orch_address:
                candidates.extend(
                    [
                        row
                        for row in caps_by_addr.get(observed_orch_address, [])
                        if lower_bound <= row["snapshot_dt"] <= upper_bound
                    ]
                )
            elif observed_orch_url:
                candidates.extend(
                    [
                        row
                        for row in caps_by_uri.get(observed_orch_url.lower(), [])
                        if lower_bound <= row["snapshot_dt"] <= upper_bound
                    ]
                )
        matched = max(candidates, key=lambda item: item["snapshot_dt"]) if candidates else None

        stale_candidates: list[dict[str, Any]] = []
        if observed_orch_address:
            stale_candidates = caps_by_addr.get(observed_orch_address, [])
        elif observed_orch_url:
            stale_candidates = caps_by_uri.get(observed_orch_url.lower(), [])
        stale_snapshot = max([row["snapshot_dt"] for row in stale_candidates], default=None)

        if matched and matched["hardware_count"] > 0 and max(
            observed_orch_address_count, observed_orch_url_count
        ) <= 1:
            attribution_status = "resolved"
        elif max(observed_orch_address_count, observed_orch_url_count) > 1 and swap_count == 0:
            attribution_status = "ambiguous"
        elif matched and matched["hardware_count"] == 0 and max(
            observed_orch_address_count, observed_orch_url_count
        ) <= 1:
            attribution_status = "hardware_less"
        elif not matched and stale_snapshot and (observed_orch_address or observed_orch_url):
            attribution_status = "stale"
        else:
            attribution_status = "unresolved"

        if not started:
            startup_outcome = "outside_denominator"
        elif playable_seen:
            startup_outcome = "success"
        elif no_orch:
            startup_outcome = "excused"
        else:
            startup_outcome = "unexcused"

        selection_candidates: list[datetime] = []
        selection_candidates.extend(
            [
                row["event_dt"]
                for row in trace
                if row.get("trace_type") == "gateway_send_first_ingest_segment"
                or (row.get("orch_raw_address") or "") != ""
                or (row.get("orch_url") or "") != ""
            ]
        )
        selection_candidates.extend(
            [
                row["event_dt"]
                for row in status
                if (row.get("orch_raw_address") or "") != "" or (row.get("orch_url") or "") != ""
            ]
        )
        selection_candidates.extend(
            [
                row["event_dt"]
                for row in events
                if (row.get("orch_raw_address") or "") != "" or (row.get("orch_url") or "") != ""
            ]
        )
        selection_ts = min(selection_candidates) if selection_candidates else None

        denominator = 3.0 if started else 1.0
        health_signal_coverage_ratio = (
            (status_sample_count > 0)
            + int(playable_seen)
            + int(status_sample_count > 0 and (not loading_only_session or not zero_output_fps_session))
        ) / denominator

        session_meta[key] = {
            "attribution_status": attribution_status,
            "startup_outcome": startup_outcome,
            "no_orch": no_orch,
            "swap_count": swap_count,
            "selection_ts": selection_ts,
            "status_sample_count": status_sample_count,
            "health_signal_coverage_ratio": health_signal_coverage_ratio,
        }

    bucket_counts = Counter(item["attribution_status"] for item in session_meta.values())
    startup_counts = Counter(item["startup_outcome"] for item in session_meta.values())
    phase_session = defaultdict(Counter)
    phase_observation = defaultdict(Counter)
    for key, meta in session_meta.items():
        rows = (
            [row for row in trace_by.get(key, []) if row["event_dt"] >= recent_start]
            + [row for row in status_by.get(key, []) if row["event_dt"] >= recent_start]
            + [row for row in events_by.get(key, []) if row["event_dt"] >= recent_start]
        )
        phases_seen: set[str] = set()
        for row in rows:
            phase = (
                "pre_selection"
                if meta["selection_ts"] is None or row["event_dt"] < meta["selection_ts"]
                else "post_selection"
            )
            phase_observation[phase][meta["attribution_status"]] += 1
            phases_seen.add(phase)
        for phase in phases_seen:
            phase_session[phase][meta["attribution_status"]] += 1

    recent_status = [row for row in status_rows if row["event_dt"] >= status_start]
    recent_status_sessions = {row["canonical_session_key"] for row in recent_status}
    resolved_status_sessions = {
        key
        for key in recent_status_sessions
        if session_meta.get(key, {}).get("attribution_status") == "resolved"
    }
    positive_fps = [row["output_fps"] for row in recent_status if row["output_fps"] > 0]
    positive_latency = [row["e2e_latency_ms"] for row in recent_status if row["e2e_latency_ms"] > 0]

    return {
        "analysis_now_ts": analysis_now.isoformat(),
        "recent_window_minutes": session_window_minutes,
        "recent_sessions": len(session_meta),
        "attribution_buckets": dict(bucket_counts),
        "startup_outcomes": dict(startup_counts),
        "no_orch_sessions": sum(1 for item in session_meta.values() if item["no_orch"]),
        "swapped_sessions": sum(1 for item in session_meta.values() if item["swap_count"] > 0),
        "avg_health_signal_coverage_ratio": round(
            sum(item["health_signal_coverage_ratio"] for item in session_meta.values())
            / len(session_meta),
            4,
        )
        if session_meta
        else None,
        "avg_status_samples_per_session": round(
            sum(item["status_sample_count"] for item in session_meta.values())
            / len([item for item in session_meta.values() if item["status_sample_count"] > 0]),
            2,
        )
        if any(item["status_sample_count"] > 0 for item in session_meta.values())
        else None,
        "phase_session_buckets": {
            phase: dict(counter) for phase, counter in phase_session.items()
        },
        "phase_observation_buckets": {
            phase: dict(counter) for phase, counter in phase_observation.items()
        },
        "status_recent": {
            "samples": len(recent_status),
            "sessions": len(recent_status_sessions),
            "resolved_sessions": len(resolved_status_sessions),
            "resolved_session_pct": percent(
                len(resolved_status_sessions), len(recent_status_sessions)
            ),
            "avg_output_fps": round(sum(positive_fps) / len(positive_fps), 2)
            if positive_fps
            else None,
            "p95_output_fps": round(quantile(positive_fps, 0.95), 2)
            if positive_fps
            else None,
            "avg_e2e_latency_s": round(sum(positive_latency) / len(positive_latency) / 1000.0, 3)
            if positive_latency
            else None,
            "p95_e2e_latency_s": round(quantile(positive_latency, 0.95) / 1000.0, 3)
            if positive_latency
            else None,
            "degraded_input_pct": percent(
                sum(1 for row in recent_status if row.get("state") == "DEGRADED_INPUT"),
                len(recent_status),
            ),
            "degraded_inference_pct": percent(
                sum(1 for row in recent_status if row.get("state") == "DEGRADED_INFERENCE"),
                len(recent_status),
            ),
            "error_sample_pct": percent(
                sum(
                    1
                    for row in recent_status
                    if (row.get("last_error") or "") not in ("", "null")
                ),
                len(recent_status),
            ),
        },
    }


def docker_stats(compose_cmd: str) -> dict[str, dict[str, Any]]:
    output = run_command(f"{compose_cmd} stats --no-stream --format json")
    stats: dict[str, dict[str, Any]] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        stats[row["Name"]] = row
    return stats


def docker_ps(compose_cmd: str) -> list[dict[str, Any]]:
    output = run_command(f"{compose_cmd} ps --format json")
    rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    return rows


def warehouse_last_run(compose_cmd: str) -> str | None:
    output = run_command(f"{compose_cmd} logs --tail 200 warehouse")
    for line in reversed(output.splitlines()):
        if "PASS=" in line and "ERROR=" in line:
            return line.split("|", 1)[-1].strip()
    return None


def clickhouse_metrics(ch: ClickHouseHTTPClient) -> dict[str, int]:
    rows = ch.query(
        """
        SELECT name, value
        FROM system.metrics
        WHERE name IN ('KafkaBackgroundReads', 'KafkaConsumersInUse')
        ORDER BY name
        """,
        fmt="JSON",
    )["data"]
    return {row["name"]: int(row["value"]) for row in rows}


def kafka_consumer_exceptions(consumers: list[dict[str, Any]]) -> int:
    count = 0
    for row in consumers:
        count += len(row.get("exceptions.text") or [])
    return count


def build_transport_summary(
    t0: SamplePoint,
    t1: SamplePoint,
    window_seconds: int,
) -> dict[str, Any]:
    network_local_t0 = consumer_partition_offsets(t0.local_consumers, "kafka_network_events")
    network_local_t1 = consumer_partition_offsets(t1.local_consumers, "kafka_network_events")
    streaming_local_t0 = consumer_partition_offsets(t0.local_consumers, "kafka_streaming_events")
    streaming_local_t1 = consumer_partition_offsets(t1.local_consumers, "kafka_streaming_events")

    def summarize(
        remote_t0: dict[int, int],
        remote_t1: dict[int, int],
        local_t0: dict[int, int],
        local_t1: dict[int, int],
        table_name: str,
    ) -> dict[str, Any]:
        lag_t0 = {key: remote_t0[key] - local_t0.get(key, 0) for key in sorted(remote_t0)}
        lag_t1 = {key: remote_t1[key] - local_t1.get(key, 0) for key in sorted(remote_t1)}
        consumed_delta = sum(local_t1.values()) - sum(local_t0.values())
        produced_delta = sum(remote_t1.values()) - sum(remote_t0.values())
        lag_delta = sum(lag_t1.values()) - sum(lag_t0.values())
        return {
            "table": table_name,
            "produced_delta": produced_delta,
            "produced_rate_per_sec": rate(produced_delta, window_seconds),
            "consumed_delta": consumed_delta,
            "consumed_rate_per_sec": rate(consumed_delta, window_seconds),
            "lag_total_t0": sum(lag_t0.values()),
            "lag_total_t1": sum(lag_t1.values()),
            "lag_delta": lag_delta,
            "lag_drain_rate_per_sec": rate(abs(lag_delta), window_seconds),
            "per_partition_lag_t1": lag_t1,
        }

    network = summarize(
        t0.remote_network_offsets,
        t1.remote_network_offsets,
        network_local_t0,
        network_local_t1,
        "kafka_network_events",
    )
    streaming = summarize(
        t0.remote_streaming_offsets,
        t1.remote_streaming_offsets,
        streaming_local_t0,
        streaming_local_t1,
        "kafka_streaming_events",
    )

    daydream_delta = int(t1.event_counts["daydream_events"]) - int(t0.event_counts["daydream_events"])
    total_delta = int(t1.event_counts["total_events"]) - int(t0.event_counts["total_events"])
    cloudspe_delta = int(t1.event_counts["cloudspe_events"]) - int(t0.event_counts["cloudspe_events"])
    catchup_minutes = None
    if network["lag_delta"] < 0 and network["lag_total_t1"] > 0:
        catchup_minutes = round(
            network["lag_total_t1"] / abs(network["lag_delta"]) * (window_seconds / 60.0), 1
        )

    return {
        "measured_at": {
            "lag_sample_start_utc": t0.event_counts["observed_at"],
            "lag_sample_end_utc": t1.event_counts["observed_at"],
            "t0_max_event_ts": t0.event_counts["max_event_ts"],
            "t1_max_event_ts": t1.event_counts["max_event_ts"],
        },
        "window_seconds": window_seconds,
        "network_events": {
            **network,
            "events_table_daydream_delta": daydream_delta,
            "events_table_daydream_rate_per_sec": rate(daydream_delta, window_seconds),
            "estimated_catchup_minutes": catchup_minutes,
            "consumer_num_messages_read_delta": consumer_message_reads(
                t1.local_consumers, "kafka_network_events"
            )
            - consumer_message_reads(t0.local_consumers, "kafka_network_events"),
        },
        "streaming_events": {
            **streaming,
            "consumer_num_messages_read_delta": consumer_message_reads(
                t1.local_consumers, "kafka_streaming_events"
            )
            - consumer_message_reads(t0.local_consumers, "kafka_streaming_events"),
        },
        "events_table_delta": {
            "total_events": total_delta,
            "daydream_events": daydream_delta,
            "cloudspe_events": cloudspe_delta,
        },
    }


def find_service_stats(stats: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any] | None:
    for name, row in stats.items():
        if name.endswith(suffix):
            return row
    return None


def to_float_prefix(text: str) -> float | None:
    if not text:
        return None
    token = text.split()[0].replace("%", "")
    try:
        return float(token)
    except ValueError:
        return None


def markdown_rate_table(rows: list[tuple[str, int, float]]) -> list[str]:
    out = ["| Bucket | Sessions | Rate |", "|---|---:|---:|"]
    for bucket, count, pct in rows:
        out.append(f"| `{bucket}` | {count} | {pct:.2f}% |")
    return out


def build_markdown_report(
    transport: dict[str, Any],
    session_summary: dict[str, Any],
    session_org: str | None,
    runtime: dict[str, Any],
) -> str:
    network = transport["network_events"]
    streaming = transport["streaming_events"]
    measured = transport["measured_at"]
    recent_count = session_summary["recent_sessions"]

    bucket_total = recent_count or 1
    attr_rows = [
        (bucket, count, percent(count, bucket_total) or 0.0)
        for bucket, count in sorted(
            session_summary["attribution_buckets"].items(), key=lambda item: item[1], reverse=True
        )
    ]

    def phase_rows(source: dict[str, int]) -> list[tuple[str, int, float]]:
        total = sum(source.values()) or 1
        return [
            (bucket, count, percent(count, total) or 0.0)
            for bucket, count in sorted(source.items(), key=lambda item: item[1], reverse=True)
        ]

    startup_rows = [
        (bucket, count, percent(count, bucket_total) or 0.0)
        for bucket, count in sorted(
            session_summary["startup_outcomes"].items(), key=lambda item: item[1], reverse=True
        )
    ]

    lines = [
        f"# {measured['lag_sample_end_utc'][:10]} Job Baseline",
        "",
        "Measurement window:",
        f"- Kafka lag / throughput sample: `{measured['lag_sample_start_utc']} UTC` to `{measured['lag_sample_end_utc']} UTC`",
        f"- Recent session attribution slice: last `{session_summary['recent_window_minutes']}m`, anchored at `{session_summary['analysis_now_ts']}`",
        "",
        "## Scope",
        "",
        "This baseline captures the transport plane and the recent session plane separately.",
        f"- Transport throughput reflects the current local processing rate for `{transport['network_events']['table']}`.",
        f"- Recent session attribution was measured from `{session_org}` because that was the active session org in the recent window.",
        "",
        "## Baseline",
        "",
        "### 1. `network_events` throughput",
        "",
        f"- Remote source produce rate: `{network['produced_delta']:,} / {transport['window_seconds']}s = {network['produced_rate_per_sec']:.2f} msg/s`",
        f"- Local consume rate by offset advance: `{network['consumed_delta']:,} / {transport['window_seconds']}s = {network['consumed_rate_per_sec']:.2f} msg/s`",
        f"- Local landed rows into `naap.events` for `daydream`: `{network['events_table_daydream_delta']:,} / {transport['window_seconds']}s = {network['events_table_daydream_rate_per_sec']:.2f} rows/s`",
        f"- Lag at sample start: `{network['lag_total_t0']:,}`",
        f"- Lag at sample end: `{network['lag_total_t1']:,}`",
        f"- Net lag reduction: `{abs(network['lag_delta']):,} / {transport['window_seconds']}s = {network['lag_drain_rate_per_sec']:.2f} msg/s`",
        (
            f"- Approximate catch-up ETA at the observed drain rate: `~{network['estimated_catchup_minutes']} minutes`"
            if network["estimated_catchup_minutes"] is not None
            else "- Approximate catch-up ETA at the observed drain rate: `n/a`"
        ),
        "",
        "Per-partition lag at sample end:",
        "",
        "| Partition | Lag |",
        "|---|---:|",
    ]
    for partition, lag in sorted(network["per_partition_lag_t1"].items()):
        lines.append(f"| {partition} | {lag:,} |")

    lines.extend(
        [
            "",
            "### 2. `streaming_events` control check",
            "",
            f"- Remote source produce rate: `{streaming['produced_delta']:,} / {transport['window_seconds']}s = {streaming['produced_rate_per_sec']:.2f} msg/s`",
            f"- Local consume rate: `{streaming['consumed_delta']:,} / {transport['window_seconds']}s = {streaming['consumed_rate_per_sec']:.2f} msg/s`",
            f"- Lag at sample end: `{streaming['lag_total_t1']:,}`",
            "",
            "### 3. Attribution rate by semantic bucket",
            "",
            f"Recent active sessions in the last `{session_summary['recent_window_minutes']}m`: `{recent_count}`",
            "",
            "Overall session buckets:",
            "",
            *markdown_rate_table(attr_rows),
            "",
            "Before orchestrator selection, by session:",
            "",
            *markdown_rate_table(
                phase_rows(session_summary["phase_session_buckets"].get("pre_selection", {}))
            ),
            "",
            "After orchestrator selection, by session:",
            "",
            *markdown_rate_table(
                phase_rows(session_summary["phase_session_buckets"].get("post_selection", {}))
            ),
            "",
            "Before orchestrator selection, by observation:",
            "",
            "| Bucket | Observations | Rate |",
            "|---|---:|---:|",
        ]
    )
    pre_obs = session_summary["phase_observation_buckets"].get("pre_selection", {})
    pre_obs_total = sum(pre_obs.values()) or 1
    for bucket, count in sorted(pre_obs.items(), key=lambda item: item[1], reverse=True):
        lines.append(f"| `{bucket}` | {count} | {(percent(count, pre_obs_total) or 0.0):.2f}% |")
    lines.extend(["", "After orchestrator selection, by observation:", "", "| Bucket | Observations | Rate |", "|---|---:|---:|"])
    post_obs = session_summary["phase_observation_buckets"].get("post_selection", {})
    post_obs_total = sum(post_obs.values()) or 1
    for bucket, count in sorted(post_obs.items(), key=lambda item: item[1], reverse=True):
        lines.append(f"| `{bucket}` | {count} | {(percent(count, post_obs_total) or 0.0):.2f}% |")

    status_recent = session_summary["status_recent"]
    lines.extend(
        [
            "",
            "### 4. Other key metrics",
            "",
            "Startup outcomes for recent sessions:",
            "",
            *markdown_rate_table(startup_rows),
            "",
            "Operational health:",
            "",
            f"- Sessions with `gateway_no_orchestrators_available`: `{session_summary['no_orch_sessions']} / {recent_count} = {(percent(session_summary['no_orch_sessions'], recent_count) or 0.0):.2f}%`",
            f"- Swapped sessions: `{session_summary['swapped_sessions']} / {recent_count} = {(percent(session_summary['swapped_sessions'], recent_count) or 0.0):.2f}%`",
            f"- Average health signal coverage ratio: `{session_summary['avg_health_signal_coverage_ratio']}`",
            f"- Average status samples per session with status evidence: `{session_summary['avg_status_samples_per_session']}`",
            "",
            f"Recent status quality, last `{status_recent and status_recent.get('samples') is not None and '' or ''}{status_recent_minutes_from_summary(session_summary)}m`:",
            "",
            f"- Status samples: `{status_recent.get('samples')}`",
            f"- Sessions represented: `{status_recent.get('sessions')}`",
            f"- Resolved sessions in those samples: `{status_recent.get('resolved_sessions')} / {status_recent.get('sessions')} = {(status_recent.get('resolved_session_pct') or 0.0):.2f}%`",
            f"- Average output FPS: `{status_recent.get('avg_output_fps')}`",
            f"- P95 output FPS: `{status_recent.get('p95_output_fps')}`",
            f"- Degraded input samples: `{(status_recent.get('degraded_input_pct') or 0.0):.2f}%`",
            f"- Degraded inference samples: `{(status_recent.get('degraded_inference_pct') or 0.0):.2f}%`",
            f"- Error-bearing samples: `{(status_recent.get('error_sample_pct') or 0.0):.2f}%`",
            (
                f"- Average E2E latency: `{status_recent.get('avg_e2e_latency_s')}s`, P95 `{status_recent.get('p95_e2e_latency_s')}s`"
                if status_recent.get("avg_e2e_latency_s") is not None
                else "- E2E latency: no non-zero samples present in this slice"
            ),
            "",
            "Local container / runtime pressure:",
            "",
            f"- `clickhouse`: `{runtime['clickhouse_cpu_pct']}%` CPU, `{runtime['clickhouse_mem']}` RSS",
            f"- `kafka`: `{runtime['kafka_cpu_pct']}%` CPU, `{runtime['kafka_mem']}` RSS",
            f"- `kafka-ui`: `{runtime['kafka_ui_cpu_pct']}%` CPU, `{runtime['kafka_ui_mem']}` RSS",
            f"- `api`: `{runtime['api_cpu_pct']}%` CPU, `{runtime['api_mem']}` RSS",
            f"- `warehouse`: last observed run `{runtime['warehouse_last_run']}`",
            f"- ClickHouse Kafka metrics: `KafkaBackgroundReads={runtime['kafka_background_reads']}`, `KafkaConsumersInUse={runtime['kafka_consumers_in_use']}`",
            f"- `system.kafka_consumers` exceptions: `{runtime['kafka_consumer_exceptions_observed']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def status_recent_minutes_from_summary(session_summary: dict[str, Any]) -> int:
    status_recent = session_summary.get("status_recent") or {}
    # This value is only used for markdown wording.
    if status_recent.get("samples") is None:
        return 0
    return 15


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)

    dotenv = read_dotenv(repo_root / ".env")
    kafka_ui_url = flag_or_raise("kafka-ui-url", args.kafka_ui_url)
    kafka_ui_user = flag_or_raise("kafka-ui-username", args.kafka_ui_username)
    kafka_ui_password = flag_or_raise("kafka-ui-password", args.kafka_ui_password)
    ch_password = value_or_raise(
        "CLICKHOUSE_ADMIN_PASSWORD", args.clickhouse_password, dotenv
    )

    kafka_ui = KafkaUIClient(kafka_ui_url, kafka_ui_user, kafka_ui_password)
    kafka_ui.login()
    ch = ClickHouseHTTPClient(
        args.clickhouse_url, args.clickhouse_user, ch_password, args.clickhouse_db
    )

    sample_t0 = sample_state(
        kafka_ui, ch, args.kafka_cluster, args.topic_network, args.topic_streaming
    )
    time.sleep(args.window_seconds)
    sample_t1 = sample_state(
        kafka_ui, ch, args.kafka_cluster, args.topic_network, args.topic_streaming
    )
    transport = build_transport_summary(sample_t0, sample_t1, args.window_seconds)

    session_org = choose_recent_session_org(ch, args.session_window_minutes)
    session_summary = {
        "analysis_now_ts": None,
        "recent_window_minutes": args.session_window_minutes,
        "recent_sessions": 0,
        "attribution_buckets": {},
        "startup_outcomes": {},
        "no_orch_sessions": 0,
        "swapped_sessions": 0,
        "avg_health_signal_coverage_ratio": None,
        "avg_status_samples_per_session": None,
        "phase_session_buckets": {},
        "phase_observation_buckets": {},
        "status_recent": {},
    }
    if session_org:
        trace_rows, status_rows, event_rows, capability_rows = fetch_recent_session_rows(
            ch, session_org
        )
        session_summary = reconstruct_recent_sessions(
            trace_rows,
            status_rows,
            event_rows,
            capability_rows,
            args.session_window_minutes,
            args.status_window_minutes,
        )

    stats = docker_stats(args.docker_compose_cmd)
    metrics = clickhouse_metrics(ch)
    runtime = {
        "clickhouse_cpu_pct": to_float_prefix(
            (find_service_stats(stats, "clickhouse-1") or {}).get("CPUPerc", "")
        ),
        "clickhouse_mem": (find_service_stats(stats, "clickhouse-1") or {}).get(
            "MemUsage"
        ),
        "kafka_cpu_pct": to_float_prefix(
            (find_service_stats(stats, "kafka-1") or {}).get("CPUPerc", "")
        ),
        "kafka_mem": (find_service_stats(stats, "kafka-1") or {}).get("MemUsage"),
        "kafka_ui_cpu_pct": to_float_prefix(
            (find_service_stats(stats, "kafka-ui-1") or {}).get("CPUPerc", "")
        ),
        "kafka_ui_mem": (find_service_stats(stats, "kafka-ui-1") or {}).get("MemUsage"),
        "api_cpu_pct": to_float_prefix(
            (find_service_stats(stats, "api-1") or {}).get("CPUPerc", "")
        ),
        "api_mem": (find_service_stats(stats, "api-1") or {}).get("MemUsage"),
        "warehouse_last_run": warehouse_last_run(args.docker_compose_cmd),
        "kafka_background_reads": metrics.get("KafkaBackgroundReads"),
        "kafka_consumers_in_use": metrics.get("KafkaConsumersInUse"),
        "kafka_consumer_exceptions_observed": kafka_consumer_exceptions(
            sample_t1.local_consumers
        ),
    }

    measured_date = transport["measured_at"]["lag_sample_end_utc"][:10]
    output_dir = repo_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{measured_date}-job-baseline.json"
    md_path = output_dir / f"{measured_date}-job-baseline.md"

    json_payload = {
        "measured_at": {
            **transport["measured_at"],
            "session_analysis_anchor_utc": session_summary["analysis_now_ts"],
        },
        "service_config": {
            "kafka_broker_list": dotenv.get("KAFKA_BROKER_LIST"),
            "kafka_network_group": dotenv.get("KAFKA_NETWORK_GROUP"),
            "kafka_streaming_group": dotenv.get("KAFKA_STREAMING_GROUP"),
            "clickhouse_url": args.clickhouse_url,
            "clickhouse_user": args.clickhouse_user,
            "kafka_ui_url": kafka_ui_url,
            "kafka_cluster": args.kafka_cluster,
        },
        "notes": [
            "network_events was in backlog catch-up during the measurement window"
            if transport["network_events"]["lag_delta"] < 0
            else "network_events was not reducing lag during the measurement window",
            f"recent session attribution was measured from {session_org} traffic"
            if session_org
            else "no recent session activity was found in the configured window",
        ],
        "network_events": transport["network_events"],
        "streaming_events": transport["streaming_events"],
        f"recent_sessions_{session_org}_{args.session_window_minutes}m" if session_org else "recent_sessions": session_summary,
        f"status_{args.status_window_minutes}m_{session_org}" if session_org else "status_recent": session_summary.get("status_recent", {}),
        "local_runtime": runtime,
    }
    md_payload = build_markdown_report(transport, session_summary, session_org, runtime)

    json_path.write_text(json_dumps(json_payload) + "\n")
    md_path.write_text(md_payload)

    print(f"wrote {md_path}")
    print(f"wrote {json_path}")


if __name__ == "__main__":
    main()
