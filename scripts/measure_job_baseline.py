#!/usr/bin/env python3
"""Measure a Kafka/ClickHouse processing baseline and write report artifacts.

The script reads existing repository settings from `.env` where available.
Kafka UI connection details may also be sourced from `.env` if local operators
choose to define them there, but remain overridable via flags.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any


UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    dotenv = read_dotenv(repo_root / ".env")

    def dotenv_first(*keys: str, fallback: str | None = None) -> str | None:
        for key in keys:
            value = dotenv.get(key)
            if value:
                return value
        return fallback

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=".local/baselines")
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--session-window-minutes", type=int, default=7 * 24 * 60)
    parser.add_argument("--status-window-minutes", type=int, default=15)
    parser.add_argument(
        "--kafka-ui-url", default=dotenv_first("KAFKA_UI_URL", fallback=None)
    )
    parser.add_argument(
        "--kafka-ui-username",
        default=dotenv_first("KAFKA_UI_USERNAME", fallback=None),
    )
    parser.add_argument(
        "--kafka-ui-password",
        default=dotenv_first("KAFKA_UI_PASSWORD", fallback=None),
    )
    parser.add_argument(
        "--clickhouse-url",
        default=dotenv_first("CLICKHOUSE_HTTP_URL", fallback="http://127.0.0.1:8123"),
    )
    parser.add_argument(
        "--clickhouse-user",
        default=dotenv_first(
            "CLICKHOUSE_ADMIN_USER", "CLICKHOUSE_USER", fallback="naap_admin"
        ),
    )
    parser.add_argument(
        "--clickhouse-password",
        default=dotenv_first(
            "CLICKHOUSE_ADMIN_PASSWORD", "CLICKHOUSE_PASSWORD", fallback=None
        ),
    )
    parser.add_argument(
        "--clickhouse-db",
        default=dotenv_first("CLICKHOUSE_DATABASE", "CLICKHOUSE_DB", fallback="naap"),
    )
    parser.add_argument("--kafka-cluster", default="naap")
    parser.add_argument("--topic-network", default="network_events")
    parser.add_argument("--topic-streaming", default="streaming_events")
    parser.add_argument("--docker-compose-cmd", default="docker compose")
    parser.add_argument(
        "--api-base-url", default=dotenv_first("API_BASE_URL", fallback="http://127.0.0.1:8000")
    )
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


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def fmt_window(minutes: int) -> str:
    if minutes % (24 * 60) == 0:
        return f"{minutes // (24 * 60)}d"
    if minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}m"


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
    def __init__(self, base_url: str, username: str | None, password: str | None) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.cookies = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies)
        )

    def login(self) -> None:
        if not self.username or not self.password:
            return
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


def fetch_api_json(base_url: str, path: str) -> tuple[dict[str, Any] | list[Any], float, int]:
    url = f"{base_url.rstrip('/')}{path}"
    started = time.perf_counter()
    with urllib.request.urlopen(url) as resp:
        body = resp.read().decode()
        status = resp.status
    elapsed = round(time.perf_counter() - started, 6)
    return json.loads(body), elapsed, status


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
        FROM naap.accepted_raw_events
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


def fetch_recent_attribution_summary(
    ch: ClickHouseHTTPClient, session_window_minutes: int, status_window_minutes: int
) -> dict[str, Any]:
    overall = ch.query(
        f"""
        SELECT
            max(last_seen) AS analysis_now_ts,
            count() AS recent_sessions,
            countIf(selection_outcome = 'selected') AS selected_sessions,
            countIf(selection_outcome = 'no_orch') AS no_orch_sessions,
            countIf(selection_outcome = 'unknown') AS unknown_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'resolved') AS resolved_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'hardware_less') AS hardware_less_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'stale') AS stale_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'ambiguous') AS ambiguous_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'unresolved') AS unresolved_sessions,
            countIf(swap_count > 0) AS swapped_sessions,
            avg(health_signal_coverage_ratio) AS avg_health_signal_coverage_ratio,
            avgIf(status_sample_count, status_sample_count > 0) AS avg_status_samples_per_session
        FROM naap.canonical_session_current
        WHERE last_seen >= now() - INTERVAL {session_window_minutes} MINUTE
        """,
        fmt="JSON",
    )["data"][0]
    startup_rows = ch.query(
        f"""
        SELECT startup_outcome, count() AS sessions
        FROM naap.canonical_session_current
        WHERE last_seen >= now() - INTERVAL {session_window_minutes} MINUTE
        GROUP BY startup_outcome
        ORDER BY sessions DESC, startup_outcome
        """,
        fmt="JSONEachRow",
    )
    per_org_rows = ch.query(
        f"""
        SELECT
            org,
            count() AS recent_sessions,
            countIf(selection_outcome = 'selected') AS selected_sessions,
            countIf(selection_outcome = 'no_orch') AS no_orch_sessions,
            countIf(selection_outcome = 'unknown') AS unknown_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'resolved') AS resolved_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'hardware_less') AS hardware_less_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'stale') AS stale_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'ambiguous') AS ambiguous_sessions,
            countIf(selection_outcome = 'selected' AND attribution_status = 'unresolved') AS unresolved_sessions
        FROM naap.canonical_session_current
        WHERE last_seen >= now() - INTERVAL {session_window_minutes} MINUTE
        GROUP BY org
        ORDER BY recent_sessions DESC, org
        """,
        fmt="JSONEachRow",
    )
    status_recent = ch.query(
        f"""
        SELECT
            count() AS samples,
            uniqExact(s.canonical_session_key) AS sessions,
            uniqExactIf(s.canonical_session_key, c.attribution_status IN ('resolved', 'hardware_less')) AS orchestrated_sessions,
            avgIf(s.output_fps, s.output_fps > 0) AS avg_output_fps,
            quantileTDigestIf(0.95)(s.output_fps, s.output_fps > 0) AS p95_output_fps,
            avgIf(s.e2e_latency_ms, s.e2e_latency_ms > 0) / 1000.0 AS avg_e2e_latency_s,
            quantileTDigestIf(0.95)(s.e2e_latency_ms, s.e2e_latency_ms > 0) / 1000.0 AS p95_e2e_latency_s,
            countIf(s.state = 'DEGRADED_INPUT') AS degraded_input_samples,
            countIf(s.state = 'DEGRADED_INFERENCE') AS degraded_inference_samples,
            countIf((s.last_error != '') AND (s.last_error != 'null')) AS error_samples
        FROM naap.stg_ai_stream_status s
        LEFT JOIN naap.canonical_session_current c USING (canonical_session_key)
        WHERE s.event_ts >= now() - INTERVAL {status_window_minutes} MINUTE
        """,
        fmt="JSON",
    )["data"][0]

    selected_total = int(overall["selected_sessions"])
    resolved = int(overall["resolved_sessions"])
    hardware_less = int(overall["hardware_less_sessions"])
    orchestrated_total = resolved + hardware_less

    per_org: list[dict[str, Any]] = []
    for row in per_org_rows:
        selected = int(row["selected_sessions"])
        resolved_count = int(row["resolved_sessions"])
        hardware_less_count = int(row["hardware_less_sessions"])
        orchestrated = resolved_count + hardware_less_count
        per_org.append(
            {
                "org": row["org"],
                "recent_sessions": int(row["recent_sessions"]),
                "selected_sessions": selected,
                "no_orch_sessions": int(row["no_orch_sessions"]),
                "no_orch_pct": percent(int(row["no_orch_sessions"]), int(row["recent_sessions"])),
                "unknown_sessions": int(row["unknown_sessions"]),
                "unknown_pct": percent(int(row["unknown_sessions"]), int(row["recent_sessions"])),
                "resolved": resolved_count,
                "hardware_less": hardware_less_count,
                "orchestrator_identity": orchestrated,
                "orchestrator_identity_pct": percent(orchestrated, selected),
                "resolved_pct": percent(resolved_count, selected),
                "hardware_less_pct": percent(hardware_less_count, selected),
                "stale": int(row["stale_sessions"]),
                "stale_pct": percent(int(row["stale_sessions"]), selected),
                "ambiguous": int(row["ambiguous_sessions"]),
                "ambiguous_pct": percent(int(row["ambiguous_sessions"]), selected),
                "unresolved": int(row["unresolved_sessions"]),
                "unresolved_pct": percent(int(row["unresolved_sessions"]), selected),
            }
        )

    return {
        "analysis_now_ts": overall.get("analysis_now_ts"),
        "recent_window_minutes": session_window_minutes,
        "recent_sessions": int(overall["recent_sessions"]),
        "selection_outcomes": {
            "selected": selected_total,
            "no_orch": int(overall["no_orch_sessions"]),
            "unknown": int(overall["unknown_sessions"]),
        },
        "selected_sessions": {
            "total": selected_total,
            "resolved": resolved,
            "hardware_less": hardware_less,
            "orchestrator_identity": orchestrated_total,
            "orchestrator_identity_pct": percent(orchestrated_total, selected_total),
            "resolved_pct": percent(resolved, selected_total),
            "hardware_less_pct": percent(hardware_less, selected_total),
            "stale": int(overall["stale_sessions"]),
            "stale_pct": percent(int(overall["stale_sessions"]), selected_total),
            "ambiguous": int(overall["ambiguous_sessions"]),
            "ambiguous_pct": percent(int(overall["ambiguous_sessions"]), selected_total),
            "unresolved": int(overall["unresolved_sessions"]),
            "unresolved_pct": percent(int(overall["unresolved_sessions"]), selected_total),
        },
        "startup_outcomes": {
            row["startup_outcome"]: int(row["sessions"]) for row in startup_rows
        },
        "swapped_sessions": int(overall["swapped_sessions"]),
        "avg_health_signal_coverage_ratio": round(
            float(overall["avg_health_signal_coverage_ratio"]), 4
        )
        if overall.get("avg_health_signal_coverage_ratio") is not None
        else None,
        "avg_status_samples_per_session": round(
            float(overall["avg_status_samples_per_session"]), 2
        )
        if overall.get("avg_status_samples_per_session") is not None
        else None,
        "per_org": per_org,
        "status_recent": {
            "window_minutes": status_window_minutes,
            "samples": int(status_recent["samples"]),
            "sessions": int(status_recent["sessions"]),
            "orchestrated_sessions": int(status_recent["orchestrated_sessions"]),
            "orchestrated_session_pct": percent(
                int(status_recent["orchestrated_sessions"]), int(status_recent["sessions"])
            ),
            "avg_output_fps": round(float(status_recent["avg_output_fps"]), 2)
            if status_recent.get("avg_output_fps") is not None
            else None,
            "p95_output_fps": round(float(status_recent["p95_output_fps"]), 2)
            if status_recent.get("p95_output_fps") is not None
            else None,
            "avg_e2e_latency_s": round(float(status_recent["avg_e2e_latency_s"]), 3)
            if status_recent.get("avg_e2e_latency_s") is not None
            else None,
            "p95_e2e_latency_s": round(float(status_recent["p95_e2e_latency_s"]), 3)
            if status_recent.get("p95_e2e_latency_s") is not None
            else None,
            "degraded_input_pct": percent(
                int(status_recent["degraded_input_samples"]), int(status_recent["samples"])
            ),
            "degraded_inference_pct": percent(
                int(status_recent["degraded_inference_samples"]), int(status_recent["samples"])
            ),
            "error_sample_pct": percent(
                int(status_recent["error_samples"]), int(status_recent["samples"])
            ),
        },
    }


def sample_sla_cohorts(ch: ClickHouseHTTPClient, days: int = 7, max_cohorts: int = 3) -> list[dict[str, Any]]:
    rows = ch.query(
        f"""
        WITH recent AS (
            SELECT
                window_start,
                orchestrator_address,
                pipeline_id,
                ifNull(model_id, '') AS model_id,
                ifNull(gpu_model_name, '') AS gpu_model_name,
                requested_sessions,
                startup_success_sessions,
                output_failed_sessions,
                total_swapped_sessions,
                health_signal_count,
                health_expected_signal_count,
                avg_output_fps,
                avg_prompt_to_first_frame_ms,
                avg_e2e_latency_ms,
                reliability_score,
                quality_score,
                latency_score,
                fps_score,
                ptff_score,
                e2e_score,
                sla_score
            FROM naap.api_sla_compliance
            WHERE window_start >= now() - INTERVAL {days} DAY
              AND requested_sessions > 0
              AND ifNull(model_id, '') != ''
        ),
        ranked AS (
            SELECT
                *,
                count() OVER (PARTITION BY pipeline_id, model_id) AS cohort_rows,
                row_number() OVER (PARTITION BY pipeline_id, model_id ORDER BY sla_score ASC, window_start DESC) AS low_rank,
                row_number() OVER (PARTITION BY pipeline_id, model_id ORDER BY abs(sla_score - 50) ASC, window_start DESC) AS medium_rank,
                row_number() OVER (PARTITION BY pipeline_id, model_id ORDER BY sla_score DESC, window_start DESC) AS high_rank
            FROM recent
        ),
        largest_cohorts AS (
            SELECT pipeline_id, model_id
            FROM ranked
            GROUP BY pipeline_id, model_id, cohort_rows
            ORDER BY cohort_rows DESC, pipeline_id, model_id
            LIMIT {max_cohorts}
        )
        SELECT
            r.pipeline_id,
            r.model_id,
            r.cohort_rows,
            multiIf(r.low_rank = 1, 'low', r.medium_rank = 1, 'medium', r.high_rank = 1, 'high', 'other') AS score_band,
            r.window_start,
            r.orchestrator_address,
            nullIf(r.gpu_model_name, '') AS gpu_model_name,
            round(r.sla_score, 2) AS sla_score,
            round(r.reliability_score, 3) AS reliability_score,
            round(r.quality_score, 3) AS quality_score,
            round(r.latency_score, 3) AS latency_score,
            round(r.fps_score, 3) AS fps_score,
            round(r.ptff_score, 3) AS ptff_score,
            round(r.e2e_score, 3) AS e2e_score,
            r.requested_sessions,
            r.startup_success_sessions,
            r.output_failed_sessions,
            r.total_swapped_sessions,
            r.health_signal_count,
            r.health_expected_signal_count,
            round(ifNull(r.avg_output_fps, 0), 3) AS avg_output_fps,
            round(ifNull(r.avg_prompt_to_first_frame_ms, 0), 1) AS avg_prompt_to_first_frame_ms,
            round(ifNull(r.avg_e2e_latency_ms, 0), 1) AS avg_e2e_latency_ms
        FROM ranked r
        INNER JOIN largest_cohorts c
            ON r.pipeline_id = c.pipeline_id
           AND r.model_id = c.model_id
        WHERE r.low_rank = 1 OR r.medium_rank = 1 OR r.high_rank = 1
        ORDER BY r.cohort_rows DESC, r.pipeline_id, r.model_id, score_band
        """,
        fmt="JSONEachRow",
    )
    return rows


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


def build_api_probe_table(rows: list[dict[str, Any]]) -> list[str]:
    out = ["| Endpoint | Status | Latency | Notes |", "|---|---:|---:|---|"]
    for row in rows:
        notes = []
        if row.get("error"):
            notes.append(f"error={row['error']}")
        if row.get("rows") is not None:
            notes.append(f"rows={row['rows']}")
        if row.get("total_count") is not None:
            notes.append(f"total_count={row['total_count']}")
        if row.get("window") is not None:
            notes.append(f"window={row['window']}")
        out.append(
            f"| `{row['name']}` | {row['status']} | {row['latency_seconds']:.6f}s | {'; '.join(notes)} |"
        )
    return out


def build_attribution_org_table(rows: list[dict[str, Any]]) -> list[str]:
    out = [
        "| Org | Recent Sessions | Selected | Identity | Resolved | Hardware-less | Stale | Ambiguous | Unresolved | No Orch | Unknown |",
        "|---|---:|---:|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        def fmt_pair(count_key: str, pct_key: str) -> str:
            pct = row.get(pct_key)
            pct_text = f"{pct:.2f}%" if pct is not None else "n/a"
            return f"{row[count_key]} ({pct_text})"

        out.append(
            f"| `{row['org']}` | {row['recent_sessions']} | {row['selected_sessions']} | "
            f"{fmt_pair('orchestrator_identity', 'orchestrator_identity_pct')} | "
            f"{fmt_pair('resolved', 'resolved_pct')} | "
            f"{fmt_pair('hardware_less', 'hardware_less_pct')} | "
            f"{fmt_pair('stale', 'stale_pct')} | "
            f"{fmt_pair('ambiguous', 'ambiguous_pct')} | "
            f"{fmt_pair('unresolved', 'unresolved_pct')} | "
            f"{fmt_pair('no_orch_sessions', 'no_orch_pct')} | "
            f"{fmt_pair('unknown_sessions', 'unknown_pct')} |"
        )
    return out


def build_sla_cohort_markdown(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["No recent non-empty SLA cohorts found."]
    out = []
    current_cohort: tuple[str, str] | None = None
    for row in rows:
        cohort = (row["pipeline_id"], row["model_id"])
        if cohort != current_cohort:
            current_cohort = cohort
            if out:
                out.append("")
            out.extend(
                [
                    f"#### `{row['pipeline_id']} / {row['model_id']}`",
                    "",
                    f"- Cohort rows in lookback: `{row['cohort_rows']}`",
                    "",
                    "| Band | SLA | Reliability | Quality | Req | Startup OK | Output Failed | Swaps | Health | Avg FPS | Avg PTFF ms | Avg E2E ms | GPU |",
                    "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|",
                ]
            )
        gpu_model_name = row.get("gpu_model_name") or "unknown"
        health = f"{row['health_signal_count']}/{row['health_expected_signal_count']}"
        out.append(
            f"| {row['score_band']} | {row['sla_score']:.2f} | {row['reliability_score']:.3f} | {row['quality_score']:.3f} | "
            f"{row['requested_sessions']} | {row['startup_success_sessions']} | {row['output_failed_sessions']} | {row['total_swapped_sessions']} | "
            f"{health} | {row['avg_output_fps']:.3f} | {row['avg_prompt_to_first_frame_ms']:.1f} | {row['avg_e2e_latency_ms']:.1f} | {gpu_model_name} |"
        )
    return out


def build_markdown_report(
    transport: dict[str, Any],
    attribution_summary: dict[str, Any],
    runtime: dict[str, Any],
    api_probes: list[dict[str, Any]],
    sla_cohort_examples: list[dict[str, Any]],
) -> str:
    measured = transport["measured_at"]
    recent_count = attribution_summary["recent_sessions"]
    selection_total = recent_count or 1
    bucket_total = recent_count or 1
    selection_rows = [
        (bucket, count, percent(count, selection_total) or 0.0)
        for bucket, count in sorted(
            attribution_summary["selection_outcomes"].items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    selected_session = attribution_summary.get("selected_sessions", {})
    startup_rows = [
        (bucket, count, percent(count, bucket_total) or 0.0)
        for bucket, count in sorted(
            attribution_summary["startup_outcomes"].items(), key=lambda item: item[1], reverse=True
        )
    ]

    lines = [
        f"# {measured['lag_sample_end_utc'][:10]} Job Baseline",
        "",
        "Measurement window:",
        (
            f"- Kafka lag / throughput sample: `{measured['lag_sample_start_utc']} UTC` to `{measured['lag_sample_end_utc']} UTC`"
            if transport.get("available", True)
            else f"- Kafka lag / throughput sample: unavailable (`{transport.get('unavailable_reason')}`)"
        ),
        f"- Recent session attribution slice: last `{fmt_window(attribution_summary['recent_window_minutes'])}`, anchored at `{attribution_summary['analysis_now_ts']}`",
        "",
        "## Scope",
        "",
        "This baseline captures the transport plane and the recent session plane separately.",
        (
            f"- Transport throughput reflects the current local processing rate for `{transport['network_events']['table']}`."
            if transport.get("available", True)
            else "- Transport throughput was not measured because Kafka UI topic metadata was unavailable or did not match the active ClickHouse source."
        ),
        f"- Recent attribution uses `canonical_session_current` over the last `{fmt_window(attribution_summary['recent_window_minutes'])}` across all orgs.",
        "",
        "## Baseline",
        "",
        "### 1. `network_events` throughput",
        "",
    ]
    if transport.get("available", True):
        network = transport["network_events"]
        streaming = transport["streaming_events"]
        lines.extend(
            [
                f"- Remote source produce rate: `{network['produced_delta']:,} / {transport['window_seconds']}s = {network['produced_rate_per_sec']:.2f} msg/s`",
                f"- Local consume rate by offset advance: `{network['consumed_delta']:,} / {transport['window_seconds']}s = {network['consumed_rate_per_sec']:.2f} msg/s`",
                f"- Local landed rows into `naap.accepted_raw_events` for `daydream`: `{network['events_table_daydream_delta']:,} / {transport['window_seconds']}s = {network['events_table_daydream_rate_per_sec']:.2f} rows/s`",
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
        )
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
            ]
        )
    else:
        lines.extend(
            [
                f"- Transport measurement unavailable: `{transport.get('unavailable_reason')}`",
                "",
                "### 2. Attribution rate by semantic bucket",
                "",
            ]
        )

    lines.extend(
        [
            f"Recent active sessions in the last `{fmt_window(attribution_summary['recent_window_minutes'])}`: `{recent_count}`",
            "",
            "Selection outcomes for those sessions:",
            "",
            *markdown_rate_table(selection_rows),
            "",
            "### 3. Attribution by org and subtype",
            "",
            "Primary metric: selected sessions with `attribution_status in ('resolved', 'hardware_less')`.",
            "That shows orchestrator identity and pipeline/model canonicalization worked, even when hardware metadata is missing.",
            "",
            f"- Selected sessions overall: `{selected_session.get('total', 0)}`",
            f"- Orchestrator identity on selected sessions: `{selected_session.get('orchestrator_identity', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('orchestrator_identity_pct') or 0.0):.2f}%`",
            f"- Resolved on selected sessions: `{selected_session.get('resolved', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('resolved_pct') or 0.0):.2f}%`",
            f"- Hardware-less on selected sessions: `{selected_session.get('hardware_less', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('hardware_less_pct') or 0.0):.2f}%`",
            f"- Stale on selected sessions: `{selected_session.get('stale', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('stale_pct') or 0.0):.2f}%`",
            f"- Ambiguous on selected sessions: `{selected_session.get('ambiguous', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('ambiguous_pct') or 0.0):.2f}%`",
            f"- Unresolved on selected sessions: `{selected_session.get('unresolved', 0)} / {selected_session.get('total', 0)} = {(selected_session.get('unresolved_pct') or 0.0):.2f}%`",
            "",
            *build_attribution_org_table(attribution_summary.get("per_org", [])),
        ]
    )
    status_recent = attribution_summary["status_recent"]
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
            f"- Sessions with `selection_outcome = no_orch`: `{attribution_summary['selection_outcomes'].get('no_orch', 0)} / {recent_count} = {(percent(attribution_summary['selection_outcomes'].get('no_orch', 0), recent_count) or 0.0):.2f}%`",
            f"- Swapped sessions: `{attribution_summary['swapped_sessions']} / {recent_count} = {(percent(attribution_summary['swapped_sessions'], recent_count) or 0.0):.2f}%`",
            f"- Average health signal coverage ratio: `{attribution_summary['avg_health_signal_coverage_ratio']}`",
            f"- Average status samples per session with status evidence: `{attribution_summary['avg_status_samples_per_session']}`",
            "",
            f"Recent status quality, last `{status_recent.get('window_minutes')}`m:",
            "",
            f"- Status samples: `{status_recent.get('samples')}`",
            f"- Sessions represented: `{status_recent.get('sessions')}`",
            f"- Sessions with orchestrator identity in those samples: `{status_recent.get('orchestrated_sessions')} / {status_recent.get('sessions')} = {(status_recent.get('orchestrated_session_pct') or 0.0):.2f}%`",
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
            "",
            "### 5. API latency probes",
            "",
            *build_api_probe_table(api_probes),
            "",
            "### 6. Sample SLA cohort examples",
            "",
            "Representative low / medium / high rows for the largest recent SLA cohorts.",
            "",
            *build_sla_cohort_markdown(sla_cohort_examples),
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)

    dotenv = read_dotenv(repo_root / ".env")
    kafka_ui_url = flag_or_raise("kafka-ui-url", args.kafka_ui_url)
    kafka_ui_user = args.kafka_ui_username
    kafka_ui_password = args.kafka_ui_password
    ch_password = value_or_raise(
        "CLICKHOUSE_ADMIN_PASSWORD", args.clickhouse_password, dotenv
    )

    kafka_ui = KafkaUIClient(kafka_ui_url, kafka_ui_user, kafka_ui_password)
    kafka_ui.login()
    ch = ClickHouseHTTPClient(
        args.clickhouse_url, args.clickhouse_user, ch_password, args.clickhouse_db
    )

    measured_start = fmt_dt(datetime.now(tz=UTC))
    sample_t1: SamplePoint | None = None
    try:
        sample_t0 = sample_state(
            kafka_ui, ch, args.kafka_cluster, args.topic_network, args.topic_streaming
        )
        time.sleep(args.window_seconds)
        sample_t1 = sample_state(
            kafka_ui, ch, args.kafka_cluster, args.topic_network, args.topic_streaming
        )
        transport = build_transport_summary(sample_t0, sample_t1, args.window_seconds)
        transport["available"] = True
        transport["unavailable_reason"] = None
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as exc:
        transport = {
            "available": False,
            "unavailable_reason": str(exc),
            "window_seconds": args.window_seconds,
            "measured_at": {
                "lag_sample_start_utc": measured_start,
                "lag_sample_end_utc": fmt_dt(datetime.now(tz=UTC)),
            },
            "network_events": {"table": "kafka_network_events"},
            "streaming_events": {"table": "kafka_streaming_events"},
        }

    attribution_summary = fetch_recent_attribution_summary(
        ch, args.session_window_minutes, args.status_window_minutes
    )

    stats = docker_stats(args.docker_compose_cmd)
    metrics = clickhouse_metrics(ch)
    api_probe_specs = [
        ("GET /healthz", "/healthz", None),
        ("GET /v1/dashboard/kpi", "/v1/dashboard/kpi", "default"),
        ("GET /v1/dashboard/orchestrators", "/v1/dashboard/orchestrators", "default"),
        ("GET /v1/sla/compliance?limit=10", "/v1/sla/compliance?limit=10", "default"),
        ("GET /v1/network/demand?limit=10", "/v1/network/demand?limit=10", "default"),
        ("GET /v1/gpu/network-demand?limit=10", "/v1/gpu/network-demand?limit=10", "default"),
        ("GET /v1/gpu/metrics?limit=10", "/v1/gpu/metrics?limit=10", "default"),
    ]
    api_probes: list[dict[str, Any]] = []
    for name, path, window in api_probe_specs:
        try:
            payload, latency, status = fetch_api_json(args.api_base_url, path)
            probe: dict[str, Any] = {
                "name": name,
                "status": status,
                "latency_seconds": latency,
            }
            if window is not None:
                probe["window"] = window
            if isinstance(payload, dict):
                if "pagination" in payload and isinstance(payload["pagination"], dict):
                    probe["total_count"] = payload["pagination"].get("total_count")
                if "compliance" in payload and isinstance(payload["compliance"], list):
                    probe["rows"] = len(payload["compliance"])
                elif "demand" in payload and isinstance(payload["demand"], list):
                    probe["rows"] = len(payload["demand"])
                elif "gpuMetrics" in payload and isinstance(payload["gpuMetrics"], list):
                    probe["rows"] = len(payload["gpuMetrics"])
                elif "orchestrators" in payload and isinstance(payload["orchestrators"], list):
                    probe["rows"] = len(payload["orchestrators"])
            api_probes.append(probe)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            api_probes.append(
                {
                    "name": name,
                    "status": "error",
                    "latency_seconds": 0.0,
                    "error": str(exc),
                    "window": window,
                }
            )

    sla_cohort_examples = sample_sla_cohorts(ch)
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
            sample_t1.local_consumers if sample_t1 is not None else []
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
            "session_analysis_anchor_utc": attribution_summary["analysis_now_ts"],
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
            (
                "network_events was in backlog catch-up during the measurement window"
                if transport.get("available", True)
                and transport["network_events"]["lag_delta"] < 0
                else "transport throughput was unavailable or network_events was not reducing lag during the measurement window"
            ),
            f"recent attribution was measured from canonical_session_current across all orgs in the last {fmt_window(args.session_window_minutes)}",
        ],
        "network_events": transport["network_events"],
        "streaming_events": transport["streaming_events"],
        f"recent_attribution_{fmt_window(args.session_window_minutes)}": attribution_summary,
        f"status_{args.status_window_minutes}m": attribution_summary.get("status_recent", {}),
        "api_probes": api_probes,
        "sla_cohort_examples": sla_cohort_examples,
        "local_runtime": runtime,
    }
    md_payload = build_markdown_report(
        transport, attribution_summary, runtime, api_probes, sla_cohort_examples
    )

    json_path.write_text(json_dumps(json_payload) + "\n")
    md_path.write_text(md_payload)

    print(f"wrote {md_path}")
    print(f"wrote {json_path}")


if __name__ == "__main__":
    main()
