#!/usr/bin/env python3
"""Run stage-based integration harness for scenario validation.

This harness is intentionally thin and reuses existing SQL/script entry points:
- scripts/replay_scenario_events.py
- scripts/run_clickhouse_query_pack.py
- scripts/run_clickhouse_data_tests.py

Outputs are written under artifacts/test-runs/<run_id>/:
- harness.log                 (top-level execution log)
- stages/<stage>.log          (per-stage command logs)
- stages/*.json               (JSON outputs from SQL assertion scripts)
- summary.json                (machine-readable stage summary)
- report.md                   (human-readable run report)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

DEFAULT_STATE_PATHS = [
    "data/gateway",
    "data/kafka",
    "data/flink/tmp",
    "data/clickhouse",
    "data/minio",
    "data/grafana",
]

STAGES = [
    "stack_up",
    "schema_apply",
    "pipeline_ready",
    "replay_events",
    "pipeline_wait",
    "query_pack",
    "assert_raw_typed",
    "assert_pipeline",
    "assert_api",
    "assert_scenarios",
    "stack_down",
]

MODE_STAGES = {
    "smoke": [
        "stack_up",
        "schema_apply",
        "pipeline_ready",
        "replay_events",
        "pipeline_wait",
        "query_pack",
        "assert_raw_typed",
        "assert_pipeline",
        "assert_api",
        "stack_down",
    ],
    "full": STAGES,
}


@dataclass
class StageResult:
    stage: str
    status: str
    started_at: str
    ended_at: str
    duration_sec: float
    log_file: str
    command: str | None = None
    error: str | None = None


class HarnessError(RuntimeError):
    """Raised when a required stage fails."""


class Harness:
    """Coordinate stage execution and artifact generation."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.repo_root = Path(__file__).resolve().parent.parent
        self.run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = (self.repo_root / args.artifacts_root / self.run_id).resolve()
        self.stages_dir = self.run_dir / "stages"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.stages_dir.mkdir(parents=True, exist_ok=True)
        self.harness_log = self.run_dir / "harness.log"
        self.diagnostics_dir = self.run_dir / "diagnostics"
        self.stage_results: list[StageResult] = []
        self.failed = False

        self.compose_files = [f.strip() for f in args.compose_files.split(",") if f.strip()]
        if not self.compose_files:
            self.compose_files = ["docker-compose.yml"]

        self.python_prefix = self._resolve_python_prefix(args.python_runner)
        self.state_paths = self._resolve_state_paths(args.state_path)
        self.backup_dir = (
            (self.repo_root / args.backup_root / self.run_id).resolve()
            if args.state_strategy == "backup-reset"
            else None
        )

    def _resolve_python_prefix(self, runner: str) -> list[str]:
        if runner == "uv":
            return ["uv", "run", "--project", "tools/python", "python"]
        if runner == "python":
            return [sys.executable]
        if shutil.which("uv"):
            return ["uv", "run", "--project", "tools/python", "python"]
        return [sys.executable]

    def _resolve_state_paths(self, requested: list[str] | None) -> list[Path]:
        values = requested if requested else DEFAULT_STATE_PATHS
        resolved: list[Path] = []
        seen: set[Path] = set()
        for value in values:
            p = Path(value)
            if not p.is_absolute():
                p = (self.repo_root / p).resolve()
            if p in seen:
                continue
            seen.add(p)
            resolved.append(p)
        return resolved

    def resolve_manifest(self) -> Path:
        """Return manifest path, falling back to latest exported snapshot when needed."""
        manifest = Path(self.args.manifest)
        if not manifest.is_absolute():
            manifest = (self.repo_root / manifest).resolve()
        if manifest.exists():
            return manifest

        # Default fallback behavior keeps local workflows low-friction:
        # if tests/integration/fixtures/manifest.json is absent, use newest snapshot.
        default_manifest = (self.repo_root / "tests/integration/fixtures/manifest.json").resolve()
        if manifest == default_manifest:
            snapshots = sorted(
                (self.repo_root / "tests/integration/fixtures").glob("prod_snapshot_*/manifest.json")
            )
            if snapshots:
                latest = snapshots[-1]
                self.log(
                    f"Manifest not found at {manifest}; using latest snapshot manifest {latest}"
                )
                return latest.resolve()

        raise HarnessError(f"Fixture manifest not found: {manifest}")

    def log(self, message: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {message}"
        print(line)
        with self.harness_log.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def display_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.repo_root))
        except ValueError:
            return str(path)

    def compose_cmd(self, *parts: str) -> list[str]:
        cmd = ["docker", "compose"]
        for compose_file in self.compose_files:
            cmd.extend(["-f", compose_file])
        cmd.extend(parts)
        return cmd

    def compose_down_cmd(self) -> list[str]:
        parts = ["down", "--remove-orphans"]
        if self.args.down_volumes:
            parts.append("--volumes")
        return self.compose_cmd(*parts)

    def run_command(self, stage: str, cmd: Sequence[str], log_file: Path, shell: bool = False) -> None:
        cmd_display = " ".join(cmd)
        self.log(f"[{stage}] running: {cmd_display}")
        mode = "a" if log_file.exists() else "w"
        env = os.environ.copy()
        env.setdefault("LOCAL_UID", str(os.getuid()))
        env.setdefault("LOCAL_GID", str(os.getgid()))
        with log_file.open(mode, encoding="utf-8") as fh:
            if mode == "a":
                fh.write("\n")
            fh.write(f"$ {cmd_display}\n\n")
            start = time.time()
            process = subprocess.Popen(
                cmd if not shell else " ".join(cmd),
                cwd=self.repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=shell,
                env=env,
            )
            assert process.stdout is not None
            for line in process.stdout:
                fh.write(line)
                print(line, end="")
            rc = process.wait()
            elapsed = time.time() - start
            fh.write(f"\n[exit_code={rc}] [duration_sec={elapsed:.3f}]\n")

        if rc != 0:
            raise HarnessError(f"Command failed with exit code {rc}: {cmd_display}")

    def run_command_capture_output(
        self, cmd: Sequence[str], log_file: Path, shell: bool = False
    ) -> tuple[int, float]:
        cmd_display = " ".join(cmd)
        mode = "a" if log_file.exists() else "w"
        env = os.environ.copy()
        env.setdefault("LOCAL_UID", str(os.getuid()))
        env.setdefault("LOCAL_GID", str(os.getgid()))
        with log_file.open(mode, encoding="utf-8") as fh:
            if mode == "a":
                fh.write("\n")
            fh.write(f"$ {cmd_display}\n\n")
            start = time.time()
            process = subprocess.Popen(
                cmd if not shell else " ".join(cmd),
                cwd=self.repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=shell,
                env=env,
            )
            assert process.stdout is not None
            for line in process.stdout:
                fh.write(line)
            rc = process.wait()
            elapsed = time.time() - start
            fh.write(f"\n[exit_code={rc}] [duration_sec={elapsed:.3f}]\n")
        return rc, elapsed

    def capture_runtime_diagnostics(self) -> None:
        if not self.args.capture_logs_on_fail:
            return
        self.diagnostics_dir.mkdir(parents=True, exist_ok=True)

        diag_jobs = [
            ("docker_compose_ps.log", self.compose_cmd("ps", "-a")),
        ]
        log_cmd = self.compose_cmd("logs", "--no-color")
        if self.args.docker_log_tail > 0:
            log_cmd.extend(["--tail", str(self.args.docker_log_tail)])
        diag_jobs.append(("docker_compose_logs.log", log_cmd))

        for file_name, cmd in diag_jobs:
            target = self.diagnostics_dir / file_name
            rc, elapsed = self.run_command_capture_output(cmd, target)
            self.log(
                f"[diagnostics] captured {file_name} (rc={rc}, duration={elapsed:.2f}s)"
            )

    def _append_stage_log(self, log_file: Path, line: str) -> None:
        with log_file.open("a", encoding="utf-8") as fh:
            fh.write(line.rstrip() + "\n")

    def ensure_builder_output_writable(self, stage_log: Path) -> None:
        # Scenario compose overrides builder/usrlib to a named volume, so host
        # flink-jobs/output writability is irrelevant in that mode.
        if any(Path(f).name == "docker-compose.scenario.yml" for f in self.compose_files):
            self._append_stage_log(stage_log, "builder_output_check_skipped=scenario_compose")
            return
        out_dir = (self.repo_root / "flink-jobs/output").resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        probe = out_dir / ".write_probe"
        try:
            probe.write_text("ok\n", encoding="utf-8")
            probe.unlink(missing_ok=True)
            self._append_stage_log(stage_log, f"builder_output_writable={out_dir}")
        except Exception as exc:
            self._append_stage_log(stage_log, f"builder_output_not_writable={out_dir} error={exc}")
            raise HarnessError(
                "Builder output directory is not writable: "
                f"{out_dir}. Fix ownership/permissions (likely root-owned from a previous run)."
            )

    def _state_path_has_contents(self, path: Path) -> bool:
        return path.exists() and any(path.iterdir())

    def _reset_state_path(self, path: Path) -> None:
        if not path.exists():
            return
        for child in path.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()

    def _prepare_state(self, stage: str, stage_log: Path) -> None:
        strategy = self.args.state_strategy
        if strategy == "preserve":
            return
        if not self.args.allow_destructive_reset:
            raise HarnessError(
                "State reset requested but --allow-destructive-reset is not set; "
                "refusing to alter local data."
            )

        paths_with_data = [p for p in self.state_paths if self._state_path_has_contents(p)]
        self.log(
            f"[{stage}] state strategy={strategy}; candidate paths={len(self.state_paths)}; "
            f"paths with data={len(paths_with_data)}"
        )
        self._append_stage_log(
            stage_log,
            f"state_strategy={strategy} allow_destructive_reset={self.args.allow_destructive_reset}",
        )
        for path in self.state_paths:
            self._append_stage_log(stage_log, f"state_path={path}")

        # Ensure containers release file handles before touching bind-mounted directories.
        down_cmd = self.compose_down_cmd()
        self.run_command(stage, down_cmd, stage_log)

        if strategy == "backup-reset":
            assert self.backup_dir is not None
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            self._append_stage_log(stage_log, f"backup_dir={self.backup_dir}")
            for path in paths_with_data:
                rel = path.relative_to(self.repo_root)
                target = self.backup_dir / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(path, target, dirs_exist_ok=True)
                self._append_stage_log(stage_log, f"backed_up={path} -> {target}")
            self.log(f"[{stage}] wrote state backup to {self.backup_dir}")

        for path in self.state_paths:
            path.mkdir(parents=True, exist_ok=True)
            self._reset_state_path(path)
            self._append_stage_log(stage_log, f"reset={path}")
        self.log(f"[{stage}] state directories reset complete")

    def _run_stage(self, stage: str) -> StageResult:
        started = datetime.now(timezone.utc)
        stage_log = self.stages_dir / f"{stage}.log"
        command_for_summary: str | None = None

        try:
            if stage == "stack_up":
                self.ensure_builder_output_writable(stage_log)
                self._prepare_state(stage, stage_log)
                cmd = self.compose_cmd("up", "-d")
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "schema_apply":
                cmd = [
                    "/bin/bash",
                    "-lc",
                    f"docker exec -i {self.args.clickhouse_container} clickhouse-client --multiquery < configs/clickhouse-init/01-schema.sql",
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "pipeline_ready":
                command_for_summary = "wait_for_pipeline_readiness"
                self.wait_for_pipeline_ready(stage, stage_log)

            elif stage == "replay_events":
                manifest = self.resolve_manifest()
                json_out = self.stages_dir / "replay_events.json"

                cmd = [
                    *self.python_prefix,
                    "scripts/replay_scenario_events.py",
                    "--manifest",
                    str(manifest),
                    "--kafka-container",
                    self.args.kafka_container,
                    "--bootstrap-server",
                    self.args.kafka_bootstrap_server,
                    "--topic",
                    self.args.kafka_input_topic,
                    "--json-out",
                    str(json_out),
                ]
                for scenario in self.args.scenario or []:
                    cmd.extend(["--scenario", scenario])
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "pipeline_wait":
                seconds = max(int(self.args.pipeline_wait_seconds), 0)
                command_for_summary = f"sleep {seconds}"
                self.log(f"[{stage}] waiting {seconds} seconds for pipeline propagation")
                with stage_log.open("w", encoding="utf-8") as fh:
                    fh.write(f"sleep {seconds}\n")
                    time.sleep(seconds)
                    fh.write("completed\n")

            elif stage == "query_pack":
                cmd = [
                    *self.python_prefix,
                    "scripts/run_clickhouse_query_pack.py",
                    "--lookback-hours",
                    str(self.args.lookback_hours),
                    "--max-rows",
                    str(self.args.max_rows),
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "assert_pipeline":
                json_out = self.stages_dir / "assert_pipeline.json"
                cmd = [
                    *self.python_prefix,
                    "scripts/run_clickhouse_data_tests.py",
                    "--sql-file",
                    "tests/integration/sql/assertions_pipeline.sql",
                    "--lookback-hours",
                    str(self.args.lookback_hours),
                    "--json-out",
                    str(json_out),
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "assert_raw_typed":
                json_out = self.stages_dir / "assert_raw_typed.json"
                cmd = [
                    *self.python_prefix,
                    "scripts/run_clickhouse_data_tests.py",
                    "--sql-file",
                    "tests/integration/sql/assertions_raw_typed.sql",
                    "--lookback-hours",
                    str(self.args.lookback_hours),
                    "--json-out",
                    str(json_out),
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "assert_api":
                json_out = self.stages_dir / "assert_api.json"
                cmd = [
                    *self.python_prefix,
                    "scripts/run_clickhouse_data_tests.py",
                    "--sql-file",
                    "tests/integration/sql/assertions_api_readiness.sql",
                    "--lookback-hours",
                    str(self.args.lookback_hours),
                    "--json-out",
                    str(json_out),
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "assert_scenarios":
                json_out = self.stages_dir / "assert_scenarios.json"
                cmd = [
                    *self.python_prefix,
                    "scripts/run_clickhouse_data_tests.py",
                    "--sql-file",
                    "tests/integration/sql/assertions_scenario_candidates.sql",
                    "--lookback-hours",
                    str(self.args.scenario_lookback_hours),
                    "--json-out",
                    str(json_out),
                ]
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            elif stage == "stack_down":
                cmd = self.compose_down_cmd()
                command_for_summary = " ".join(cmd)
                self.run_command(stage, cmd, stage_log)

            else:
                raise HarnessError(f"Unknown stage: {stage}")

            status = "PASS"
            error = None

        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            error = str(exc)

        ended = datetime.now(timezone.utc)
        result = StageResult(
            stage=stage,
            status=status,
            started_at=started.isoformat(),
            ended_at=ended.isoformat(),
            duration_sec=(ended - started).total_seconds(),
            log_file=self.display_path(stage_log),
            command=command_for_summary,
            error=error,
        )
        self.stage_results.append(result)
        return result

    def run(self) -> int:
        requested = self.args.stage or MODE_STAGES[self.args.mode]
        self.log(f"Run id: {self.run_id}")
        self.log(f"Mode: {self.args.mode}")
        self.log(f"Requested stages: {', '.join(requested)}")
        self.log(f"Artifacts directory: {self.run_dir}")
        self.log(f"Python runner: {' '.join(self.python_prefix)}")

        stack_started = False
        stack_down_executed = False

        try:
            for stage in requested:
                if stage == "stack_up":
                    # If stack_up fails part-way, containers may still exist.
                    stack_started = True
                result = self._run_stage(stage)
                if stage == "stack_down":
                    stack_down_executed = True
                self.log(f"[{stage}] {result.status} ({result.duration_sec:.2f}s)")
                if result.status != "PASS":
                    self.failed = True
                    if stage != "stack_down":
                        break

            if self.failed and stack_started and not stack_down_executed:
                self.capture_runtime_diagnostics()
                if not self.args.keep_stack_on_fail:
                    self.log("Failure detected before stack_down; running best-effort stack_down")
                    self._run_stage("stack_down")

        finally:
            self.write_reports()

        return 1 if self.failed else 0

    def write_reports(self) -> None:
        summary = {
            "run_id": self.run_id,
            "mode": self.args.mode,
            "requested_stages": self.args.stage or MODE_STAGES[self.args.mode],
            "state_strategy": self.args.state_strategy,
            "allow_destructive_reset": bool(self.args.allow_destructive_reset),
            "down_volumes": bool(self.args.down_volumes),
            "state_paths": [str(p) for p in self.state_paths],
            "backup_dir": str(self.backup_dir) if self.backup_dir else None,
            "failed": self.failed,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "results": [asdict(r) for r in self.stage_results],
        }
        summary_path = self.run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        report_path = self.run_dir / "report.md"
        lines = [
            "# Scenario Integration Harness Report",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- Mode: `{self.args.mode}`",
            f"- State Strategy: `{self.args.state_strategy}`",
            f"- Generated At (UTC): `{summary['generated_at']}`",
            f"- Result: `{'FAIL' if self.failed else 'PASS'}`",
            "",
            "## Stage Results",
            "",
            "| Stage | Status | Duration (s) | Log |",
            "|---|---:|---:|---|",
        ]
        for r in self.stage_results:
            lines.append(f"| `{r.stage}` | `{r.status}` | {r.duration_sec:.2f} | `{r.log_file}` |")

        failed = [r for r in self.stage_results if r.status != "PASS"]
        lines.extend(["", "## Failures", ""])
        if not failed:
            lines.append("No stage failures.")
        else:
            for r in failed:
                lines.append(f"- `{r.stage}`: {r.error or 'unknown error'}")

        lines.extend(
            [
                "",
                "## Key Artifacts",
                "",
                f"- Harness log: `{self.display_path(self.harness_log)}`",
                f"- JSON summary: `{self.display_path(summary_path)}`",
            ]
        )
        if self.diagnostics_dir.exists():
            lines.append(f"- Runtime diagnostics: `{self.display_path(self.diagnostics_dir)}`")
        if self.backup_dir:
            lines.append(f"- State backup: `{self.display_path(self.backup_dir)}`")

        for json_name in [
            "replay_events.json",
            "assert_raw_typed.json",
            "assert_pipeline.json",
            "assert_api.json",
            "assert_scenarios.json",
        ]:
            candidate = self.stages_dir / json_name
            if candidate.exists():
                lines.append(f"- Assertion JSON: `{self.display_path(candidate)}`")

        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.log(f"Wrote report: {self.display_path(report_path)}")
        self.log(f"Wrote summary: {self.display_path(summary_path)}")

    def _http_get_json(self, url: str, timeout_seconds: int = 5) -> dict:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)

    def _flink_has_running_job(self) -> tuple[bool, str]:
        url = f"http://{self.args.flink_rest_host}:{self.args.flink_rest_port}/jobs/overview"
        try:
            payload = self._http_get_json(url)
            jobs = payload.get("jobs", []) or []
            running_jobs = [j for j in jobs if str(j.get("state", "")).upper() == "RUNNING"]
            if running_jobs:
                names = [str(j.get("name", "<unknown>")) for j in running_jobs]
                return True, f"running_jobs={len(running_jobs)} names={names}"
            return False, f"running_jobs=0 total_jobs={len(jobs)}"
        except Exception as exc:
            return False, f"flink_overview_error={exc}"

    def _connector_running(self, name: str) -> tuple[bool, str]:
        base = f"http://{self.args.connect_rest_host}:{self.args.connect_rest_port}"
        url = f"{base}/connectors/{name}/status"
        try:
            payload = self._http_get_json(url)
            connector_state = str(payload.get("connector", {}).get("state", "")).upper()
            tasks = payload.get("tasks", []) or []
            task_states = [str(t.get("state", "")).upper() for t in tasks]
            tasks_ok = bool(task_states) and all(s == "RUNNING" for s in task_states)
            ok = connector_state == "RUNNING" and tasks_ok
            return ok, (
                f"connector_state={connector_state or 'UNKNOWN'} "
                f"task_states={task_states if task_states else ['<none>']}"
            )
        except urllib.error.HTTPError as exc:
            return False, f"connector_http_error={exc.code}"
        except Exception as exc:
            return False, f"connector_status_error={exc}"

    def wait_for_pipeline_ready(self, stage: str, stage_log: Path) -> None:
        timeout = max(int(self.args.pipeline_ready_timeout_seconds), 1)
        poll = max(int(self.args.pipeline_ready_poll_seconds), 1)
        required_connectors = [c.strip() for c in (self.args.required_connector or []) if c.strip()]
        deadline = time.time() + timeout
        attempts = 0

        with stage_log.open("w", encoding="utf-8") as fh:
            fh.write(f"pipeline_ready_timeout_seconds={timeout}\n")
            fh.write(f"pipeline_ready_poll_seconds={poll}\n")
            fh.write(f"required_connectors={required_connectors}\n")

            while time.time() < deadline:
                attempts += 1
                flink_ok, flink_diag = self._flink_has_running_job()
                connector_diags: list[str] = []
                connectors_ok = True
                for connector in required_connectors:
                    ok, diag = self._connector_running(connector)
                    connectors_ok = connectors_ok and ok
                    connector_diags.append(f"{connector}: {diag}")

                line = (
                    f"attempt={attempts} flink_ok={flink_ok} connectors_ok={connectors_ok} "
                    f"flink_diag={flink_diag} connector_diag={'; '.join(connector_diags)}"
                )
                fh.write(line + "\n")
                self.log(f"[{stage}] {line}")

                if flink_ok and connectors_ok:
                    fh.write("pipeline_ready=true\n")
                    return

                time.sleep(poll)

            fh.write("pipeline_ready=false\n")
            raise HarnessError(
                "Pipeline readiness timeout: Flink job and/or required connectors were not RUNNING "
                f"within {timeout}s"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run scenario integration harness with stage-level controls")
    parser.add_argument("--mode", choices=sorted(MODE_STAGES.keys()), default="full")
    parser.add_argument(
        "--stage",
        action="append",
        choices=STAGES,
        help="Run only specified stage(s). Repeatable. Overrides --mode stage list.",
    )
    parser.add_argument("--run-id", help="Override run id used for artifact folder")
    parser.add_argument("--artifacts-root", default="artifacts/test-runs")
    parser.add_argument("--compose-files", default="docker-compose.yml", help="Comma-separated compose file list")
    parser.add_argument("--clickhouse-container", default="livepeer-analytics-clickhouse")
    parser.add_argument("--kafka-container", default="live-video-to-video-kafka")
    parser.add_argument("--kafka-bootstrap-server", default="kafka:9092")
    parser.add_argument("--kafka-input-topic", default="streaming_events")
    parser.add_argument("--manifest", default="tests/integration/fixtures/manifest.json")
    parser.add_argument(
        "--scenario",
        action="append",
        help="Optional scenario name filter for replay stage; repeatable.",
    )
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--scenario-lookback-hours", type=int, default=720)
    parser.add_argument("--pipeline-wait-seconds", type=int, default=20)
    parser.add_argument("--pipeline-ready-timeout-seconds", type=int, default=240)
    parser.add_argument("--pipeline-ready-poll-seconds", type=int, default=5)
    parser.add_argument("--flink-rest-host", default="localhost")
    parser.add_argument("--flink-rest-port", type=int, default=8081)
    parser.add_argument("--connect-rest-host", default="localhost")
    parser.add_argument("--connect-rest-port", type=int, default=8083)
    parser.add_argument(
        "--required-connector",
        action="append",
        default=["clickhouse-raw-events-sink", "minio-raw-events-sink"],
        help=(
            "Connector names that must be RUNNING before replay. Repeatable. "
            "Default: clickhouse-raw-events-sink and minio-raw-events-sink."
        ),
    )
    parser.add_argument("--max-rows", type=int, default=50)
    parser.add_argument("--keep-stack-on-fail", action="store_true")
    parser.add_argument(
        "--capture-logs-on-fail",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Capture docker compose ps/logs into run artifacts when a stage fails.",
    )
    parser.add_argument(
        "--docker-log-tail",
        type=int,
        default=2000,
        help="Tail lines for compose logs capture on failure; 0 means full logs.",
    )
    parser.add_argument(
        "--down-volumes",
        action="store_true",
        help="When stack_down runs, include --volumes to remove compose-managed named volumes.",
    )
    parser.add_argument(
        "--state-strategy",
        choices=["preserve", "reset", "backup-reset"],
        default="preserve",
        help=(
            "How to handle local stack state before stack_up: "
            "preserve (no cleanup), reset (delete state dirs), "
            "backup-reset (backup then delete)."
        ),
    )
    parser.add_argument(
        "--allow-destructive-reset",
        action="store_true",
        help="Required when --state-strategy is reset or backup-reset.",
    )
    parser.add_argument(
        "--state-path",
        action="append",
        help=(
            "State directory to include in reset/backup operations. "
            "Repeatable. Defaults to key ./data/* bind mount paths."
        ),
    )
    parser.add_argument(
        "--backup-root",
        default="artifacts/state-backups",
        help="Backup root for --state-strategy backup-reset.",
    )
    parser.add_argument(
        "--python-runner",
        choices=["auto", "uv", "python"],
        default="auto",
        help="How to invoke Python scripts (auto prefers uv when available)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    harness = Harness(args)
    sys.exit(harness.run())


if __name__ == "__main__":
    main()
