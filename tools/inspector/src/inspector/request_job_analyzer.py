"""Request-job event handlers for the inspector.

Covers AI batch (fixed pipelines) and BYOC (dynamic capabilities) event families:
  - ai_batch_request   → _handle_ai_batch_request
  - ai_llm_request     → _handle_ai_llm_request
  - job_gateway        → _handle_job_gateway
  - job_auth           → _handle_job_auth
  - worker_lifecycle   → _handle_worker_lifecycle

These handlers are imported and dispatched from analyzer.analyze().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from inspector.analyzer import AnalysisResult


def _handle_ai_batch_request(v: dict, data: dict, ts: int, result: "AnalysisResult") -> None:
    """AI batch job lifecycle — fixed pipelines (text-to-image, llm, etc.)."""
    subtype = data.get("type", "")
    result.ai_batch_subtypes[subtype] += 1

    if subtype == "ai_batch_request_completed":
        pipeline = data.get("pipeline", "unknown")
        success_raw = data.get("success")
        success = bool(success_raw) if success_raw is not None else None
        duration_ms = data.get("duration_ms")
        latency_score = data.get("latency_score")
        result.ai_batch_by_pipeline[pipeline].append({
            "success": success,
            "duration_ms": float(duration_ms) if duration_ms is not None else None,
            "latency_score": float(latency_score) if latency_score is not None else None,
        })


def _handle_ai_llm_request(v: dict, data: dict, ts: int, result: "AnalysisResult") -> None:
    """LLM-specific metrics that pair with AI batch jobs via request_id."""
    model = data.get("model", "unknown")
    tps = data.get("tokens_per_second")
    ttft_ms = data.get("ttft_ms")
    total_tokens = data.get("total_tokens")
    error = data.get("error", "")
    result.ai_batch_llm_by_model[model].append({
        "tps": float(tps) if tps is not None else None,
        "ttft_ms": float(ttft_ms) if ttft_ms is not None else None,
        "total_tokens": int(total_tokens) if total_tokens is not None else None,
        "has_error": bool(error),
    })


def _handle_job_gateway(v: dict, data: dict, ts: int, result: "AnalysisResult") -> None:
    """BYOC job routing events — capabilities are stored verbatim, never hardcoded.

    Note: data['type'] contains the full composite subtype string, e.g.
    'job_gateway_completed' and 'job_gateway_submitted'. This matches the value
    stored in normalized_byoc_job.subtype and used as the canonical filter in
    canonical_byoc_jobs.sql. The short form ('completed', 'submitted') is NOT used.
    """
    subtype = data.get("type", "")
    result.byoc_job_subtypes[subtype] += 1

    if subtype == "job_gateway_completed":
        capability = data.get("capability", "unknown")
        success_raw = data.get("success")
        success = bool(success_raw) if success_raw is not None else None
        duration_ms = data.get("duration_ms")
        result.byoc_jobs_by_capability[capability].append({
            "success": success,
            "duration_ms": float(duration_ms) if duration_ms is not None else None,
        })


def _handle_job_auth(v: dict, data: dict, ts: int, result: "AnalysisResult") -> None:
    """BYOC auth events — success/failure by capability."""
    capability = data.get("capability", "unknown")
    success_raw = data.get("success")
    success = bool(success_raw) if success_raw is not None else None
    result.byoc_auth_by_capability[capability].append({"success": success})


def _handle_worker_lifecycle(v: dict, data: dict, ts: int, result: "AnalysisResult") -> None:
    """BYOC worker/model inventory — capability stored verbatim."""
    capability = data.get("capability", "unknown")
    worker_options = data.get("worker_options") or []
    price_per_unit = data.get("price_per_unit")

    if capability not in result.byoc_workers_by_capability:
        result.byoc_workers_by_capability[capability] = {
            "worker_count": 0,
            "models": set(),
            "prices": [],
        }
    w = result.byoc_workers_by_capability[capability]
    w["worker_count"] += 1

    # Extract models from worker_options array (each entry may have a "model" key)
    for opt in worker_options:
        if isinstance(opt, dict):
            model = opt.get("model")
            if model:
                w["models"].add(str(model))

    if price_per_unit is not None:
        try:
            w["prices"].append(float(price_per_unit))
        except (ValueError, TypeError):
            pass
