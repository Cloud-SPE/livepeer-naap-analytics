-- Livepeer Analytics: Ops Activity Validation Queries
-- Scope: last 24 hours (UTC)
-- Note: run each query independently if your SQL client disallows multi-statements.

-- 1) Stream/session activity over time (5m buckets)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  toStartOfInterval(session_start_ts, INTERVAL 5 MINUTE) AS bucket,
  count() AS session_rows,
  uniqExact(workflow_session_id) AS sessions,
  uniqExact(stream_id) AS streams,
  uniqExact(request_id) AS requests
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
GROUP BY bucket
ORDER BY bucket;

-- 2) Top orchestrators by served sessions
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  orchestrator_address,
  count() AS session_rows,
  uniqExact(workflow_session_id) AS sessions,
  avg(startup_success) AS startup_success_rate,
  sum(swap_count) AS swaps
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
GROUP BY orchestrator_address
ORDER BY sessions DESC
LIMIT 25;

-- 3) Gateway -> orchestrator traffic matrix
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  gateway,
  orchestrator_address,
  uniqExact(workflow_session_id) AS sessions
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
GROUP BY gateway, orchestrator_address
ORDER BY sessions DESC
LIMIT 100;

-- 4) Session classification / reliability summary
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS sessions,
  sum(known_stream) AS known_stream_sessions,
  sum(startup_success) AS startup_success_sessions,
  sum(startup_excused) AS startup_excused_sessions,
  sum(startup_unexcused) AS startup_unexcused_sessions,
  sum(swap_count > 0) AS swapped_sessions,
  avg(startup_unexcused) AS unexcused_rate
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= from_ts AND session_start_ts < to_ts;

-- 5) Swap activity details (segment-level), joined to latest session identifiers
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts,
  latest_sessions AS (
    SELECT
      workflow_session_id,
      argMax(stream_id, version) AS stream_id,
      argMax(request_id, version) AS request_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  )
SELECT
  seg.workflow_session_id,
  ls.stream_id,
  ls.request_id,
  count() AS segment_rows,
  uniqExactIf(seg.orchestrator_address, seg.orchestrator_address != '') AS orchestrators_seen,
  min(seg.segment_start_ts) AS first_segment_ts,
  max(coalesce(seg.segment_end_ts, seg.segment_start_ts)) AS last_segment_ts
FROM livepeer_analytics.fact_workflow_session_segments AS seg
LEFT JOIN latest_sessions AS ls USING (workflow_session_id)
WHERE seg.segment_start_ts >= from_ts AND seg.segment_start_ts < to_ts
GROUP BY seg.workflow_session_id, ls.stream_id, ls.request_id
HAVING orchestrators_seen > 1
ORDER BY last_segment_ts DESC
LIMIT 50;

-- 6) Model/GPU attribution coverage
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS sessions,
  sum(model_id != '' AND model_id IS NOT NULL) AS with_model,
  sum(gpu_id != '' AND gpu_id IS NOT NULL) AS with_gpu,
  avg(gpu_attribution_confidence) AS avg_attr_conf
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= from_ts AND session_start_ts < to_ts;

-- 7) Error update activity from raw ai_stream_status (tracks last_error_time changes)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  stream_id,
  request_id,
  uniqExact(last_error_time) AS distinct_error_updates,
  max(last_error_time) AS latest_error_time,
  argMax(last_error, last_error_time) AS latest_error
FROM livepeer_analytics.ai_stream_status
WHERE event_timestamp >= from_ts
  AND event_timestamp < to_ts
  AND last_error_time IS NOT NULL
GROUP BY stream_id, request_id
ORDER BY distinct_error_updates DESC, latest_error_time DESC
LIMIT 50;

-- ============================================================
-- API / DASHBOARD SERVING VALIDATION
-- ============================================================

-- A1) Goal: Confirm rollup tables are populated for the validation window.
-- What this query does: Counts rows in each agg_* table over last 24h.
-- Valid output: Non-zero counts for tables expected to have traffic.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT 'agg_stream_performance_1m' AS object_name, count() AS rows_24h
FROM livepeer_analytics.agg_stream_performance_1m
WHERE window_start >= from_ts AND window_start < to_ts
UNION ALL
SELECT 'agg_reliability_1h' AS object_name, count() AS rows_24h
FROM livepeer_analytics.agg_reliability_1h
WHERE window_start >= from_ts AND window_start < to_ts
;

-- A2) Goal: Confirm API/dashboard views are populated.
-- What this query does: Counts rows in each v_api_* view over last 24h.
-- Valid output: Non-zero counts for views expected to have traffic.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT 'v_api_gpu_metrics' AS object_name, count() AS rows_24h
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= from_ts AND window_start < to_ts
UNION ALL
SELECT 'v_api_network_demand' AS object_name, count() AS rows_24h
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= from_ts AND window_start < to_ts
UNION ALL
SELECT 'v_api_sla_compliance' AS object_name, count() AS rows_24h
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= from_ts AND window_start < to_ts;

-- A3) Goal: Verify v_api_gpu_metrics matches its rollup source numerically.
-- What this query does: Recomputes avg_output_fps from agg_stream_performance_1m
-- and compares with v_api_gpu_metrics on identical grouping keys.
-- Valid output: mean_abs_diff_fps = 0 and max_abs_diff_fps = 0.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS joined_rows,
  avg(abs(a.avg_output_fps - b.avg_output_fps)) AS mean_abs_diff_fps,
  max(abs(a.avg_output_fps - b.avg_output_fps)) AS max_abs_diff_fps
FROM
(
  SELECT
    window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region,
    avgMerge(output_fps_avg_state) AS avg_output_fps
  FROM livepeer_analytics.agg_stream_performance_1m
  WHERE window_start >= from_ts AND window_start < to_ts
  GROUP BY window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region
) a
INNER JOIN
(
  SELECT
    window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region,
    avg_output_fps
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= from_ts AND window_start < to_ts
) b
USING (window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region);

-- A4) Goal: Verify v_api_sla_compliance counts match deduped session facts.
-- What this query does: Recomputes known/unexcused/swapped counts from latest
-- (argMax by version) workflow sessions and compares to v_api_sla_compliance.
-- Valid output: known_diff = 0, unexcused_diff = 0, swapped_diff = 0.
WITH
  now64(3,'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS joined_rows,
  sum(abs(a.known_sessions - b.known_sessions)) AS known_diff,
  sum(abs(a.unexcused_sessions - b.unexcused_sessions)) AS unexcused_diff,
  sum(abs(a.swapped_sessions - b.swapped_sessions)) AS swapped_diff
FROM
(
  WITH latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      argMax(pipeline_id, version) AS pipeline_id,
      argMax(model_id, version) AS model_id,
      argMax(gpu_id, version) AS gpu_id,
      argMax(region, version) AS region,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(swap_count, version) AS swap_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  )
  SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region,
    sum(toUInt64(known_stream)) AS known_sessions,
    sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
    sum(toUInt64(swap_count > 0)) AS swapped_sessions
  FROM latest_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
  GROUP BY window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region
) a
INNER JOIN
(
  SELECT
    window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region,
    known_sessions, unexcused_sessions, swapped_sessions
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= from_ts AND window_start < to_ts
) b
USING (window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region);

-- A5) Goal: Validate SLA ratios are mathematically valid.
-- What this query does: Checks API ratio columns for out-of-bound values.
-- Valid output: bad_success_ratio = 0 and bad_no_swap_ratio = 0.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  countIf(success_ratio < 0 OR success_ratio > 1) AS bad_success_ratio,
  countIf(no_swap_ratio < 0 OR no_swap_ratio > 1) AS bad_no_swap_ratio
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= from_ts AND window_start < to_ts;

-- A6) Goal: Measure dimensional completeness for dashboard filters.
-- What this query does: Counts rows with missing orchestrator/pipeline/model/gpu keys
-- in v_api_gpu_metrics for last 24h.
-- Valid output: Missing counts should be low and explainable by known startup failures.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS rows_total,
  countIf(orchestrator_address = '') AS missing_orch,
  countIf(pipeline = '') AS missing_pipeline,
  countIf(model_id = '' OR model_id IS NULL) AS missing_model,
  countIf(gpu_id = '' OR gpu_id IS NULL) AS missing_gpu
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= from_ts AND window_start < to_ts;

-- A7) Goal: Detect missing time buckets in the minute-level API output.
-- What this query does: Builds the expected minute series for last 24h and left joins
-- to observed minute buckets in v_api_gpu_metrics.
-- Valid output: missing_minute_buckets = 0 for continuous traffic windows.
WITH
  toDateTime(toStartOfInterval(now64(3,'UTC') - INTERVAL 24 HOUR, INTERVAL 1 MINUTE)) AS start_min,
  toDateTime(toStartOfInterval(now64(3,'UTC'), INTERVAL 1 MINUTE)) AS end_min
SELECT count() AS missing_minute_buckets
FROM
(
  SELECT addMinutes(start_min, toInt32(number)) AS window_start
  FROM numbers(dateDiff('minute', start_min, end_min))
) expected
LEFT JOIN
(
  SELECT DISTINCT toDateTime(window_start) AS window_start
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= start_min AND window_start < end_min
) observed
USING (window_start)
WHERE observed.window_start IS NULL;

-- A8) Goal: Validate lifecycle unmatched-edge diagnostics are being emitted.
-- What this query does: Summarizes matched/unmatched coverage for terminal lifecycle signals.
-- Valid output: Non-zero total_terminal_rows when traffic exists; unmatched rows should be explainable.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS total_terminal_rows,
  countIf(unmatched_reason = '') AS terminal_matched_rows,
  countIf(unmatched_reason != '') AS terminal_unmatched_rows
FROM livepeer_analytics.fact_lifecycle_edge_coverage
WHERE signal_ts >= from_ts
  AND signal_ts < to_ts
  AND is_terminal_signal = 1;

-- A9) Goal: Validate 5-minute jitter serving contract with sample threshold.
-- What this query does: Checks v_api_jitter_5m output and confirms no rows violate sample minimum.
-- Valid output: rows_24h >= 0 and rows_below_min_samples = 0.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS rows_24h,
  countIf(status_samples < 10) AS rows_below_min_samples
FROM livepeer_analytics.v_api_jitter_5m
WHERE window_start_5m >= from_ts
  AND window_start_5m < to_ts;

-- A10) Goal: Validate hourly jitter trend support (query 1.2 equivalent).
-- What this query does: Computes hourly jitter coefficient per orchestrator + pipeline
-- from curated status facts over last 24h.
-- Valid output: Non-empty rows during traffic windows; sample_count >= 10 for each row.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  toStartOfInterval(sample_ts, INTERVAL 1 HOUR) AS hour_bucket,
  orchestrator_address,
  pipeline,
  count() AS sample_count,
  avg(output_fps) AS mean_fps,
  stddevPop(output_fps) / nullIf(avg(output_fps), 0) AS jitter_coefficient,
  quantileTDigest(0.95)(output_fps) AS p95_fps
FROM livepeer_analytics.fact_stream_status_samples
WHERE sample_ts >= from_ts
  AND sample_ts < to_ts
  AND output_fps > 0
GROUP BY hour_bucket, orchestrator_address, pipeline
HAVING sample_count >= 10
ORDER BY hour_bucket DESC, jitter_coefficient DESC;

-- A11) Goal: Validate network-wide jitter benchmark support (query 1.5 equivalent).
-- What this query does: Computes per-pipeline network jitter distribution over last 1h.
-- Valid output: One row per active pipeline with stable percentile outputs.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 1 HOUR AS from_ts
SELECT
  pipeline,
  uniqExact(orchestrator_address) AS num_orchestrators,
  uniqExact(stream_id) AS num_streams,
  count() AS total_samples,
  avg(output_fps) AS network_mean_fps,
  stddevPop(output_fps) / nullIf(avg(output_fps), 0) AS network_jitter_coefficient,
  quantileTDigest(0.05)(output_fps) AS p05_fps,
  quantileTDigest(0.25)(output_fps) AS p25_fps,
  quantileTDigest(0.50)(output_fps) AS p50_fps,
  quantileTDigest(0.75)(output_fps) AS p75_fps,
  quantileTDigest(0.95)(output_fps) AS p95_fps,
  quantileTDigest(0.99)(output_fps) AS p99_fps
FROM livepeer_analytics.fact_stream_status_samples
WHERE sample_ts >= from_ts
  AND sample_ts < to_ts
  AND output_fps > 0
GROUP BY pipeline
ORDER BY pipeline;

-- A12) Goal: Alert query for high jitter (query 4.1 equivalent).
-- What this query does: Uses 5-minute serving jitter view and flags rows above threshold.
-- Valid output: Empty/low row count in healthy periods.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 15 MINUTE AS from_ts
SELECT
  window_start_5m,
  orchestrator_address,
  pipeline,
  model_id,
  gpu_id,
  status_samples,
  jitter_coeff_fps
FROM livepeer_analytics.v_api_jitter_5m
WHERE window_start_5m >= from_ts
  AND window_start_5m < to_ts
  AND jitter_coeff_fps > 0.2
ORDER BY jitter_coeff_fps DESC, status_samples DESC;

-- A13) Goal: Alert query for high startup latency proxy (query 4.2 equivalent).
-- What this query does: Builds per-orchestrator startup latency stats from session facts
-- using first_processed_ts - first_stream_request_ts over last 15 minutes.
-- Valid output: Empty/low row count in healthy periods.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 15 MINUTE AS from_ts,
  latencies AS (
    SELECT
      orchestrator_address,
      dateDiff('millisecond', first_stream_request_ts, first_processed_ts) AS latency_ms
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts >= from_ts
      AND session_start_ts < to_ts
      AND first_stream_request_ts IS NOT NULL
      AND first_processed_ts IS NOT NULL
      AND first_processed_ts > first_stream_request_ts
  )
SELECT
  orchestrator_address,
  count() AS stream_count,
  avg(latency_ms) AS avg_latency_ms,
  quantileTDigest(0.95)(latency_ms) AS p95_latency_ms,
  max(latency_ms) AS max_latency_ms
FROM latencies
GROUP BY orchestrator_address
HAVING avg_latency_ms > 3000
    OR p95_latency_ms > 5000
ORDER BY avg_latency_ms DESC;
