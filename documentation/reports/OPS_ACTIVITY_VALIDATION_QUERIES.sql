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
  avg(attribution_confidence) AS avg_attr_conf
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
UNION ALL
SELECT 'agg_latency_edges_1m' AS object_name, count() AS rows_24h
FROM livepeer_analytics.agg_latency_edges_1m
WHERE window_start >= from_ts AND window_start < to_ts;

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

-- A4) Goal: Verify v_api_sla_compliance counts match reliability rollups.
-- What this query does: Recomputes known/unexcused/swapped session counts from
-- agg_reliability_1h aggregate states and compares to v_api_sla_compliance.
-- Valid output: known_diff = 0, unexcused_diff = 0, swapped_diff = 0.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS joined_rows,
  sum(abs(a.known_sessions - b.known_sessions)) AS known_diff,
  sum(abs(a.unexcused_sessions - b.unexcused_sessions)) AS unexcused_diff,
  sum(abs(a.swapped_sessions - b.swapped_sessions)) AS swapped_diff
FROM
(
  SELECT
    window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region,
    sumMerge(known_sessions_state) AS known_sessions,
    sumMerge(unexcused_sessions_state) AS unexcused_sessions,
    sumMerge(swapped_sessions_state) AS swapped_sessions
  FROM livepeer_analytics.agg_reliability_1h
  WHERE window_start >= from_ts AND window_start < to_ts
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
