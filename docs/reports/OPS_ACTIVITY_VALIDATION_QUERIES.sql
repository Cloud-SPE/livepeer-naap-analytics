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
  sum(confirmed_swap_count) AS confirmed_swaps,
  sum(inferred_orchestrator_change_count) AS inferred_orchestrator_changes
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
  sum((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0)) AS total_swapped_sessions,
  sum(confirmed_swap_count > 0) AS confirmed_swapped_sessions,
  sum(inferred_orchestrator_change_count > 0) AS inferred_orchestrator_change_sessions,
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
FROM livepeer_analytics.raw_ai_stream_status
WHERE event_timestamp >= from_ts
  AND event_timestamp < to_ts
  AND last_error_time IS NOT NULL
GROUP BY stream_id, request_id
ORDER BY distinct_error_updates DESC, latest_error_time DESC
LIMIT 50;

-- ============================================================
-- API / DASHBOARD SERVING VALIDATION
-- ============================================================

-- A1) Goal: Confirm append-safe perf rollup is populated for the validation window.
-- What this query does: Counts rows in `agg_stream_performance_1m` over last 24h.
-- Valid output: Non-zero count when status traffic exists.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT 'agg_stream_performance_1m' AS object_name, count() AS rows_24h
FROM livepeer_analytics.agg_stream_performance_1m
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

-- A2b) Goal: Confirm GPU demand companion view is populated.
-- What this query does: Counts rows in `v_api_network_demand_by_gpu` over last 24h.
-- Valid output: Non-zero count when attributed perf/session demand traffic exists.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT count() AS rows_24h
FROM livepeer_analytics.v_api_network_demand_by_gpu
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
    window_start, orchestrator_address, pipeline, model_id, gpu_id, region,
    avgMerge(output_fps_avg_state) AS avg_output_fps
  FROM livepeer_analytics.agg_stream_performance_1m
  WHERE window_start >= from_ts AND window_start < to_ts
  GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
) a
INNER JOIN
(
  SELECT
    window_start, orchestrator_address, pipeline, model_id, gpu_id, region,
    avg_output_fps
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= from_ts AND window_start < to_ts
) b
USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region);

-- A4b) Goal: Verify v_api_network_demand_by_gpu reliability counters match deduped latest sessions.
-- What this query does: Recomputes known/unexcused/swapped counts from latest sessions at
-- GPU demand key grain and compares with `v_api_network_demand_by_gpu`.
-- Valid output: known_diff = 0, unexcused_diff = 0, swapped_diff = 0.
WITH
  now64(3,'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS joined_rows,
  sum(abs(r.known_sessions_count - a.known_sessions_count)) AS known_diff,
  sum(abs(r.startup_unexcused_sessions - a.startup_unexcused_sessions)) AS unexcused_diff,
  sum(abs(r.total_swapped_sessions - a.total_swapped_sessions)) AS swapped_diff
FROM
(
  WITH latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(region, version) AS region,
      argMax(pipeline, version) AS pipeline,
      argMax(model_id, version) AS model_id,
      argMax(gpu_id, version) AS gpu_id,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_gpu_by_key AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      orchestrator_address,
      region,
      pipeline,
      model_id,
      argMaxIf(gpu_id, session_start_ts, ifNull(gpu_id, '') != '') AS gpu_id
    FROM latest_sessions
    GROUP BY window_start, gateway, orchestrator_address, region, pipeline, model_id
  )
  SELECT
    toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) AS window_start,
    s.gateway,
    s.orchestrator_address,
    ifNull(s.region, '') AS region_key,
    s.pipeline,
    ifNull(s.model_id, '') AS model_id_key,
    ifNull(nullIf(if(ifNull(s.gpu_id, '') != '', s.gpu_id, ifNull(k.gpu_id, '')), ''), '') AS gpu_id_key,
    sum(toUInt64(s.known_stream)) AS known_sessions_count,
    sum(toUInt64(s.startup_unexcused)) AS startup_unexcused_sessions,
    sum(toUInt64((s.confirmed_swap_count > 0) OR (s.inferred_orchestrator_change_count > 0))) AS total_swapped_sessions
  FROM latest_sessions s
  LEFT JOIN latest_gpu_by_key k
    ON k.window_start = toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR)
   AND k.gateway = s.gateway
   AND k.orchestrator_address = s.orchestrator_address
   AND ifNull(k.region, '') = ifNull(s.region, '')
   AND k.pipeline = s.pipeline
   AND ifNull(k.model_id, '') = ifNull(s.model_id, '')
  WHERE s.session_start_ts >= from_ts AND s.session_start_ts < to_ts
  GROUP BY window_start, s.gateway, s.orchestrator_address, region_key, s.pipeline, model_id_key, gpu_id_key
) r
INNER JOIN
(
  SELECT
    window_start,
    gateway,
    orchestrator_address,
    ifNull(region, '') AS region_key,
    pipeline,
    ifNull(model_id, '') AS model_id_key,
    ifNull(gpu_id, '') AS gpu_id_key,
    known_sessions_count,
    startup_unexcused_sessions,
    total_swapped_sessions
  FROM livepeer_analytics.v_api_network_demand_by_gpu
  WHERE window_start >= from_ts AND window_start < to_ts
) a
USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key);

-- A4) Goal: Verify v_api_sla_compliance counts match deduped session facts.
-- What this query does: Recomputes known/unexcused/swapped counts from latest
-- (argMax by version) workflow sessions and compares to v_api_sla_compliance.
-- Valid output: known_diff = 0, unexcused_diff = 0, swapped_diff = 0.
WITH
  now64(3,'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS joined_rows,
  sum(abs(a.known_sessions_count - b.known_sessions_count)) AS known_diff,
  sum(abs(a.startup_unexcused_sessions - b.startup_unexcused_sessions)) AS unexcused_diff,
  sum(abs(a.total_swapped_sessions - b.total_swapped_sessions)) AS swapped_diff
FROM
(
  WITH latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      argMax(model_id, version) AS model_id,
      argMax(gpu_id, version) AS gpu_id,
      argMax(region, version) AS region,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  )
  SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address, pipeline, model_id, gpu_id, region,
    sum(toUInt64(known_stream)) AS known_sessions_count,
    sum(toUInt64(startup_unexcused)) AS startup_unexcused_sessions,
    sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS total_swapped_sessions
  FROM latest_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
  GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
) a
INNER JOIN
(
  SELECT
    window_start, orchestrator_address, pipeline, model_id, gpu_id, region,
    known_sessions_count, startup_unexcused_sessions, total_swapped_sessions
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= from_ts AND window_start < to_ts
) b
USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region);

-- A5) Goal: Validate SLA ratios are mathematically valid.
-- What this query does: Checks API ratio columns for out-of-bound values.
-- Valid output: bad_success_ratio = 0 and bad_no_swap_ratio = 0.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  countIf(effective_success_rate < 0 OR effective_success_rate > 1) AS bad_success_ratio,
  countIf(no_swap_rate < 0 OR no_swap_rate > 1) AS bad_no_swap_ratio
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= from_ts AND window_start < to_ts;

-- A5b) Goal: Validate network-demand startup/effective success semantics.
-- What this query does: Checks ratio bounds and enforces effective success <= startup success.
-- Valid output: all counters = 0.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  countIf(startup_success_rate < 0 OR startup_success_rate > 1) AS bad_startup_success_ratio,
  countIf(effective_success_rate < 0 OR effective_success_rate > 1) AS bad_effective_success_ratio,
  countIf(startup_success_rate + 0.000001 < effective_success_rate) AS effective_gt_startup
FROM livepeer_analytics.v_api_network_demand
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

-- A6b) Goal: Track canonical pipeline coverage for attributable GPU/SLA rows.
-- What this query does: Computes rolling 24h coverage where model is present and pipeline is non-empty.
-- Valid output: pipeline_coverage_ratio >= 0.99 unless there is a known attribution incident.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  sum(rows_with_model) AS attributable_rows_with_model,
  sum(rows_with_pipeline) AS attributable_rows_with_pipeline,
  round(if(sum(rows_with_model) = 0, 1.0, sum(rows_with_pipeline) / sum(rows_with_model)), 6) AS pipeline_coverage_ratio
FROM
(
  SELECT
    countIf(ifNull(model_id, '') != '') AS rows_with_model,
    countIf(ifNull(model_id, '') != '' AND pipeline != '') AS rows_with_pipeline
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= from_ts AND window_start < to_ts

  UNION ALL

  SELECT
    countIf(ifNull(model_id, '') != '') AS rows_with_model,
    countIf(ifNull(model_id, '') != '' AND pipeline != '') AS rows_with_pipeline
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= from_ts AND window_start < to_ts
);

-- A6c) Goal: Surface top attribution-miss hotspots for triage.
-- What this query does: Ranks hour/orchestrator/model/GPU keys where model exists but pipeline is empty.
-- Valid output: ideally zero rows; otherwise use top rows to trace lifecycle attribution gaps.
WITH now64(3,'UTC') AS to_ts, to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  view_name,
  window_start,
  orchestrator_address,
  model_id,
  gpu_id,
  count() AS rows_with_empty_pipeline
FROM
(
  SELECT
    'v_api_gpu_metrics' AS view_name,
    window_start,
    orchestrator_address,
    ifNull(model_id, '') AS model_id,
    ifNull(gpu_id, '') AS gpu_id
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= from_ts AND window_start < to_ts
    AND ifNull(model_id, '') != ''
    AND pipeline = ''

  UNION ALL

  SELECT
    'v_api_sla_compliance' AS view_name,
    window_start,
    orchestrator_address,
    ifNull(model_id, '') AS model_id,
    ifNull(gpu_id, '') AS gpu_id
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= from_ts AND window_start < to_ts
    AND ifNull(model_id, '') != ''
    AND pipeline = ''
)
GROUP BY view_name, window_start, orchestrator_address, model_id, gpu_id
ORDER BY rows_with_empty_pipeline DESC
LIMIT 50;

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
  fps_jitter_coefficient
FROM livepeer_analytics.v_api_jitter_5m
WHERE window_start_5m >= from_ts
  AND window_start_5m < to_ts
  AND fps_jitter_coefficient > 0.2
ORDER BY fps_jitter_coefficient DESC, status_samples DESC;

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
