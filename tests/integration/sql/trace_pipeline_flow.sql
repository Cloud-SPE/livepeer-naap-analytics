-- Ordered data-trace query pack for Flink pipeline validation.
-- Run with params:
--   from_ts DateTime64(3)
--   to_ts   DateTime64(3)
--
-- Suggested default window: last 24h.

-- QUERY: 01_raw_ingest
SELECT
  'streaming_events' AS object_name,
  count() AS rows_window,
  min(event_timestamp) AS min_ts,
  max(event_timestamp) AS max_ts
FROM livepeer_analytics.streaming_events
WHERE event_timestamp >= {from_ts:DateTime64(3)}
  AND event_timestamp < {to_ts:DateTime64(3)}
UNION ALL
SELECT
  'streaming_events_dlq' AS object_name,
  count() AS rows_window,
  min(source_record_timestamp) AS min_ts,
  max(source_record_timestamp) AS max_ts
FROM livepeer_analytics.streaming_events_dlq
WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
  AND source_record_timestamp < {to_ts:DateTime64(3)}
UNION ALL
SELECT
  'streaming_events_quarantine' AS object_name,
  count() AS rows_window,
  min(source_record_timestamp) AS min_ts,
  max(source_record_timestamp) AS max_ts
FROM livepeer_analytics.streaming_events_quarantine
WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
  AND source_record_timestamp < {to_ts:DateTime64(3)};

-- QUERY: 02_typed_tables
SELECT
  object_name,
  rows_window,
  min_ts,
  max_ts
FROM
(
  SELECT
    'ai_stream_status' AS object_name,
    count() AS rows_window,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts
  FROM livepeer_analytics.ai_stream_status
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'stream_trace_events' AS object_name,
    count() AS rows_window,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts
  FROM livepeer_analytics.stream_trace_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'ai_stream_events' AS object_name,
    count() AS rows_window,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts
  FROM livepeer_analytics.ai_stream_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'stream_ingest_metrics' AS object_name,
    count() AS rows_window,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts
  FROM livepeer_analytics.stream_ingest_metrics
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'network_capabilities' AS object_name,
    count() AS rows_window,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts
  FROM livepeer_analytics.network_capabilities
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
)
ORDER BY object_name;

-- QUERY: 03_silver_projection_counts
SELECT
  object_name,
  rows_window,
  min_ts,
  max_ts
FROM
(
  SELECT
    'fact_stream_status_samples' AS object_name,
    count() AS rows_window,
    min(sample_ts) AS min_ts,
    max(sample_ts) AS max_ts
  FROM livepeer_analytics.fact_stream_status_samples
  WHERE sample_ts >= {from_ts:DateTime64(3)}
    AND sample_ts < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'fact_stream_trace_edges' AS object_name,
    count() AS rows_window,
    min(edge_ts) AS min_ts,
    max(edge_ts) AS max_ts
  FROM livepeer_analytics.fact_stream_trace_edges
  WHERE edge_ts >= {from_ts:DateTime64(3)}
    AND edge_ts < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'fact_stream_ingest_samples' AS object_name,
    count() AS rows_window,
    min(sample_ts) AS min_ts,
    max(sample_ts) AS max_ts
  FROM livepeer_analytics.fact_stream_ingest_samples
  WHERE sample_ts >= {from_ts:DateTime64(3)}
    AND sample_ts < {to_ts:DateTime64(3)}
)
ORDER BY object_name;

-- QUERY: 04_stateful_fact_counts
SELECT
  object_name,
  rows_window,
  min_ts,
  max_ts
FROM
(
  SELECT
    'fact_workflow_sessions' AS object_name,
    count() AS rows_window,
    min(session_start_ts) AS min_ts,
    max(session_start_ts) AS max_ts
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'fact_workflow_session_segments' AS object_name,
    count() AS rows_window,
    min(segment_start_ts) AS min_ts,
    max(segment_start_ts) AS max_ts
  FROM livepeer_analytics.fact_workflow_session_segments
  WHERE segment_start_ts >= {from_ts:DateTime64(3)}
    AND segment_start_ts < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT
    'fact_workflow_param_updates' AS object_name,
    count() AS rows_window,
    min(update_ts) AS min_ts,
    max(update_ts) AS max_ts
  FROM livepeer_analytics.fact_workflow_param_updates
  WHERE update_ts >= {from_ts:DateTime64(3)}
    AND update_ts < {to_ts:DateTime64(3)}

  -- UNION ALL

  -- SELECT
  --   'fact_lifecycle_edge_coverage' AS object_name,
  --   count() AS rows_window,
  --   min(signal_ts) AS min_ts,
  --   max(signal_ts) AS max_ts
  -- FROM livepeer_analytics.fact_lifecycle_edge_coverage
  -- WHERE signal_ts >= {from_ts:DateTime64(3)}
  --   AND signal_ts < {to_ts:DateTime64(3)}
)
ORDER BY object_name;

-- QUERY: 05_reliability_and_swap_summary
WITH fs_latest AS
(
  SELECT *
  FROM
  (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY workflow_session_id
        ORDER BY version DESC, session_start_ts DESC, session_end_ts DESC
      ) AS rn
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
  )
  WHERE rn = 1
)
SELECT
  count() AS sessions,
  sum(known_stream) AS known_stream_sessions,
  sum(startup_success) AS startup_success_sessions,
  sum(startup_excused) AS startup_excused_sessions,
  sum(startup_unexcused) AS startup_unexcused_sessions,
  sum(confirmed_swap_count > 0) AS confirmed_swapped_sessions,
  sum(inferred_orchestrator_change_count > 0) AS inferred_orchestrator_change_sessions,
  sum(swap_count > 0) AS swapped_sessions,
  avg(startup_unexcused) AS unexcused_rate
FROM fs_latest;

-- QUERY: 06_rollup_population
SELECT
  object_name,
  rows_window
FROM
(
  SELECT 'agg_stream_performance_1m' AS object_name, count() AS rows_window
  FROM livepeer_analytics.agg_stream_performance_1m
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'agg_reliability_1h' AS object_name, count() AS rows_window
  FROM livepeer_analytics.agg_reliability_1h
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}
)
ORDER BY object_name;

-- QUERY: 07_view_population
SELECT
  object_name,
  rows_window
FROM
(
  SELECT 'v_api_gpu_metrics' AS object_name, count() AS rows_window
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'v_api_network_demand' AS object_name, count() AS rows_window
  FROM livepeer_analytics.v_api_network_demand
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'v_api_sla_compliance' AS object_name, count() AS rows_window
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}
)
ORDER BY object_name;

-- QUERY: 08_gpu_view_parity
WITH
  rollup AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      avgMerge(output_fps_avg_state) AS avg_output_fps
    FROM livepeer_analytics.agg_stream_performance_1m
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      avg_output_fps
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      avg(abs(r.avg_output_fps - a.avg_output_fps)) AS mean_abs_diff_fps,
      max(abs(r.avg_output_fps - a.avg_output_fps)) AS max_abs_diff_fps
    FROM rollup r
    INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
  )
SELECT
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND ifNull(max_abs_diff_fps, 0) > 0.000001, 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  rollup_rows,
  view_rows,
  joined_rows,
  rollup_only_keys,
  view_only_keys,
  rollup_empty_orch_rows,
  rollup_empty_gpu_rows,
  rollup_empty_region_rows,
  view_empty_orch_rows,
  view_empty_gpu_rows,
  view_empty_region_rows,
  mean_abs_diff_fps,
  max_abs_diff_fps
FROM
(
  SELECT
    (SELECT count() FROM rollup) AS rollup_rows,
    (SELECT count() FROM api) AS view_rows,
    (SELECT joined_rows FROM joined) AS joined_rows,
    (
      SELECT count()
      FROM rollup r
      LEFT JOIN api a
        USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
      WHERE a.api_marker IS NULL
    ) AS rollup_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN rollup r
        USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
      WHERE r.rollup_marker IS NULL
    ) AS view_only_keys,
    (SELECT countIf(orchestrator_address = '') FROM rollup) AS rollup_empty_orch_rows,
    (SELECT countIf(gpu_id = '') FROM rollup) AS rollup_empty_gpu_rows,
    (SELECT countIf(region = '') FROM rollup) AS rollup_empty_region_rows,
    (SELECT countIf(orchestrator_address = '') FROM api) AS view_empty_orch_rows,
    (SELECT countIf(gpu_id = '') FROM api) AS view_empty_gpu_rows,
    (SELECT countIf(region = '') FROM api) AS view_empty_region_rows,
    (SELECT mean_abs_diff_fps FROM joined) AS mean_abs_diff_fps,
    (SELECT max_abs_diff_fps FROM joined) AS max_abs_diff_fps
);

-- QUERY: 09_sla_view_parity
WITH
  latest_sessions AS (
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
  ),
  raw AS (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key,
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
  ),
  api AS (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key,
      known_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  multiIf(
    raw_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    raw_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND (ifNull(total_known_diff, 0) > 0 OR ifNull(total_unexcused_diff, 0) > 0 OR ifNull(total_swapped_diff, 0) > 0), 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  raw_rows,
  view_rows,
  joined_rows,
  raw_only_keys,
  view_only_keys,
  total_known_diff,
  total_unexcused_diff,
  total_swapped_diff
FROM
(
  SELECT
    (SELECT count() FROM raw) AS raw_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM raw
      INNER JOIN api
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS joined_rows,
    (
      SELECT count()
      FROM raw r
      LEFT JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE a.window_start IS NULL
    ) AS raw_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN raw r
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE r.window_start IS NULL
    ) AS view_only_keys,
    (
      SELECT sum(abs(r.raw_known_sessions - a.known_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_known_diff,
    (
      SELECT sum(abs(r.raw_unexcused_sessions - a.unexcused_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_unexcused_diff,
    (
      SELECT sum(abs(r.raw_swapped_sessions - a.swapped_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_swapped_diff
);

-- QUERY: 10_network_demand_view_parity
WITH
  perf_1h AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id,
      uniqExactMerge(sessions_uniq_state) AS total_sessions,
      uniqExactMerge(streams_uniq_state) AS total_streams,
      countMerge(sample_count_state) / 60.0 AS total_inference_minutes,
      avgMerge(output_fps_avg_state) AS avg_output_fps
    FROM livepeer_analytics.agg_stream_performance_1m
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  demand_1h AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      pipeline,
      model_id,
      sum(toUInt64(known_stream)) AS known_sessions,
      sum(toUInt64(known_stream AND orchestrator_address != '')) AS served_sessions,
      sum(toUInt64(known_stream AND orchestrator_address = '')) AS unserved_sessions,
      sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  expected AS
  (
    SELECT
      p.rollup_marker AS expected_marker,
      p.window_start,
      p.gateway,
      p.region,
      p.pipeline,
      p.model_id,
      p.total_sessions,
      p.total_streams,
      p.total_inference_minutes,
      p.avg_output_fps,
      ifNull(d.known_sessions, toUInt64(0)) AS known_sessions,
      ifNull(d.served_sessions, toUInt64(0)) AS served_sessions,
      ifNull(d.unserved_sessions, toUInt64(0)) AS unserved_sessions,
      ifNull(d.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
      ifNull(d.swapped_sessions, toUInt64(0)) AS swapped_sessions
    FROM perf_1h p
    LEFT JOIN demand_1h d
      USING (window_start, gateway, region, pipeline, model_id)
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id,
      total_sessions,
      total_streams,
      total_inference_minutes,
      avg_output_fps,
      known_sessions,
      served_sessions,
      unserved_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      avg(abs(e.avg_output_fps - a.avg_output_fps)) AS mean_abs_diff_fps,
      max(abs(e.avg_output_fps - a.avg_output_fps)) AS max_abs_diff_fps,
      avg(abs(e.total_inference_minutes - a.total_inference_minutes)) AS mean_abs_diff_minutes,
      max(abs(e.total_inference_minutes - a.total_inference_minutes)) AS max_abs_diff_minutes,
      sum(abs(toInt64(e.total_sessions) - toInt64(a.total_sessions))) AS total_diff_sessions,
      sum(abs(toInt64(e.total_streams) - toInt64(a.total_streams))) AS total_diff_streams,
      sum(abs(toInt64(e.known_sessions) - toInt64(a.known_sessions))) AS total_diff_known_sessions,
      sum(abs(toInt64(e.served_sessions) - toInt64(a.served_sessions))) AS total_diff_served_sessions,
      sum(abs(toInt64(e.unserved_sessions) - toInt64(a.unserved_sessions))) AS total_diff_unserved_sessions,
      sum(abs(toInt64(e.unexcused_sessions) - toInt64(a.unexcused_sessions))) AS total_diff_unexcused_sessions,
      sum(abs(toInt64(e.swapped_sessions) - toInt64(a.swapped_sessions))) AS total_diff_swapped_sessions
    FROM expected e
    INNER JOIN api a
      USING (window_start, gateway, region, pipeline, model_id)
  )
SELECT
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND (
      ifNull(max_abs_diff_fps, 0) > 0.000001
      OR ifNull(max_abs_diff_minutes, 0) > 0.000001
      OR ifNull(total_diff_sessions, 0) > 0
      OR ifNull(total_diff_streams, 0) > 0
      OR ifNull(total_diff_known_sessions, 0) > 0
      OR ifNull(total_diff_served_sessions, 0) > 0
      OR ifNull(total_diff_unserved_sessions, 0) > 0
      OR ifNull(total_diff_unexcused_sessions, 0) > 0
      OR ifNull(total_diff_swapped_sessions, 0) > 0
    ), 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  rollup_rows,
  view_rows,
  joined_rows,
  rollup_only_keys,
  view_only_keys,
  mean_abs_diff_fps,
  max_abs_diff_fps,
  mean_abs_diff_minutes,
  max_abs_diff_minutes,
  total_diff_sessions,
  total_diff_streams,
  total_diff_known_sessions,
  total_diff_served_sessions,
  total_diff_unserved_sessions,
  total_diff_unexcused_sessions,
  total_diff_swapped_sessions
FROM
(
  SELECT
    (SELECT count() FROM expected) AS rollup_rows,
    (SELECT count() FROM api) AS view_rows,
    (SELECT joined_rows FROM joined) AS joined_rows,
    (
      SELECT count()
      FROM expected e
      LEFT JOIN api a
        USING (window_start, gateway, region, pipeline, model_id)
      WHERE a.api_marker IS NULL
    ) AS rollup_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN expected e
        USING (window_start, gateway, region, pipeline, model_id)
      WHERE e.expected_marker IS NULL
    ) AS view_only_keys,
    (SELECT mean_abs_diff_fps FROM joined) AS mean_abs_diff_fps,
    (SELECT max_abs_diff_fps FROM joined) AS max_abs_diff_fps,
    (SELECT mean_abs_diff_minutes FROM joined) AS mean_abs_diff_minutes,
    (SELECT max_abs_diff_minutes FROM joined) AS max_abs_diff_minutes,
    (SELECT total_diff_sessions FROM joined) AS total_diff_sessions,
    (SELECT total_diff_streams FROM joined) AS total_diff_streams,
    (SELECT total_diff_known_sessions FROM joined) AS total_diff_known_sessions,
    (SELECT total_diff_served_sessions FROM joined) AS total_diff_served_sessions,
    (SELECT total_diff_unserved_sessions FROM joined) AS total_diff_unserved_sessions,
    (SELECT total_diff_unexcused_sessions FROM joined) AS total_diff_unexcused_sessions,
    (SELECT total_diff_swapped_sessions FROM joined) AS total_diff_swapped_sessions
);
