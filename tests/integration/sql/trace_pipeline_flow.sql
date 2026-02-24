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
SELECT
  count() AS sessions,
  sum(known_stream) AS known_stream_sessions,
  sum(startup_success) AS startup_success_sessions,
  sum(startup_excused) AS startup_excused_sessions,
  sum(startup_unexcused) AS startup_unexcused_sessions,
  sum(confirmed_swap_count > 0) AS confirmed_swapped_sessions,
  sum(inferred_orchestrator_change_count > 0) AS inferred_orchestrator_change_sessions,
  sum((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0)) AS swapped_sessions,
  avg(startup_unexcused) AS unexcused_rate
FROM livepeer_analytics.fact_workflow_sessions FINAL
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

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
SELECT
  count() AS joined_rows,
  avg(abs(a.avg_output_fps - b.avg_output_fps)) AS mean_abs_diff_fps,
  max(abs(a.avg_output_fps - b.avg_output_fps)) AS max_abs_diff_fps
FROM
(
  SELECT
    window_start,
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
) a
INNER JOIN
(
  SELECT
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
) b
USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region);

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
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  api AS (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      known_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  count() AS joined_rows,
  sum(abs(raw.raw_known_sessions - api.known_sessions)) AS total_known_diff,
  sum(abs(raw.raw_unexcused_sessions - api.unexcused_sessions)) AS total_unexcused_diff,
  sum(abs(raw.raw_swapped_sessions - api.swapped_sessions)) AS total_swapped_diff
FROM raw
INNER JOIN api
USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region);
