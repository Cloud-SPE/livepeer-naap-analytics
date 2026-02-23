-- Integration assertions for raw -> typed -> facts -> rollups -> API views.
-- Each test must return exactly one row with:
--   failed_rows UInt64/Int64
-- Additional columns are treated as diagnostics.

-- TEST: raw_events_present
SELECT
  countIf(rows_window = 0) AS failed_rows,
  groupArrayIf(object_name, rows_window = 0) AS missing_objects
FROM
(
  SELECT 'streaming_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.streaming_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'ai_stream_status' AS object_name, count() AS rows_window
  FROM livepeer_analytics.ai_stream_status
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'stream_trace_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.stream_trace_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
);

-- TEST: capability_dimension_mvs_present
SELECT
  countIf(mv_exists = 0) AS failed_rows,
  groupArrayIf(mv_name, mv_exists = 0) AS missing_mvs
FROM
(
  SELECT
    mv_name,
    toUInt8(count() > 0) AS mv_exists
  FROM
  (
    SELECT 'mv_network_capabilities_to_dim_orchestrator_capability_snapshots' AS mv_name
    UNION ALL SELECT 'mv_network_capabilities_advertised_to_dim_orchestrator_capability_advertised_snapshots'
    UNION ALL SELECT 'mv_network_capabilities_model_constraints_to_dim_orchestrator_capability_model_constraints'
    UNION ALL SELECT 'mv_network_capabilities_prices_to_dim_orchestrator_capability_prices'
  ) required
  LEFT JOIN
  (
    SELECT name
    FROM system.tables
    WHERE database = 'livepeer_analytics'
      AND engine = 'MaterializedView'
  ) present
    ON required.mv_name = present.name
  GROUP BY mv_name
);

-- TEST: capability_dimensions_projecting
WITH
  raw_rows AS
  (
    SELECT
      (SELECT count() FROM livepeer_analytics.network_capabilities WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS caps_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_advertised WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS adv_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_model_constraints WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS mc_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_prices WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS price_rows
  ),
  dim_rows AS
  (
    SELECT
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_snapshots WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_caps_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_advertised_snapshots WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_adv_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_model_constraints WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_mc_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_prices WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_price_rows
  )
SELECT
  toUInt64(
    if(caps_rows = 0, 0, dim_caps_rows = 0)
    + if(adv_rows = 0, 0, dim_adv_rows = 0)
    + if(mc_rows = 0, 0, dim_mc_rows = 0)
    + if(price_rows = 0, 0, dim_price_rows = 0)
  ) AS failed_rows,
  caps_rows,
  dim_caps_rows,
  adv_rows,
  dim_adv_rows,
  mc_rows,
  dim_mc_rows,
  price_rows,
  dim_price_rows
FROM raw_rows
CROSS JOIN dim_rows;

-- TEST: session_fact_present
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS sessions_window
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: status_raw_to_silver_projection
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: trace_raw_to_silver_projection
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: ingest_raw_to_silver_projection
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.stream_ingest_metrics
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_ingest_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: session_final_uniqueness
WITH window_rows AS
(
  SELECT workflow_session_id, version
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
),
latest_versions AS
(
  SELECT workflow_session_id, max(version) AS max_version
  FROM window_rows
  GROUP BY workflow_session_id
),
latest_rows AS
(
  SELECT
    w.workflow_session_id,
    count() AS latest_row_count
  FROM window_rows w
  INNER JOIN latest_versions lv
    ON lv.workflow_session_id = w.workflow_session_id
   AND lv.max_version = w.version
  GROUP BY w.workflow_session_id
)
SELECT
  countIf(latest_row_count != 1) AS failed_rows,
  count() AS unique_sessions_checked
FROM latest_rows;

-- TEST: workflow_session_has_identifier
SELECT
  countIf(workflow_session_id = '') AS failed_rows,
  count() AS total_rows
FROM livepeer_analytics.fact_workflow_sessions FINAL
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: swapped_sessions_have_evidence
WITH
  swapped AS (
    SELECT workflow_session_id
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND swap_count > 0
  ),
  seg AS (
    SELECT
      workflow_session_id,
      uniqExactIf(orchestrator_address, orchestrator_address != '') AS orch_count
    FROM livepeer_analytics.fact_workflow_session_segments FINAL
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  edge AS (
    SELECT
      workflow_session_id,
      max(toUInt8(is_swap_event > 0)) AS has_swap_edge
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  )
SELECT
  count() AS failed_rows,
  count() AS swapped_without_evidence
FROM swapped s
LEFT JOIN seg USING (workflow_session_id)
LEFT JOIN edge USING (workflow_session_id)
WHERE ifNull(seg.orch_count, 0) <= 1
  AND ifNull(edge.has_swap_edge, 0) = 0;

-- TEST: param_updates_reference_existing_session
WITH session_ids AS
(
  SELECT DISTINCT workflow_session_id
  FROM livepeer_analytics.fact_workflow_sessions FINAL
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
)
SELECT
  count() AS failed_rows,
  count() AS orphan_param_updates
FROM livepeer_analytics.fact_workflow_param_updates pu
LEFT JOIN session_ids s USING (workflow_session_id)
WHERE pu.update_ts >= {from_ts:DateTime64(3)}
  AND pu.update_ts < {to_ts:DateTime64(3)}
  AND s.workflow_session_id IS NULL;

-- TEST: gpu_view_matches_rollup
SELECT
  toUInt64(ifNull(max_abs_diff_fps, 0) > 0.000001) AS failed_rows,
  joined_rows,
  mean_abs_diff_fps,
  max_abs_diff_fps
FROM
(
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
  USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
);

-- TEST: sla_view_matches_session_fact
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
      argMax(swap_count, version) AS swap_count
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
      sum(toUInt64(swap_count > 0)) AS raw_swapped_sessions
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
  toUInt64(total_known_diff > 0 OR total_unexcused_diff > 0 OR total_swapped_diff > 0) AS failed_rows,
  joined_rows,
  total_known_diff,
  total_unexcused_diff,
  total_swapped_diff
FROM
(
  SELECT
    count() AS joined_rows,
    sum(abs(raw.raw_known_sessions - api.known_sessions)) AS total_known_diff,
    sum(abs(raw.raw_unexcused_sessions - api.unexcused_sessions)) AS total_unexcused_diff,
    sum(abs(raw.raw_swapped_sessions - api.swapped_sessions)) AS total_swapped_diff
  FROM raw
  INNER JOIN api
  USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
);

-- TEST: sla_ratios_in_bounds
SELECT
  countIf(success_ratio < 0 OR success_ratio > 1)
  + countIf(no_swap_ratio < 0 OR no_swap_ratio > 1) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};
