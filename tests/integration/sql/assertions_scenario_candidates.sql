-- Assertions that ensure candidate sessions exist for each key scenario.
-- Intended for production snapshot selection windows.
-- Params:
--   from_ts DateTime64(3)
--   to_ts   DateTime64(3)

-- TEST: scenario_1_clean_success_no_swap_fps_gt_12_exists
WITH canonical_cap_wallets AS
(
  SELECT DISTINCT lower(orchestrator_address) AS wallet
  FROM livepeer_analytics.network_capabilities
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
    AND orchestrator_address != ''
)
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS candidates
FROM
(
  SELECT fs.workflow_session_id
  FROM
  (
    SELECT *
    FROM livepeer_analytics.fact_workflow_sessions FINAL
  ) fs
  INNER JOIN
  (
    SELECT workflow_session_id
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
    HAVING avg(output_fps) > 12
  ) fps USING (workflow_session_id)
  WHERE fs.session_start_ts >= {from_ts:DateTime64(3)}
    AND fs.session_start_ts < {to_ts:DateTime64(3)}
    AND fs.known_stream = 1
    AND fs.startup_success = 1
    AND fs.startup_unexcused = 0
    -- Scenario 1 assertion remains confirmed-no-swap for fixture stability.
    AND fs.swap_count = 0
    AND fs.orchestrator_address != ''
    AND lower(fs.orchestrator_address) IN (SELECT wallet FROM canonical_cap_wallets)
  LIMIT 1
);

-- TEST: scenario_2_no_orchestrator_then_closed_exists
WITH flags AS
(
  SELECT
    workflow_session_id,
    max(toUInt8(trace_type = 'gateway_no_orchestrators_available')) AS has_no_orch,
    max(toUInt8(trace_type = 'gateway_ingest_stream_closed')) AS has_close
  FROM livepeer_analytics.fact_stream_trace_edges
  WHERE edge_ts >= {from_ts:DateTime64(3)}
    AND edge_ts < {to_ts:DateTime64(3)}
  GROUP BY workflow_session_id
)
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS candidates
FROM
(
  SELECT fs.workflow_session_id
  FROM
  (
    SELECT *
    FROM livepeer_analytics.fact_workflow_sessions FINAL
  ) fs
  INNER JOIN flags f USING (workflow_session_id)
  WHERE fs.session_start_ts >= {from_ts:DateTime64(3)}
    AND fs.session_start_ts < {to_ts:DateTime64(3)}
    AND fs.startup_success = 0
    AND f.has_no_orch = 1
    AND f.has_close = 1
  LIMIT 1
);

-- TEST: scenario_3_success_with_swap_exists
-- Swap diagnostics are explicit to prevent ambiguity:
-- - explicit: confirmed_swap_count > 0
-- - derived: inferred_orchestrator_change_count > 0 OR >1 orchestrator segment
WITH seg_stats AS
(
  SELECT
    workflow_session_id,
    uniqExactIf(orchestrator_address, orchestrator_address != '') AS segment_orchestrators
  FROM livepeer_analytics.fact_workflow_session_segments FINAL
  WHERE segment_start_ts >= {from_ts:DateTime64(3)}
    AND segment_start_ts < {to_ts:DateTime64(3)}
  GROUP BY workflow_session_id
),
candidates AS
(
  SELECT
    fs.workflow_session_id AS workflow_session_id,
    toUInt8(fs.confirmed_swap_count > 0) AS explicit_swap,
    toUInt8(
      fs.inferred_orchestrator_change_count > 0
      OR ifNull(seg.segment_orchestrators, 0) > 1
    ) AS derived_swap
  FROM
  (
    SELECT *
    FROM livepeer_analytics.fact_workflow_sessions FINAL
  ) fs
  LEFT JOIN seg_stats seg
    ON seg.workflow_session_id = fs.workflow_session_id
  WHERE fs.session_start_ts >= {from_ts:DateTime64(3)}
    AND fs.session_start_ts < {to_ts:DateTime64(3)}
    AND fs.startup_success = 1
    AND (
      fs.confirmed_swap_count > 0
      OR fs.inferred_orchestrator_change_count > 0
      OR ifNull(seg.segment_orchestrators, 0) > 1
    )
)
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS candidates,
  countIf(explicit_swap = 1) AS explicit_candidates,
  countIf(derived_swap = 1) AS derived_candidates
FROM candidates;

-- TEST: scenario_4_success_with_param_updates_exists
-- Informational only for now: production currently has zero rows in
-- fact_workflow_param_updates, so this check should not block the suite.
SELECT
  toUInt64(0) AS failed_rows,
  count() AS candidates
FROM
(
  SELECT fs.workflow_session_id
  FROM
  (
    SELECT *
    FROM livepeer_analytics.fact_workflow_sessions FINAL
  ) fs
  INNER JOIN livepeer_analytics.fact_workflow_param_updates pu
    ON pu.workflow_session_id = fs.workflow_session_id
  WHERE fs.session_start_ts >= {from_ts:DateTime64(3)}
    AND fs.session_start_ts < {to_ts:DateTime64(3)}
    AND fs.startup_success = 1
  LIMIT 1
);

-- TEST: scenario_6_same_hour_failed_no_output_retained_in_views
WITH candidates AS
(
  WITH fs_latest AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(session_end_ts, version) AS session_end_ts,
      argMax(last_error_occurred, version) AS last_error_occurred,
      argMax(loading_only_session, version) AS loading_only_session,
      argMax(zero_output_fps_session, version) AS zero_output_fps_session
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  status_1h AS
  (
    SELECT
      toStartOfInterval(sample_ts, INTERVAL 1 HOUR) AS window_start,
      workflow_session_id,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key,
      count() AS status_samples,
      countIf(output_fps > 0) AS fps_positive_samples,
      countIf(state IN ('ONLINE', 'DEGRADED_INFERENCE', 'DEGRADED_INPUT')) AS running_state_samples
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
      AND is_attributed = 1
    GROUP BY window_start, workflow_session_id, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
  )
  SELECT
    s.window_start,
    s.orchestrator_address,
    s.pipeline,
    s.model_id_key,
    s.gpu_id_key,
    s.region_key
  FROM status_1h s
  INNER JOIN fs_latest fs
    ON fs.workflow_session_id = s.workflow_session_id
  WHERE s.fps_positive_samples = 0
    AND s.running_state_samples = 0
    AND s.status_samples < 3
    AND fs.session_start_ts >= s.window_start
    AND fs.session_start_ts < s.window_start + INTERVAL 1 HOUR
    AND fs.session_end_ts >= s.window_start
    AND fs.session_end_ts < s.window_start + INTERVAL 1 HOUR
    AND (fs.last_error_occurred > 0 OR fs.loading_only_session > 0 OR fs.zero_output_fps_session > 0)
    AND s.orchestrator_address != ''
    AND s.gpu_id_key != ''
  GROUP BY s.window_start, s.orchestrator_address, s.pipeline, s.model_id_key, s.gpu_id_key, s.region_key
),
gpu_missing AS
(
  SELECT count() AS c
  FROM candidates a
  LEFT JOIN livepeer_analytics.v_api_gpu_metrics g
    ON g.window_start = a.window_start
   AND g.orchestrator_address = a.orchestrator_address
   AND g.pipeline = a.pipeline
   AND ifNull(g.model_id, '') = a.model_id_key
   AND ifNull(g.gpu_id, '') = a.gpu_id_key
   AND ifNull(g.region, '') = a.region_key
  WHERE g.window_start IS NULL
),
sla_missing AS
(
  SELECT count() AS c
  FROM candidates a
  LEFT JOIN livepeer_analytics.v_api_sla_compliance s
    ON s.window_start = a.window_start
   AND s.orchestrator_address = a.orchestrator_address
   AND s.pipeline = a.pipeline
   AND ifNull(s.model_id, '') = a.model_id_key
   AND ifNull(s.gpu_id, '') = a.gpu_id_key
   AND ifNull(s.region, '') = a.region_key
  WHERE s.window_start IS NULL
)
SELECT
  toUInt64(
    if(
      (SELECT count() FROM candidates) = 0,
      0,
      (SELECT c FROM gpu_missing) + (SELECT c FROM sla_missing)
    ) > 0
  ) AS failed_rows,
  (SELECT count() FROM candidates) AS candidates,
  (SELECT c FROM gpu_missing) AS gpu_missing_rows,
  (SELECT c FROM sla_missing) AS sla_missing_rows;

-- TEST: scenario_7_rollover_tail_filtered_in_views
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    argMax(session_start_ts, version) AS session_start_ts,
    argMax(session_end_ts, version) AS session_end_ts
  FROM livepeer_analytics.fact_workflow_sessions
  GROUP BY workflow_session_id
),
status_1h AS
(
  SELECT
    toStartOfInterval(sample_ts, INTERVAL 1 HOUR) AS window_start,
    workflow_session_id,
    orchestrator_address,
    pipeline,
    ifNull(model_id, '') AS model_id_key,
    ifNull(gpu_id, '') AS gpu_id_key,
    ifNull(region, '') AS region_key,
    count() AS status_samples,
    countIf(output_fps > 0) AS fps_positive_samples,
    countIf(state IN ('ONLINE', 'DEGRADED_INFERENCE', 'DEGRADED_INPUT')) AS running_state_samples
  FROM livepeer_analytics.fact_stream_status_samples
  WHERE sample_ts >= {from_ts:DateTime64(3)} - INTERVAL 1 HOUR
    AND sample_ts < {to_ts:DateTime64(3)}
    AND is_attributed = 1
  GROUP BY window_start, workflow_session_id, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
),
candidates_with_presence AS
(
  SELECT
    s.window_start AS window_start,
    s.orchestrator_address AS orchestrator_address,
    s.pipeline AS pipeline,
    s.model_id_key AS model_id_key,
    s.gpu_id_key AS gpu_id_key,
    s.region_key AS region_key,
    toUInt8(g.window_start IS NOT NULL) AS present_in_gpu_view,
    toUInt8(sl.window_start IS NOT NULL) AS present_in_sla_view
  FROM status_1h s
  INNER JOIN fs_latest fs
    ON fs.workflow_session_id = s.workflow_session_id
  INNER JOIN status_1h p
    ON p.workflow_session_id = s.workflow_session_id
   AND p.window_start = s.window_start - INTERVAL 1 HOUR
   AND p.orchestrator_address = s.orchestrator_address
   AND p.pipeline = s.pipeline
   AND p.model_id_key = s.model_id_key
   AND p.gpu_id_key = s.gpu_id_key
   AND p.region_key = s.region_key
  LEFT JOIN livepeer_analytics.v_api_gpu_metrics g
    ON g.window_start = s.window_start
   AND g.orchestrator_address = s.orchestrator_address
   AND g.pipeline = s.pipeline
   AND ifNull(g.model_id, '') = s.model_id_key
   AND ifNull(g.gpu_id, '') = s.gpu_id_key
   AND ifNull(g.region, '') = s.region_key
  LEFT JOIN livepeer_analytics.v_api_sla_compliance sl
    ON sl.window_start = s.window_start
   AND sl.orchestrator_address = s.orchestrator_address
   AND sl.pipeline = s.pipeline
   AND ifNull(sl.model_id, '') = s.model_id_key
   AND ifNull(sl.gpu_id, '') = s.gpu_id_key
   AND ifNull(sl.region, '') = s.region_key
  WHERE fs.session_start_ts < s.window_start
    AND fs.session_end_ts >= s.window_start
    AND fs.session_end_ts < s.window_start + INTERVAL 1 HOUR
    AND s.fps_positive_samples = 0
    AND s.running_state_samples = 0
    AND s.status_samples < 3
    AND p.fps_positive_samples = 0
    AND p.running_state_samples = 0
    AND p.status_samples < 3
  GROUP BY
    s.window_start,
    s.orchestrator_address,
    s.pipeline,
    s.model_id_key,
    s.gpu_id_key,
    s.region_key,
    present_in_gpu_view,
    present_in_sla_view
)
SELECT
  toUInt64(
    if(
      (SELECT count() FROM candidates_with_presence) = 0,
      0,
      (
        SELECT countIf(present_in_gpu_view = 1) + countIf(present_in_sla_view = 1)
        FROM candidates_with_presence
      )
    ) > 0
  ) AS failed_rows,
  (SELECT count() FROM candidates_with_presence) AS candidates,
  (SELECT countIf(present_in_gpu_view = 1) FROM candidates_with_presence) AS gpu_rows_found,
  (SELECT countIf(present_in_sla_view = 1) FROM candidates_with_presence) AS sla_rows_found;

-- TEST: scenario_5_out_of_category_baseline_exists
-- Informational: sampled subset of notebook fallout_df semantics.
SELECT
  toUInt64(0) AS failed_rows,
  count() AS candidates
FROM
(
  WITH fs_latest AS
  (
    SELECT
      workflow_session_id,
      startup_success,
      known_stream,
      startup_unexcused,
      confirmed_swap_count,
      inferred_orchestrator_change_count,
      swap_count
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
  ),
  status_stats AS
  (
    SELECT workflow_session_id, avg(output_fps) AS avg_output_fps
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  trace_flags AS
  (
    SELECT
      workflow_session_id,
      max(toUInt8(trace_type = 'gateway_no_orchestrators_available')) AS has_no_orch,
      max(toUInt8(trace_type = 'gateway_ingest_stream_closed')) AS has_close
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  seg_stats AS
  (
    SELECT
      workflow_session_id,
      uniqExactIf(orchestrator_address, orchestrator_address != '') AS segment_orchestrators
    FROM livepeer_analytics.fact_workflow_session_segments FINAL
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  update_counts AS
  (
    SELECT workflow_session_id, count() AS updates
    FROM livepeer_analytics.fact_workflow_param_updates
    WHERE update_ts >= {from_ts:DateTime64(3)}
      AND update_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  )
  SELECT fs.workflow_session_id
  FROM fs_latest fs
  LEFT JOIN status_stats ss USING (workflow_session_id)
  LEFT JOIN trace_flags tf USING (workflow_session_id)
  LEFT JOIN seg_stats seg USING (workflow_session_id)
  LEFT JOIN update_counts uc USING (workflow_session_id)
  WHERE
    NOT (
      fs.known_stream = 1
      AND fs.startup_success = 1
      AND fs.startup_unexcused = 0
      AND fs.swap_count = 0
      AND ifNull(ss.avg_output_fps, 0.0) > 12
    )
    AND NOT (
      fs.startup_success = 0
      AND ifNull(tf.has_no_orch, 0) = 1
      AND ifNull(tf.has_close, 0) = 1
    )
    AND NOT (
      fs.startup_success = 1
      AND (
        fs.confirmed_swap_count > 0
        OR fs.inferred_orchestrator_change_count > 0
        OR ifNull(seg.segment_orchestrators, 0) > 1
        OR fs.swap_count > 0
      )
    )
    AND NOT (
      fs.startup_success = 1
      AND ifNull(uc.updates, 0) > 0
    )
  LIMIT 1
);
