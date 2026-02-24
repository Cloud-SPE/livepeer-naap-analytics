-- Candidate sessions for scenario fixture extraction.
-- Params:
--   from_ts DateTime64(3)
--   to_ts   DateTime64(3)
--   limit_per_scenario UInt32

-- Each query canonicalizes fact_workflow_sessions to one row per
-- workflow_session_id before applying scenario predicates. This prevents
-- duplicate logical sessions (and cross-scenario ambiguity) when multiple
-- fact rows exist for the same session id.

-- QUERY: scenario_1_clean_success_no_swap_fps_gt_12
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    session_start_ts,
    session_end_ts,
    known_stream,
    startup_success,
    startup_unexcused,
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
)
SELECT
  'scenario_1_clean_success_no_swap_fps_gt_12' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  avg(ss.output_fps) AS avg_output_fps,
  uniqExactIf(seg.orchestrator_address, seg.orchestrator_address != '') AS segment_orchestrators
FROM fs_latest fs
LEFT JOIN livepeer_analytics.fact_stream_status_samples ss
  ON ss.workflow_session_id = fs.workflow_session_id
LEFT JOIN
(
  SELECT *
  FROM livepeer_analytics.fact_workflow_session_segments FINAL
) seg
  ON seg.workflow_session_id = fs.workflow_session_id
WHERE fs.known_stream = 1
  AND fs.startup_success = 1
  AND fs.startup_unexcused = 0
  AND fs.swap_count = 0
GROUP BY
  fs.workflow_session_id,
  fs.stream_id,
  fs.request_id,
  fs.session_start_ts,
  fs.session_end_ts
HAVING avg_output_fps > 12
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};

-- QUERY: scenario_2_no_orchestrator_then_closed
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    session_start_ts,
    session_end_ts,
    startup_success,
    startup_excused,
    startup_unexcused
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
)
SELECT
  'scenario_2_no_orchestrator_then_closed' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  fs.startup_success AS startup_success,
  fs.startup_excused AS startup_excused,
  fs.startup_unexcused AS startup_unexcused,
  tf.has_no_orch AS has_no_orch,
  tf.has_close AS has_close
FROM fs_latest fs
INNER JOIN trace_flags tf
  ON tf.workflow_session_id = fs.workflow_session_id
WHERE fs.startup_success = 0
  AND tf.has_no_orch = 1
  AND tf.has_close = 1
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};

-- QUERY: scenario_3_success_with_swap
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    session_start_ts,
    session_end_ts,
    startup_success,
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
)
SELECT
  'scenario_3_success_with_swap' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  fs.swap_count AS swap_count,
  uniqExactIf(seg.orchestrator_address, seg.orchestrator_address != '') AS segment_orchestrators
FROM fs_latest fs
LEFT JOIN
(
  SELECT *
  FROM livepeer_analytics.fact_workflow_session_segments FINAL
) seg
  ON seg.workflow_session_id = fs.workflow_session_id
WHERE fs.startup_success = 1
  AND fs.swap_count > 0
GROUP BY
  fs.workflow_session_id,
  fs.stream_id,
  fs.request_id,
  fs.session_start_ts,
  fs.session_end_ts,
  fs.swap_count
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};

-- QUERY: scenario_4_success_with_param_updates
WITH update_counts AS
(
  SELECT
    workflow_session_id,
    count() AS updates
  FROM livepeer_analytics.fact_workflow_param_updates
  WHERE update_ts >= {from_ts:DateTime64(3)}
    AND update_ts < {to_ts:DateTime64(3)}
  GROUP BY workflow_session_id
),
fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    session_start_ts,
    session_end_ts,
    startup_success
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
  'scenario_4_success_with_param_updates' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  uc.updates AS updates
FROM fs_latest fs
INNER JOIN update_counts uc
  ON uc.workflow_session_id = fs.workflow_session_id
WHERE fs.startup_success = 1
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};
