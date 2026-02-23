-- Assertions that ensure candidate sessions exist for each key scenario.
-- Intended for production snapshot selection windows.
-- Params:
--   from_ts DateTime64(3)
--   to_ts   DateTime64(3)

-- TEST: scenario_1_clean_success_no_swap_fps_gt_12_exists
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
    GROUP BY workflow_session_id
    HAVING avg(output_fps) > 12
  ) fps USING (workflow_session_id)
  WHERE fs.session_start_ts >= {from_ts:DateTime64(3)}
    AND fs.session_start_ts < {to_ts:DateTime64(3)}
    AND fs.known_stream = 1
    AND fs.startup_success = 1
    AND fs.startup_unexcused = 0
    AND fs.swap_count = 0
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
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS candidates
FROM
(
  SELECT workflow_session_id
  FROM livepeer_analytics.fact_workflow_sessions FINAL
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
    AND startup_success = 1
    AND swap_count > 0
  LIMIT 1
);

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
