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
-- - legacy: swap_count > 0 (fallback only)
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
    ) AS derived_swap,
    toUInt8(fs.swap_count > 0) AS legacy_swap
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
      OR fs.swap_count > 0
    )
)
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS candidates,
  countIf(explicit_swap = 1) AS explicit_candidates,
  countIf(derived_swap = 1) AS derived_candidates,
  countIf(legacy_swap = 1) AS legacy_candidates
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
