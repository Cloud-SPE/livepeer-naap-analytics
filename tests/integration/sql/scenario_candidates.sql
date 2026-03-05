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
    orchestrator_address,
    session_start_ts,
    session_end_ts,
    known_stream,
    startup_success,
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
cap_wallets AS
(
  -- Strict contract check for scenario extraction: typed/session identities
  -- should line up with canonical capability wallets. Avoid fallback joins
  -- here so normalization regressions surface in notebook output.
  SELECT DISTINCT lower(orchestrator_address) AS wallet
  FROM livepeer_analytics.raw_network_capabilities
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
    AND orchestrator_address != ''
),
wallet_overlap_sessions AS
(
  -- Some sessions can miss session-level overlap when attribution is delayed.
  -- Accept overlap from either the latest session orchestrator or any segment orchestrator
  -- for the same workflow session so discovery remains stable while preserving canonical checks.
  SELECT DISTINCT workflow_session_id
  FROM
  (
    SELECT
      fs.workflow_session_id AS workflow_session_id,
      lower(fs.orchestrator_address) AS wallet
    FROM fs_latest fs
    WHERE fs.orchestrator_address != ''
    UNION ALL
    SELECT
      seg.workflow_session_id AS workflow_session_id,
      lower(seg.orchestrator_address) AS wallet
    FROM
    (
      SELECT *
      FROM livepeer_analytics.fact_workflow_session_segments FINAL
    ) seg
    INNER JOIN fs_latest fs
      ON fs.workflow_session_id = seg.workflow_session_id
    WHERE seg.segment_start_ts >= {from_ts:DateTime64(3)}
      AND seg.segment_start_ts < {to_ts:DateTime64(3)}
      AND seg.orchestrator_address != ''
  ) x
  INNER JOIN cap_wallets c
    ON c.wallet = x.wallet
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
  -- Scenario 1 remains confirmed-no-swap for fixture stability.
  -- Inferred orchestrator changes are still surfaced as diagnostics.
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

-- QUERY: scenario_6_same_hour_failed_no_output_retained
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    orchestrator_address,
    session_start_ts,
    session_end_ts,
    pipeline,
    model_id,
    gpu_id,
    last_error_occurred,
    loading_only_session,
    zero_output_fps_session
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
  'scenario_6_same_hour_failed_no_output_retained' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  s.window_start AS window_start,
  s.status_samples AS status_samples,
  s.fps_positive_samples AS fps_positive_samples,
  s.running_state_samples AS running_state_samples,
  toUInt8(g.window_start IS NOT NULL) AS present_in_gpu_view,
  toUInt8(sl.window_start IS NOT NULL) AS present_in_sla_view
FROM fs_latest fs
INNER JOIN status_1h s
  ON s.workflow_session_id = fs.workflow_session_id
LEFT JOIN livepeer_analytics.v_api_gpu_metrics g
  ON g.window_start = s.window_start
 AND g.orchestrator_address = s.orchestrator_address
 AND g.pipeline_id = s.pipeline
 AND ifNull(g.model_id, '') = s.model_id_key
 AND ifNull(g.gpu_id, '') = s.gpu_id_key
 AND ifNull(g.region, '') = s.region_key
LEFT JOIN livepeer_analytics.v_api_sla_compliance sl
  ON sl.window_start = s.window_start
 AND sl.orchestrator_address = s.orchestrator_address
 AND sl.pipeline_id = s.pipeline
 AND ifNull(sl.model_id, '') = s.model_id_key
 AND ifNull(sl.gpu_id, '') = s.gpu_id_key
 AND ifNull(sl.region, '') = s.region_key
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
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};

-- QUERY: scenario_7_rollover_terminal_tail_filtered_prev_hour_no_work
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    session_start_ts,
    session_end_ts
  FROM
  (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY workflow_session_id
        ORDER BY version DESC, session_start_ts DESC, session_end_ts DESC
      ) AS rn
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts < {to_ts:DateTime64(3)}
      AND session_end_ts >= {from_ts:DateTime64(3)}
  )
  WHERE rn = 1
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
)
SELECT
  'scenario_7_rollover_terminal_tail_filtered_prev_hour_no_work' AS scenario_name,
  s.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  s.window_start AS window_start,
  s.status_samples AS status_samples,
  p.status_samples AS prev_status_samples,
  toUInt8(
    (
      s.window_start,
      s.orchestrator_address,
      s.pipeline,
      s.model_id_key,
      s.gpu_id_key,
      s.region_key
    ) IN
    (
      SELECT
        window_start,
        orchestrator_address,
        pipeline_id,
        ifNull(model_id, ''),
        ifNull(gpu_id, ''),
        ifNull(region, '')
      FROM livepeer_analytics.v_api_gpu_metrics
    )
  ) AS present_in_gpu_view,
  toUInt8(
    (
      s.window_start,
      s.orchestrator_address,
      s.pipeline,
      s.model_id_key,
      s.gpu_id_key,
      s.region_key
    ) IN
    (
      SELECT
        window_start,
        orchestrator_address,
        pipeline_id,
        ifNull(model_id, ''),
        ifNull(gpu_id, ''),
        ifNull(region, '')
      FROM livepeer_analytics.v_api_sla_compliance
    )
  ) AS present_in_sla_view
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
WHERE fs.session_start_ts < s.window_start
  AND fs.session_end_ts >= s.window_start
  AND fs.session_end_ts < s.window_start + INTERVAL 1 HOUR
  AND s.fps_positive_samples = 0
  AND s.running_state_samples = 0
  AND s.status_samples < 3
  AND p.fps_positive_samples = 0
  AND p.running_state_samples = 0
  AND p.status_samples < 3
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
-- Uses canonical swap semantics:
-- any confirmed swap OR inferred orchestrator-change evidence qualifies as swap.
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    orchestrator_address,
    session_start_ts,
    session_end_ts,
    confirmed_swap_count,
    inferred_orchestrator_change_count,
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
cap_wallets AS
(
  -- Strict contract check for scenario extraction: typed/session identities
  -- should line up with canonical capability wallets. Avoid fallback joins
  -- here so normalization regressions surface in notebook output.
  SELECT DISTINCT lower(orchestrator_address) AS wallet
  FROM livepeer_analytics.raw_network_capabilities
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
    AND orchestrator_address != ''
),
wallet_overlap_sessions AS
(
  -- NOTE: scenario 3 intentionally does not require capability-wallet overlap.
  -- Swapped sessions can legitimately appear without in-window capabilities overlap
  -- in fixture windows; filtering on overlap would hide valid scenario rows.
  SELECT workflow_session_id
  FROM fs_latest
)
SELECT
  'scenario_3_success_with_swap' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  fs.confirmed_swap_count AS confirmed_swap_count,
  fs.inferred_orchestrator_change_count AS inferred_orchestrator_change_count,
  ifNull(seg.segment_orchestrators, 0) AS segment_orchestrators,
  toUInt8(fs.confirmed_swap_count > 0) AS has_explicit_swap_signal,
  toUInt8(fs.inferred_orchestrator_change_count > 0 OR ifNull(seg.segment_orchestrators, 0) > 1) AS has_derived_swap_signal,
  multiIf(
    fs.confirmed_swap_count > 0, 'explicit',
    fs.inferred_orchestrator_change_count > 0 OR ifNull(seg.segment_orchestrators, 0) > 1, 'derived',
    'none'
  ) AS swap_signal_source
FROM fs_latest fs
LEFT JOIN seg_stats seg
  ON seg.workflow_session_id = fs.workflow_session_id
WHERE fs.startup_success = 1
  AND (
    fs.confirmed_swap_count > 0
    OR fs.inferred_orchestrator_change_count > 0
    OR ifNull(seg.segment_orchestrators, 0) > 1
  )
  AND fs.workflow_session_id IN (SELECT workflow_session_id FROM wallet_overlap_sessions)
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

-- QUERY: scenario_5_out_of_category_baseline
-- Sample sessions that do not match scenarios 1-4. This aligns to notebook
-- fallout_df semantics (out-of-category population) while keeping fixture
-- volume bounded.
WITH fs_latest AS
(
  SELECT
    workflow_session_id,
    stream_id,
    request_id,
    orchestrator_address,
    session_start_ts,
    session_end_ts,
    known_stream,
    startup_success,
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
  SELECT
    workflow_session_id,
    avg(output_fps) AS avg_output_fps
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
  SELECT
    workflow_session_id,
    count() AS updates
  FROM livepeer_analytics.fact_workflow_param_updates
  WHERE update_ts >= {from_ts:DateTime64(3)}
    AND update_ts < {to_ts:DateTime64(3)}
  GROUP BY workflow_session_id
)
SELECT
  'scenario_5_out_of_category_baseline' AS scenario_name,
  fs.workflow_session_id AS workflow_session_id,
  fs.stream_id AS stream_id,
  fs.request_id AS request_id,
  fs.session_start_ts AS session_start_ts,
  fs.session_end_ts AS session_end_ts,
  ifNull(ss.avg_output_fps, 0.0) AS avg_output_fps,
  ifNull(tf.has_no_orch, 0) AS has_no_orch,
  ifNull(tf.has_close, 0) AS has_close,
  ifNull(seg.segment_orchestrators, 0) AS segment_orchestrators,
  ifNull(uc.updates, 0) AS updates
FROM fs_latest fs
LEFT JOIN status_stats ss
  ON ss.workflow_session_id = fs.workflow_session_id
LEFT JOIN trace_flags tf
  ON tf.workflow_session_id = fs.workflow_session_id
LEFT JOIN seg_stats seg
  ON seg.workflow_session_id = fs.workflow_session_id
LEFT JOIN update_counts uc
  ON uc.workflow_session_id = fs.workflow_session_id
WHERE
  -- Not scenario 1
  NOT (
    fs.known_stream = 1
    AND fs.startup_success = 1
    AND fs.startup_unexcused = 0
    AND fs.swap_count = 0
    AND ifNull(ss.avg_output_fps, 0.0) > 12
  )
  -- Not scenario 2
  AND NOT (
    fs.startup_success = 0
    AND ifNull(tf.has_no_orch, 0) = 1
    AND ifNull(tf.has_close, 0) = 1
  )
  -- Not scenario 3
  AND NOT (
    fs.startup_success = 1
    AND (
      fs.confirmed_swap_count > 0
      OR fs.inferred_orchestrator_change_count > 0
      OR ifNull(seg.segment_orchestrators, 0) > 1
    )
  )
  -- Not scenario 4
  AND NOT (
    fs.startup_success = 1
    AND ifNull(uc.updates, 0) > 0
  )
ORDER BY fs.session_start_ts DESC
LIMIT {limit_per_scenario:UInt32};
