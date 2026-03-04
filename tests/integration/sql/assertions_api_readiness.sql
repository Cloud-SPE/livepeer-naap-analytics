-- API readiness assertions for canonical serving views:
--   - v_api_gpu_metrics
--   - v_api_network_demand
--   - v_api_sla_compliance
-- Each test returns exactly one row with `failed_rows` + diagnostics.

-- TEST: api_views_present
SELECT
  countIf(view_exists = 0) AS failed_rows,
  groupArrayIf(view_name, view_exists = 0) AS missing_views
FROM
(
  SELECT
    view_name,
    toUInt8(count() > 0) AS view_exists
  FROM
  (
    SELECT 'v_api_gpu_metrics' AS view_name
    UNION ALL SELECT 'v_api_network_demand'
    UNION ALL SELECT 'v_api_network_demand_by_gpu'
    UNION ALL SELECT 'v_api_sla_compliance'
  ) required
  LEFT JOIN
  (
    SELECT name
    FROM system.tables
    WHERE database = 'livepeer_analytics'
      AND engine = 'View'
  ) present
    ON required.view_name = present.name
  GROUP BY view_name
);

-- TEST: gpu_metrics_keys_not_null
SELECT
  toUInt64(countIf(
    orchestrator_address = ''
    OR (pipeline = '' AND ifNull(model_id, '') = '')
    OR ifNull(gpu_id, '') = ''
    OR window_start IS NULL
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: gold_pipeline_coverage_attributable_min_threshold
-- Enforces near-complete canonical pipeline attribution on attributable rows.
SELECT
  toUInt64(
    total_attributable_model_rows > 0
    AND (attributed_with_pipeline / total_attributable_model_rows) < 0.99
  ) AS failed_rows,
  round(if(total_attributable_model_rows = 0, 1.0, attributed_with_pipeline / total_attributable_model_rows), 6) AS pipeline_coverage_ratio,
  total_attributable_model_rows,
  attributed_with_pipeline
FROM
(
  SELECT
    toFloat64(sum(rows_with_model)) AS total_attributable_model_rows,
    toFloat64(sum(rows_with_pipeline)) AS attributed_with_pipeline
  FROM
  (
    SELECT
      countIf(ifNull(model_id, '') != '') AS rows_with_model,
      countIf(ifNull(model_id, '') != '' AND pipeline != '') AS rows_with_pipeline
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}

    UNION ALL

    SELECT
      countIf(ifNull(model_id, '') != '') AS rows_with_model,
      countIf(ifNull(model_id, '') != '' AND pipeline != '') AS rows_with_pipeline
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
);

-- TEST: gpu_metrics_rollup_fields_consistent
SELECT
  toUInt64(countIf(
    abs(failure_rate - ifNull(unexcused_sessions / nullIf(known_sessions, 0), 0)) > 0.000001
    OR abs(swap_rate - ifNull(swapped_sessions / nullIf(known_sessions, 0), 0)) > 0.000001
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: gpu_metrics_latency_fields_nonnegative
SELECT
  toUInt64(countIf(
    (prompt_to_first_frame_ms IS NOT NULL AND prompt_to_first_frame_ms < 0)
    OR (startup_time_ms IS NOT NULL AND startup_time_ms < 0)
    OR (startup_time_s IS NOT NULL AND startup_time_s < 0)
    OR (e2e_latency_ms IS NOT NULL AND e2e_latency_ms < 0)
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: gpu_metrics_startup_seconds_matches_ms
SELECT
  toUInt64(countIf(
    startup_time_ms IS NOT NULL
    AND startup_time_s IS NOT NULL
    AND abs(startup_time_s - (startup_time_ms / 1000.0)) > 0.000001
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_hourly_grain
SELECT
  toUInt64(countIf(toMinute(window_start) != 0) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_required_columns_present
SELECT
  toUInt64(countIf(is_present = 0) > 0) AS failed_rows,
  groupArrayIf(column_name, is_present = 0) AS missing_columns
FROM
(
  SELECT
    required.column_name,
    toUInt8(count(c.name) > 0) AS is_present
  FROM
  (
    SELECT 'model_id' AS column_name
    UNION ALL SELECT 'pipeline'
    UNION ALL SELECT 'total_minutes'
    UNION ALL SELECT 'startup_success_ratio'
    UNION ALL SELECT 'success_ratio'
  ) required
  LEFT JOIN system.columns c
    ON c.database = 'livepeer_analytics'
   AND c.table = 'v_api_network_demand'
   AND c.name = required.column_name
  GROUP BY required.column_name
);

-- TEST: network_demand_by_gpu_hourly_grain
SELECT
  toUInt64(countIf(toMinute(window_start) != 0) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand_by_gpu
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_by_gpu_required_columns_present
SELECT
  toUInt64(countIf(is_present = 0) > 0) AS failed_rows,
  groupArrayIf(column_name, is_present = 0) AS missing_columns
FROM
(
  SELECT
    required.column_name,
    toUInt8(count(c.name) > 0) AS is_present
  FROM
  (
    SELECT 'gpu_id' AS column_name
    UNION ALL SELECT 'gpu_type'
  ) required
  LEFT JOIN system.columns c
    ON c.database = 'livepeer_analytics'
   AND c.table = 'v_api_network_demand_by_gpu'
   AND c.name = required.column_name
  GROUP BY required.column_name
);

-- TEST: network_demand_by_gpu_capacity_fields_nonnegative
SELECT
  toUInt64(countIf(
    inference_minutes_by_gpu_type < 0
    OR used_inference_minutes < 0
    OR available_capacity_minutes < 0
    OR capacity_rate < 0
    OR capacity_rate > 1.5
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand_by_gpu
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_additive_fields_nonnegative
SELECT
  toUInt64(countIf(
    total_streams < 0
    OR total_sessions < 0
    OR total_minutes < 0
    OR known_sessions < 0
    OR served_sessions < 0
    OR unserved_sessions < 0
    OR total_demand_sessions < 0
    OR unexcused_sessions < 0
    OR swapped_sessions < 0
    OR missing_capacity_count < 0
    OR fee_payment_eth < 0
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_served_unserved_consistency
SELECT
  toUInt64(countIf(
    total_demand_sessions != served_sessions + unserved_sessions
    OR missing_capacity_count != unserved_sessions
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_pipeline_not_model_id
SELECT
  toUInt64(countIf(
    pipeline != ''
    AND ifNull(model_id, '') != ''
    AND lowerUTF8(pipeline) = lowerUTF8(ifNull(model_id, ''))
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_pipeline_fallback_applied
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      argMax(region, version) AS region,
      argMax(pipeline, version) AS pipeline,
      argMax(model_id, version) AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  fallback AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      ifNull(region, '') AS region,
      ifNull(model_id, '') AS model_id,
      if(
        countDistinctIf(pipeline, pipeline != '') = 1,
        anyIf(pipeline, pipeline != ''),
        ''
      ) AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, model_id
  )
SELECT
  toUInt64(count() > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand d
INNER JOIN fallback f
  ON f.window_start = d.window_start
 AND f.gateway = d.gateway
 AND f.region = ifNull(d.region, '')
 AND f.model_id = ifNull(d.model_id, '')
WHERE d.window_start >= {from_ts:DateTime64(3)}
  AND d.window_start < {to_ts:DateTime64(3)}
  AND ifNull(d.model_id, '') != ''
  AND ifNull(d.pipeline, '') = ''
  AND ifNull(f.pipeline_fallback, '') != '';

-- TEST: sla_compliance_rollup_safe
SELECT
  toUInt64(countIf(
    abs(startup_success_ratio - ifNull(1 - (unexcused_sessions / nullIf(known_sessions, 0)), 0)) > 0.000001
    OR success_ratio < 0
    OR success_ratio > 1
    OR startup_success_ratio + 0.000001 < success_ratio
    OR abs(no_swap_ratio - ifNull(1 - (swapped_sessions / nullIf(known_sessions, 0)), 0)) > 0.000001
    OR abs(health_completeness_ratio - ifNull(health_signal_count / nullIf(health_expected_signal_count, 0), 1.0)) > 0.000001
    OR sla_score < 0
    OR sla_score > 100
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: sla_compliance_required_columns_present
SELECT
  toUInt64(countIf(is_present = 0) > 0) AS failed_rows,
  groupArrayIf(column_name, is_present = 0) AS missing_columns
FROM
(
  SELECT
    required.column_name,
    toUInt8(count(c.name) > 0) AS is_present
  FROM
  (
    SELECT 'startup_success_ratio' AS column_name
    UNION ALL SELECT 'success_ratio'
  ) required
  LEFT JOIN system.columns c
    ON c.database = 'livepeer_analytics'
   AND c.table = 'v_api_sla_compliance'
   AND c.name = required.column_name
  GROUP BY required.column_name
);

-- TEST: network_demand_success_ratios_valid
SELECT
  toUInt64(countIf(
    startup_success_ratio < 0 OR startup_success_ratio > 1
    OR success_ratio < 0 OR success_ratio > 1
    OR startup_success_ratio + 0.000001 < success_ratio
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_success_ratio_penalizes_failure_indicators
SELECT
  toUInt64(countIf(
    known_sessions > 0
    AND (
      unexcused_sessions > 0
      OR zero_output_fps_sessions > 0
      OR loading_only_sessions > 0
    )
    AND success_ratio >= 0.999999
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_low_sample_windows
-- Informational telemetry sparsity metric; not a contract failure.
SELECT
  toUInt64(0) AS failed_rows,
  count() AS rows_checked,
  countIf(total_minutes < (1.0 / 60.0)) AS low_sample_windows
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: sla_recompute_parity_vs_latest_sessions
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
    argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
    argMax(last_error_occurred, version) AS last_error_occurred,
    argMax(loading_only_session, version) AS loading_only_session,
    argMax(zero_output_fps_session, version) AS zero_output_fps_session,
    argMax(status_error_sample_count, version) AS status_error_sample_count,
    argMax(health_signal_count, version) AS health_signal_count,
    argMax(health_expected_signal_count, version) AS health_expected_signal_count
  FROM livepeer_analytics.fact_workflow_sessions
  GROUP BY workflow_session_id
),
raw_rollup AS
(
  SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address,
    pipeline,
    ifNull(model_id, '') AS model_id_key,
    ifNull(gpu_id, '') AS gpu_id_key,
    ifNull(region, '') AS region_key,
    sum(toUInt64(known_stream)) AS known_sessions,
    sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
    sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions,
    sum(toUInt64(last_error_occurred > 0)) AS sessions_with_last_error,
    sum(toUInt64(loading_only_session > 0)) AS loading_only_sessions,
    sum(toUInt64(zero_output_fps_session > 0)) AS zero_output_fps_sessions,
    sum(toUInt64(status_error_sample_count)) AS status_error_samples,
    sum(toUInt64(health_signal_count)) AS health_signal_count,
    sum(toUInt64(health_expected_signal_count)) AS health_expected_signal_count
  FROM latest_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
    AND orchestrator_address != ''
  GROUP BY window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
),
api_rollup AS
(
  SELECT
    window_start,
    orchestrator_address,
    pipeline,
    ifNull(model_id, '') AS model_id_key,
    ifNull(gpu_id, '') AS gpu_id_key,
    ifNull(region, '') AS region_key,
    known_sessions,
    unexcused_sessions,
    swapped_sessions,
    sessions_with_last_error,
    loading_only_sessions,
    zero_output_fps_sessions,
    status_error_samples,
    health_signal_count,
    health_expected_signal_count
  FROM livepeer_analytics.v_api_sla_compliance
  WHERE window_start >= {from_ts:DateTime64(3)}
    AND window_start < {to_ts:DateTime64(3)}
)
SELECT
  toUInt64(
    (raw_rows > 0 AND view_rows > 0 AND joined_rows = 0)
    OR raw_only_keys > 0
    OR view_only_keys > 0
    OR (
      joined_rows > 0
      AND (
        total_known_diff > 0
        OR total_unexcused_diff > 0
        OR total_swapped_diff > 0
        OR total_last_error_diff > 0
        OR total_loading_only_diff > 0
        OR total_zero_output_diff > 0
        OR total_status_error_samples_diff > 0
        OR total_health_signal_count_diff > 0
        OR total_health_expected_signal_count_diff > 0
      )
    )
  ) AS failed_rows,
  raw_rows,
  view_rows,
  joined_rows,
  raw_only_keys,
  view_only_keys,
  total_known_diff,
  total_unexcused_diff,
  total_swapped_diff,
  total_last_error_diff,
  total_loading_only_diff,
  total_zero_output_diff,
  total_status_error_samples_diff,
  total_health_signal_count_diff,
  total_health_expected_signal_count_diff
FROM
(
  SELECT
    (SELECT count() FROM raw_rollup) AS raw_rows,
    (SELECT count() FROM api_rollup) AS view_rows,
    (
      SELECT count()
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS joined_rows,
    (
      SELECT count()
      FROM raw_rollup r
      LEFT JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE a.window_start IS NULL
    ) AS raw_only_keys,
    (
      SELECT count()
      FROM api_rollup a
      LEFT JOIN raw_rollup r
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE r.window_start IS NULL
    ) AS view_only_keys,
    (
      SELECT ifNull(sum(abs(r.known_sessions - a.known_sessions)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_known_diff,
    (
      SELECT ifNull(sum(abs(r.unexcused_sessions - a.unexcused_sessions)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_unexcused_diff,
    (
      SELECT ifNull(sum(abs(r.swapped_sessions - a.swapped_sessions)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_swapped_diff,
    (
      SELECT ifNull(sum(abs(r.sessions_with_last_error - a.sessions_with_last_error)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_last_error_diff,
    (
      SELECT ifNull(sum(abs(r.loading_only_sessions - a.loading_only_sessions)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_loading_only_diff,
    (
      SELECT ifNull(sum(abs(r.zero_output_fps_sessions - a.zero_output_fps_sessions)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_zero_output_diff,
    (
      SELECT ifNull(sum(abs(r.status_error_samples - a.status_error_samples)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_status_error_samples_diff,
    (
      SELECT ifNull(sum(abs(r.health_signal_count - a.health_signal_count)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_health_signal_count_diff,
    (
      SELECT ifNull(sum(abs(r.health_expected_signal_count - a.health_expected_signal_count)), 0)
      FROM raw_rollup r
      INNER JOIN api_rollup a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_health_expected_signal_count_diff
);
