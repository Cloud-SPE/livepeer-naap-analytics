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
    OR pipeline = ''
    OR ifNull(gpu_id, '') = ''
    OR window_start IS NULL
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: gpu_metrics_pipeline_id_coverage
-- Informational telemetry-coverage metric: some producers currently emit empty pipeline_id.
SELECT
  toUInt64(0) AS failed_rows,
  count() AS rows_checked,
  countIf(pipeline_id = '') AS empty_pipeline_id_rows
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

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

-- TEST: network_demand_by_gpu_hourly_grain
SELECT
  toUInt64(countIf(toMinute(window_start) != 0) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand_by_gpu
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

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
    OR total_inference_minutes < 0
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

-- TEST: sla_compliance_rollup_safe
SELECT
  toUInt64(countIf(
    abs(success_ratio - ifNull(1 - (unexcused_sessions / nullIf(known_sessions, 0)), 0)) > 0.000001
    OR abs(no_swap_ratio - ifNull(1 - (swapped_sessions / nullIf(known_sessions, 0)), 0)) > 0.000001
  ) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_low_sample_windows
-- Informational telemetry sparsity metric; not a contract failure.
SELECT
  toUInt64(0) AS failed_rows,
  count() AS rows_checked,
  countIf(total_inference_minutes < (1.0 / 60.0)) AS low_sample_windows
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
    argMax(pipeline_id, version) AS pipeline_id,
    argMax(model_id, version) AS model_id,
    argMax(gpu_id, version) AS gpu_id,
    argMax(region, version) AS region,
    argMax(known_stream, version) AS known_stream,
    argMax(startup_unexcused, version) AS startup_unexcused,
    argMax(swap_count, version) AS swap_count
  FROM livepeer_analytics.fact_workflow_sessions
  GROUP BY workflow_session_id
),
raw_rollup AS
(
  SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
    ifNull(model_id, '') AS model_id,
    ifNull(gpu_id, '') AS gpu_id,
    ifNull(region, '') AS region,
    sum(toUInt64(known_stream)) AS known_sessions,
    sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
    sum(toUInt64(swap_count > 0)) AS swapped_sessions
  FROM latest_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
  GROUP BY window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region
),
api_rollup AS
(
  SELECT
    window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
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
  toUInt64(
    sum(abs(r.known_sessions - a.known_sessions))
    + sum(abs(r.unexcused_sessions - a.unexcused_sessions))
    + sum(abs(r.swapped_sessions - a.swapped_sessions)) > 0
  ) AS failed_rows,
  count() AS joined_rows
FROM raw_rollup r
INNER JOIN api_rollup a
  USING (window_start, orchestrator_address, pipeline, pipeline_id, model_id, gpu_id, region);
