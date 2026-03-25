-- Migration 015: ClickHouse views for SLA Compliance, Network Demand, and GPU Metrics APIs.
-- These views exist for documentation and ad-hoc querying.
-- The Go API layer executes equivalent inline CTE queries directly against the underlying tables.
--
-- Field approximations (see handlers_sla_gpu_demand.go for rationale):
--   region                    → always NULL (not captured in naap.events)
--   runner_version/cuda_version → always NULL (not tracked)
--   startup_excused_sessions  → 0
--   confirmed_swapped_sessions → 0
--   inferred_swap_sessions    → 0 (restart_count not linked per-session here)
--   avg_prompt_to_first_frame_ms, p95 variant → NULL
--   fps_jitter_coefficient    → NULL
--   ticket_face_value_eth     → proportional share of pipeline payments (approximation)

-- ---------------------------------------------------------------------------
-- v_api_sla_compliance
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_sla_compliance AS
WITH session_stats AS (
    SELECT
        org,
        orch_address,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS had_error_state,
        count()                                                                          AS total_samples,
        max(is_attributed)                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, orch_address, pipeline, stream_id
)
SELECT
    s.window_start,
    CAST(NULL AS Nullable(String))                                                         AS org,
    s.orch_address                                                                         AS orchestrator_address,
    s.pipeline                                                                             AS pipeline_id,
    g.model_id,
    g.gpu_id,
    CAST(NULL AS Nullable(String))                                                         AS region,
    toUInt64(count())                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                            AS startup_success_sessions,
    toUInt64(0)                                                                            AS startup_excused_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                       AS sessions_ending_in_error,
    toUInt64(sum(s.error_sample_count))                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                  AS health_signal_coverage_ratio,
    if(count() >= 5, toFloat64(sum(s.was_online)) / toFloat64(count()), CAST(NULL AS Nullable(Float64)))         AS startup_success_rate,
    if(count() >= 5, toFloat64(sum(s.was_online)) / toFloat64(count()), CAST(NULL AS Nullable(Float64)))         AS effective_success_rate,
    if(count() >= 5, 1.0 - toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count()), CAST(NULL AS Nullable(Float64))) AS no_swap_rate,
    if(count() >= 5,
        0.6 * toFloat64(sum(s.was_online)) / toFloat64(count()) +
        0.4 * (1.0 - toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count())),
        CAST(NULL AS Nullable(Float64))
    )                                                                                      AS sla_score
FROM session_stats s
LEFT JOIN (
    SELECT orch_address, pipeline, any(model_id) AS model_id, any(gpu_id) AS gpu_id
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY orch_address, pipeline
) g ON g.orch_address = s.orch_address AND g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.orch_address, s.pipeline, g.model_id, g.gpu_id;

-- ---------------------------------------------------------------------------
-- v_api_sla_compliance_by_org  (includes real org in output)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_sla_compliance_by_org AS
WITH session_stats AS (
    SELECT
        org,
        orch_address,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS had_error_state,
        max(is_attributed)                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, orch_address, pipeline, stream_id
)
SELECT
    s.window_start,
    s.org,
    s.orch_address                                                                         AS orchestrator_address,
    s.pipeline                                                                             AS pipeline_id,
    g.model_id,
    g.gpu_id,
    CAST(NULL AS Nullable(String))                                                         AS region,
    toUInt64(count())                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                            AS startup_success_sessions,
    toUInt64(0)                                                                            AS startup_excused_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                       AS sessions_ending_in_error,
    toUInt64(sum(s.error_sample_count))                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                  AS health_signal_coverage_ratio,
    if(count() >= 5, toFloat64(sum(s.was_online)) / toFloat64(count()), CAST(NULL AS Nullable(Float64)))         AS startup_success_rate,
    if(count() >= 5, toFloat64(sum(s.was_online)) / toFloat64(count()), CAST(NULL AS Nullable(Float64)))         AS effective_success_rate,
    if(count() >= 5, 1.0 - toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count()), CAST(NULL AS Nullable(Float64))) AS no_swap_rate,
    if(count() >= 5,
        0.6 * toFloat64(sum(s.was_online)) / toFloat64(count()) +
        0.4 * (1.0 - toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count())),
        CAST(NULL AS Nullable(Float64))
    )                                                                                      AS sla_score
FROM session_stats s
LEFT JOIN (
    SELECT orch_address, pipeline, any(model_id) AS model_id, any(gpu_id) AS gpu_id
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY orch_address, pipeline
) g ON g.orch_address = s.orch_address AND g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.orch_address, s.pipeline, g.model_id, g.gpu_id;

-- ---------------------------------------------------------------------------
-- v_api_network_demand
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_network_demand AS
WITH session_stats AS (
    SELECT
        org,
        gateway,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS had_error_state,
        avg(output_fps)                                                                  AS avg_session_fps,
        toFloat64(dateDiff('millisecond', min(sample_ts), max(sample_ts))) / 60000.0     AS session_minutes,
        count()                                                                          AS total_samples,
        max(is_attributed)                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, gateway, pipeline, stream_id
)
SELECT
    s.window_start,
    CAST(NULL AS Nullable(String))                                                         AS org,
    s.gateway,
    CAST(NULL AS Nullable(String))                                                         AS region,
    s.pipeline                                                                             AS pipeline_id,
    g.model_id,
    toUInt64(count())                                                                      AS sessions_count,
    avg(s.avg_session_fps)                                                                 AS avg_output_fps,
    sum(s.session_minutes)                                                                 AS total_minutes,
    toUInt64(count())                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                            AS served_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS unserved_sessions,
    toUInt64(count())                                                                      AS total_demand_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                       AS sessions_ending_in_error,
    toUInt64(sum(s.error_sample_count))                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                  AS health_signal_coverage_ratio,
    if(count() > 0, toFloat64(sum(s.was_online)) / toFloat64(count()), 0.0)               AS startup_success_rate,
    if(count() > 0, toFloat64(sum(s.was_online)) / toFloat64(count()), 0.0)               AS effective_success_rate,
    0.0                                                                                    AS ticket_face_value_eth
FROM session_stats s
LEFT JOIN (
    SELECT pipeline, any(model_id) AS model_id
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY pipeline
) g ON g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.gateway, s.pipeline, g.model_id;

-- ---------------------------------------------------------------------------
-- v_api_network_demand_by_org
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_network_demand_by_org AS
WITH session_stats AS (
    SELECT
        org,
        gateway,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))             AS had_error_state,
        avg(output_fps)                                                                  AS avg_session_fps,
        toFloat64(dateDiff('millisecond', min(sample_ts), max(sample_ts))) / 60000.0     AS session_minutes,
        count()                                                                          AS total_samples,
        max(is_attributed)                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, gateway, pipeline, stream_id
)
SELECT
    s.window_start,
    s.org,
    s.gateway,
    CAST(NULL AS Nullable(String))                                                         AS region,
    s.pipeline                                                                             AS pipeline_id,
    g.model_id,
    toUInt64(count())                                                                      AS sessions_count,
    avg(s.avg_session_fps)                                                                 AS avg_output_fps,
    sum(s.session_minutes)                                                                 AS total_minutes,
    toUInt64(count())                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                            AS served_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS unserved_sessions,
    toUInt64(count())                                                                      AS total_demand_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                       AS sessions_ending_in_error,
    toUInt64(sum(s.error_sample_count))                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                  AS health_signal_coverage_ratio,
    if(count() > 0, toFloat64(sum(s.was_online)) / toFloat64(count()), 0.0)               AS startup_success_rate,
    if(count() > 0, toFloat64(sum(s.was_online)) / toFloat64(count()), 0.0)               AS effective_success_rate,
    0.0                                                                                    AS ticket_face_value_eth
FROM session_stats s
LEFT JOIN (
    SELECT pipeline, any(model_id) AS model_id
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY pipeline
) g ON g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.gateway, s.pipeline, g.model_id;

-- ---------------------------------------------------------------------------
-- v_api_gpu_metrics
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_gpu_metrics AS
WITH session_stats AS (
    SELECT
        org,
        orch_address,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))                             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))                             AS had_error_state,
        count()                                                                                          AS total_samples,
        avgIf(output_fps, output_fps > 0)                                                               AS avg_session_fps,
        quantileIf(0.95)(output_fps, output_fps > 0)                                                    AS p95_session_fps,
        avgIf(e2e_latency_ms, e2e_latency_ms > 0)                                                       AS avg_session_e2e,
        quantileIf(0.95)(e2e_latency_ms, e2e_latency_ms > 0)                                            AS p95_session_e2e,
        toUInt8(avgIf(e2e_latency_ms, e2e_latency_ms > 0) > 0)                                          AS has_e2e,
        if(maxIf(1, state = 'ONLINE') = 1,
            toFloat64(dateDiff('millisecond', min(sample_ts), minIf(sample_ts, state = 'ONLINE'))),
            CAST(NULL AS Nullable(Float64))
        )                                                                                                AS startup_latency_ms,
        max(is_attributed)                                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, orch_address, pipeline, stream_id
)
SELECT
    s.window_start,
    CAST(NULL AS Nullable(String))                                                                         AS org,
    s.orch_address                                                                                         AS orchestrator_address,
    s.pipeline                                                                                             AS pipeline_id,
    g.model_id,
    g.gpu_id,
    CAST(NULL AS Nullable(String))                                                                         AS region,
    avg(s.avg_session_fps)                                                                                 AS avg_output_fps,
    toFloat32(quantile(0.95)(s.p95_session_fps))                                                           AS p95_output_fps,
    CAST(NULL AS Nullable(Float64))                                                                        AS fps_jitter_coefficient,
    toUInt64(sum(s.total_samples))                                                                         AS status_samples,
    toUInt64(sum(s.error_sample_count))                                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                                  AS health_signal_coverage_ratio,
    g.gpu_model,
    g.memory_bytes,
    CAST(NULL AS Nullable(String))                                                                         AS runner_version,
    CAST(NULL AS Nullable(String))                                                                         AS cuda_version,
    CAST(NULL AS Nullable(Float64))                                                                        AS avg_prompt_to_first_frame_ms,
    if(count() >= 5, avgIf(s.startup_latency_ms, s.startup_latency_ms IS NOT NULL AND s.startup_latency_ms >= 0), CAST(NULL AS Nullable(Float64))) AS avg_startup_latency_ms,
    if(count() >= 5, avg(s.avg_session_e2e), CAST(NULL AS Nullable(Float64)))                             AS avg_e2e_latency_ms,
    CAST(NULL AS Nullable(Float32))                                                                        AS p95_prompt_to_first_frame_latency_ms,
    if(count() >= 5, toFloat32(quantile(0.95)(s.startup_latency_ms)), CAST(NULL AS Nullable(Float32)))    AS p95_startup_latency_ms,
    if(count() >= 5, toFloat32(quantile(0.95)(s.p95_session_e2e)), CAST(NULL AS Nullable(Float32)))       AS p95_e2e_latency_ms,
    toUInt64(0)                                                                                            AS prompt_to_first_frame_sample_count,
    toUInt64(sum(s.was_online))                                                                            AS startup_latency_sample_count,
    toUInt64(countIf(s.has_e2e = 1))                                                                      AS e2e_latency_sample_count,
    toUInt64(count())                                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                                            AS startup_success_sessions,
    toUInt64(0)                                                                                            AS startup_excused_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                                       AS sessions_ending_in_error,
    if(count() > 0, toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count()), 0.0)                  AS startup_unexcused_rate,
    0.0                                                                                                    AS swap_rate
FROM session_stats s
LEFT JOIN (
    SELECT orch_address, pipeline, any(model_id) AS model_id, any(gpu_id) AS gpu_id,
           any(gpu_model) AS gpu_model, any(memory_bytes) AS memory_bytes
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY orch_address, pipeline
) g ON g.orch_address = s.orch_address AND g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.orch_address, s.pipeline, g.model_id, g.gpu_id, g.gpu_model, g.memory_bytes;

-- ---------------------------------------------------------------------------
-- v_api_gpu_metrics_by_org  (includes real org in output)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW naap.v_api_gpu_metrics_by_org AS
WITH session_stats AS (
    SELECT
        org,
        orch_address,
        pipeline,
        stream_id,
        toStartOfHour(min(sample_ts))                                                                    AS window_start,
        toUInt8(maxIf(1, state = 'ONLINE'))                                                              AS was_online,
        toUInt64(countIf(state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))                             AS error_sample_count,
        toUInt8(maxIf(1, state IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')))                             AS had_error_state,
        count()                                                                                          AS total_samples,
        avgIf(output_fps, output_fps > 0)                                                               AS avg_session_fps,
        quantileIf(0.95)(output_fps, output_fps > 0)                                                    AS p95_session_fps,
        avgIf(e2e_latency_ms, e2e_latency_ms > 0)                                                       AS avg_session_e2e,
        quantileIf(0.95)(e2e_latency_ms, e2e_latency_ms > 0)                                            AS p95_session_e2e,
        toUInt8(avgIf(e2e_latency_ms, e2e_latency_ms > 0) > 0)                                          AS has_e2e,
        if(maxIf(1, state = 'ONLINE') = 1,
            toFloat64(dateDiff('millisecond', min(sample_ts), minIf(sample_ts, state = 'ONLINE'))),
            CAST(NULL AS Nullable(Float64))
        )                                                                                                AS startup_latency_ms,
        max(is_attributed)                                                                               AS is_attr
    FROM naap.agg_stream_status_samples
    GROUP BY org, orch_address, pipeline, stream_id
)
SELECT
    s.window_start,
    s.org,
    s.orch_address                                                                                         AS orchestrator_address,
    s.pipeline                                                                                             AS pipeline_id,
    g.model_id,
    g.gpu_id,
    CAST(NULL AS Nullable(String))                                                                         AS region,
    avg(s.avg_session_fps)                                                                                 AS avg_output_fps,
    toFloat32(quantile(0.95)(s.p95_session_fps))                                                           AS p95_output_fps,
    CAST(NULL AS Nullable(Float64))                                                                        AS fps_jitter_coefficient,
    toUInt64(sum(s.total_samples))                                                                         AS status_samples,
    toUInt64(sum(s.error_sample_count))                                                                    AS error_status_samples,
    if(count() > 0, toFloat64(sum(s.is_attr)) / toFloat64(count()), 0.0)                                  AS health_signal_coverage_ratio,
    g.gpu_model,
    g.memory_bytes,
    CAST(NULL AS Nullable(String))                                                                         AS runner_version,
    CAST(NULL AS Nullable(String))                                                                         AS cuda_version,
    CAST(NULL AS Nullable(Float64))                                                                        AS avg_prompt_to_first_frame_ms,
    if(count() >= 5, avgIf(s.startup_latency_ms, s.startup_latency_ms IS NOT NULL AND s.startup_latency_ms >= 0), CAST(NULL AS Nullable(Float64))) AS avg_startup_latency_ms,
    if(count() >= 5, avg(s.avg_session_e2e), CAST(NULL AS Nullable(Float64)))                             AS avg_e2e_latency_ms,
    CAST(NULL AS Nullable(Float32))                                                                        AS p95_prompt_to_first_frame_latency_ms,
    if(count() >= 5, toFloat32(quantile(0.95)(s.startup_latency_ms)), CAST(NULL AS Nullable(Float32)))    AS p95_startup_latency_ms,
    if(count() >= 5, toFloat32(quantile(0.95)(s.p95_session_e2e)), CAST(NULL AS Nullable(Float32)))       AS p95_e2e_latency_ms,
    toUInt64(0)                                                                                            AS prompt_to_first_frame_sample_count,
    toUInt64(sum(s.was_online))                                                                            AS startup_latency_sample_count,
    toUInt64(countIf(s.has_e2e = 1))                                                                      AS e2e_latency_sample_count,
    toUInt64(count())                                                                                      AS known_sessions_count,
    toUInt64(sum(s.was_online))                                                                            AS startup_success_sessions,
    toUInt64(0)                                                                                            AS startup_excused_sessions,
    toUInt64(sum(1 - toUInt8(s.was_online)))                                                               AS startup_unexcused_sessions,
    toUInt64(0)                                                                                            AS confirmed_swapped_sessions,
    toUInt64(0)                                                                                            AS inferred_swap_sessions,
    toUInt64(0)                                                                                            AS total_swapped_sessions,
    toUInt64(sum(s.had_error_state))                                                                       AS sessions_ending_in_error,
    if(count() > 0, toFloat64(sum(1 - toUInt8(s.was_online))) / toFloat64(count()), 0.0)                  AS startup_unexcused_rate,
    0.0                                                                                                    AS swap_rate
FROM session_stats s
LEFT JOIN (
    SELECT orch_address, pipeline, any(model_id) AS model_id, any(gpu_id) AS gpu_id,
           any(gpu_model) AS gpu_model, any(memory_bytes) AS memory_bytes
    FROM naap.agg_gpu_inventory FINAL
    GROUP BY orch_address, pipeline
) g ON g.orch_address = s.orch_address AND g.pipeline = s.pipeline
GROUP BY s.window_start, s.org, s.orch_address, s.pipeline, g.model_id, g.gpu_id, g.gpu_model, g.memory_bytes;
