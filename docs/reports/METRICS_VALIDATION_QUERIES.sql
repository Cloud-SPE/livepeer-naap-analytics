-- Metrics Validation Query Pack
-- Replace window bounds before running

-- 0) Stream -> Model -> GPU attribution coverage (first gating validation)
-- Goal: verify we can attribute stream sessions to orchestrator capability metadata.
-- Outcome: the above query completed this report with ~ 61% coverage on model_id and gpu_id, which was sufficient confidence to proceed with the rest of the analysis.
-- Note: we had to join capabilities on ai_stream_status.orchestrator_address (hot wallet address) to network_capabilities.local_address (also hot wallet address) because the orchestrator_address field in ai_stream_status can be a dummy wallet address in some cases
-- Need to validate this join strategy works consistently 
WITH
  toDateTime64('2026-01-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  session_base AS (
    SELECT
      stream_id,
      request_id,
      orchestrator_address,
      min(event_timestamp) AS session_start_ts
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
      AND orchestrator_address != ''
    GROUP BY stream_id, request_id, orchestrator_address
  ),
  cap_latest AS (
    SELECT
      lower(local_address) AS orchestrator_proxy_address,
      lower(orchestrator_address) AS orchestrator_address,
      argMax(model_id, event_timestamp) AS model_id_latest,
      argMax(gpu_id, event_timestamp) AS gpu_id_latest
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp < to_ts
      AND orchestrator_address != '' AND local_address != ''
    GROUP BY orchestrator_address, local_address
  )
SELECT
  count() AS sessions_total,
  countIf(c.model_id_latest != '' AND c.model_id_latest IS NOT NULL) AS sessions_with_model,
  countIf(c.gpu_id_latest != '' AND c.gpu_id_latest IS NOT NULL) AS sessions_with_gpu,
  countIf(c.model_id_latest != '' AND c.model_id_latest IS NOT NULL) / nullIf(count(), 0) AS model_coverage_ratio,
  countIf(c.gpu_id_latest != '' AND c.gpu_id_latest IS NOT NULL) / nullIf(count(), 0) AS gpu_coverage_ratio
FROM session_base s
LEFT JOIN cap_latest c
  ON s.orchestrator_address = c.orchestrator_proxy_address;

-- 0b) Optional detail view for attribution gaps
WITH
  toDateTime64('2026-01-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  session_base AS (
    SELECT
      stream_id,
      request_id,
      orchestrator_address,
      min(event_timestamp) AS session_start_ts
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
      AND orchestrator_address != ''
    GROUP BY stream_id, request_id, orchestrator_address
  ),
  cap_latest AS (
    SELECT
      lower(local_address) AS orchestrator_proxy_address,
      lower(orchestrator_address) AS orchestrator_address,
      argMax(model_id, event_timestamp) AS model_id_latest,
      argMax(gpu_id, event_timestamp) AS gpu_id_latest
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp < to_ts
      AND orchestrator_address != '' AND local_address != ''
    GROUP BY orchestrator_address, local_address
  )
SELECT
  s.stream_id,
  s.request_id,
  s.orchestrator_address,
  c.model_id_latest AS model_id,
  c.gpu_id_latest AS gpu_id,
  s.session_start_ts
FROM session_base s
LEFT JOIN cap_latest c
  ON s.orchestrator_address = c.orchestrator_proxy_address
ORDER BY s.session_start_ts DESC
LIMIT 500;

-- 1) Output FPS + normalized efficiency ratio
-- orchestrator address in this table can be a dummy wallet address in some cases, 
-- so we need to join this with net capablities or a new fact table with a mapping between orch hot addr and dummy addr


-- AND orchestrator_address LIKE '0xa28%' -- speedy

-- AND orchestrator_address LIKE '0x52cf%' -- xode & open pool 


WITH
  toDateTime64('2026-01-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) AS window_start,
    orchestrator_address,
    avg(output_fps) AS avg_output_fps,
    quantile(0.95)(output_fps) AS p95_output_fps,
    avg(output_fps / nullIf(input_fps, 0)) AS avg_efficiency_ratio
FROM livepeer_analytics.ai_stream_status
WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
GROUP BY window_start, orchestrator_address
ORDER BY window_start, orchestrator_address;

-- 2) FPS jitter coefficient (separate from network jitter)
WITH
  toDateTime64('2026-01-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) AS window_start,
    orchestrator_address,
    stddevPop(output_fps) / nullIf(avg(output_fps), 0) AS jitter_coeff_fps
FROM livepeer_analytics.ai_stream_status
WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
GROUP BY window_start, orchestrator_address
ORDER BY window_start, orchestrator_address;

-- 3) Network jitter rollup from WHIP ingest metrics
WITH
  toDateTime64('2026-01-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) AS window_start,
    request_id,
    avg(video_jitter) AS avg_video_jitter,
    avg(audio_jitter) AS avg_audio_jitter
FROM stream_ingest_metrics
WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}
GROUP BY window_start, request_id
ORDER BY window_start, request_id;

-- 4) Bandwidth 

-- not possible with existing data


-- 5) Startup proxy latency (very similar to #6 and #7 with different trace edges)
-- startup_ms = first(gateway_receive_first_processed_segment) - first(gateway_receive_stream_request)
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  trace_edges AS (
    SELECT
        request_id,
        minIf(data_timestamp, trace_type = 'gateway_receive_stream_request') AS ts_start,
        minIf(data_timestamp, trace_type = 'gateway_receive_first_processed_segment') AS ts_first_processed
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
    GROUP BY request_id
)
SELECT
    count() AS requests_total,
    countIf(ts_start IS NOT NULL AND ts_first_processed IS NOT NULL) AS requests_with_pair,
    avgIf(dateDiff('millisecond', ts_start, ts_first_processed),
          ts_start IS NOT NULL AND ts_first_processed IS NOT NULL AND ts_first_processed >= ts_start) AS avg_startup_ms
FROM trace_edges;

-- 6) E2E proxy latency
-- e2e_ms = first(runner_send_first_processed_segment) - first(gateway_send_first_ingest_segment)
-- see #7 below for prompt-to-playable latency which includes the time from runner sending first processed to playable


-- 7) Prompt-to-playable proxy latency
-- prompt_to_playable_ms = first(gateway_receive_few_processed_segments) - min(stream start_time from ai_stream_status)
-- orch ingest to playable = first(gateway_receive_few_processed_segments) - first(gateway_send_first_ingest_segment)
-- first segment from runner to playable = first(gateway_receive_few_processed_segments) - first(runner_send_first_processed_segment)

WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  starts AS (
    SELECT request_id, min(start_time) AS start_time
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
    GROUP BY request_id
),
trace_edges AS (
    -- 
    SELECT request_id, 
    minIf(data_timestamp, trace_type = 'gateway_receive_few_processed_segments') AS ts_playable,
    minIf(data_timestamp, trace_type = 'gateway_send_first_ingest_segment') AS ts_sent_to_o,
    minIf(data_timestamp, trace_type = 'runner_send_first_processed_segment') AS ts_runner_first_processed

    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
    GROUP BY request_id
)
SELECT
    count() AS requests_total,
    countIf(start_time IS NOT NULL AND ts_playable IS NOT NULL AND ts_sent_to_o IS NOT NULL AND ts_runner_first_processed IS NOT NULL) AS requests_with_all_events,
    avgIf(dateDiff('millisecond', start_time, ts_playable),
          start_time IS NOT NULL AND ts_playable IS NOT NULL AND ts_playable >= start_time) AS avg_start_request_to_playable_ms,
    avgIf(dateDiff('millisecond', ts_sent_to_o, ts_playable),
          ts_sent_to_o IS NOT NULL AND ts_playable IS NOT NULL AND ts_playable >= ts_sent_to_o) AS avg_orch_ingest_to_playable_ms,
    avgIf(dateDiff('millisecond', ts_runner_first_processed, ts_playable),
        start_time IS NOT NULL AND ts_runner_first_processed IS NOT NULL AND ts_playable >= ts_runner_first_processed) AS avg_first_segment_from_runner_to_playable_ms
FROM starts
FULL OUTER JOIN trace_edges USING (request_id);

-- 8a) Stream startup outcome classification and overall unexcused failure rate
-- Mirrors other-project concepts:
--   success   -> gateway startup completed
--   excused   -> known excusable conditions
--   unexcused -> known stream that is neither success nor excused
-- Session key uses stream_id + request_id to avoid request_id reuse collisions.
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  trace_sessions AS (
    SELECT
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        ) AS session_key,
        anyIf(stream_id, stream_id != '') AS any_stream_id,
        anyIf(request_id, request_id != '') AS any_request_id,
        anyIf(orchestrator_address, orchestrator_address != '') AS orchestrator_address,
        anyIf(orchestrator_url, orchestrator_url != '') AS orchestrator_url,
        min(event_timestamp) AS first_seen_ts,
        max(event_timestamp) AS last_seen_ts,
        maxIf(1, trace_type = 'gateway_receive_stream_request') AS known_stream,
        maxIf(1, trace_type = 'gateway_receive_few_processed_segments') AS startup_success,
        maxIf(1, trace_type = 'gateway_no_orchestrators_available') AS no_orchestrators_available,
        maxIf(1, trace_type = 'gateway_ingest_stream_closed') AS ingest_closed
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
    GROUP BY session_key
),
error_sessions AS (
    SELECT
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        ) AS session_key,
        countIf(event_type = 'error') AS error_count,
        countIf(
            event_type = 'error' AND (
                lowerUTF8(message) LIKE '%no orchestrators available%' OR
                lowerUTF8(message) LIKE '%mediamtx ingest disconnected%' OR
                lowerUTF8(message) LIKE '%whip disconnected%' OR
                lowerUTF8(message) LIKE '%missing video%' OR
                lowerUTF8(message) LIKE '%ice connection state failed%' OR
                lowerUTF8(message) LIKE '%user disconnected%'
            )
        ) AS excusable_error_count
    FROM livepeer_analytics.ai_stream_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
    GROUP BY session_key
),
classified AS (
    SELECT
        t.session_key,
        t.any_stream_id AS stream_id,
        t.any_request_id AS request_id,
        t.orchestrator_address,
        t.orchestrator_url,
        t.known_stream,
        t.startup_success,
        t.no_orchestrators_available,
        t.ingest_closed,
        ifNull(e.error_count, 0) AS error_count,
        ifNull(e.excusable_error_count, 0) AS excusable_error_count,
        CASE
            WHEN t.known_stream = 0 THEN 'not_in_denominator'
            WHEN t.startup_success = 1 THEN 'success'
            WHEN t.no_orchestrators_available = 1 THEN 'excused'
            WHEN ifNull(e.error_count, 0) > 0 AND ifNull(e.error_count, 0) = ifNull(e.excusable_error_count, 0) THEN 'excused'
            ELSE 'unexcused'
        END AS startup_status
    FROM trace_sessions t
    LEFT JOIN error_sessions e USING (session_key)
)
SELECT
    countIf(known_stream = 1) AS known_stream_sessions,
    countIf(known_stream = 1 AND startup_status = 'success') AS success_sessions,
    countIf(known_stream = 1 AND startup_status = 'excused') AS excused_sessions,
    countIf(known_stream = 1 AND startup_status = 'unexcused') AS unexcused_sessions,
    countIf(known_stream = 1 AND startup_status = 'unexcused') / nullIf(countIf(known_stream = 1), 0) AS unexcused_failure_rate
FROM classified;

-- 8b) Optional detailed output for review of classification decisions
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  trace_sessions AS (
    SELECT
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        ) AS session_key,
        anyIf(stream_id, stream_id != '') AS any_stream_id,
        anyIf(request_id, request_id != '') AS any_request_id,
        anyIf(orchestrator_address, orchestrator_address != '') AS orchestrator_address,
        anyIf(orchestrator_url, orchestrator_url != '') AS orchestrator_url,
        min(event_timestamp) AS first_seen_ts,
        max(event_timestamp) AS last_seen_ts,
        maxIf(1, trace_type = 'gateway_receive_stream_request') AS known_stream,
        maxIf(1, trace_type = 'gateway_receive_few_processed_segments') AS startup_success,
        maxIf(1, trace_type = 'gateway_no_orchestrators_available') AS no_orchestrators_available,
        maxIf(1, trace_type = 'gateway_ingest_stream_closed') AS ingest_closed
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
    GROUP BY session_key
),
error_sessions AS (
    SELECT
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        ) AS session_key,
        countIf(event_type = 'error') AS error_count,
        countIf(
            event_type = 'error' AND (
                lowerUTF8(message) LIKE '%no orchestrators available%' OR
                lowerUTF8(message) LIKE '%mediamtx ingest disconnected%' OR
                lowerUTF8(message) LIKE '%whip disconnected%' OR
                lowerUTF8(message) LIKE '%missing video%' OR
                lowerUTF8(message) LIKE '%ice connection state failed%' OR
                lowerUTF8(message) LIKE '%user disconnected%'
            )
        ) AS excusable_error_count,
        groupArrayIf(message, event_type = 'error') AS error_messages
    FROM livepeer_analytics.ai_stream_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
    GROUP BY session_key
),
classified AS (
    SELECT
        t.*,
        ifNull(e.error_count, 0) AS error_count,
        ifNull(e.excusable_error_count, 0) AS excusable_error_count,
        ifNull(e.error_messages, []) AS error_messages,
        CASE
            WHEN t.known_stream = 0 THEN 'not_in_denominator'
            WHEN t.startup_success = 1 THEN 'success'
            WHEN t.no_orchestrators_available = 1 THEN 'excused'
            WHEN ifNull(e.error_count, 0) > 0 AND ifNull(e.error_count, 0) = ifNull(e.excusable_error_count, 0) THEN 'excused'
            ELSE 'unexcused'
        END AS startup_status
    FROM trace_sessions t
    LEFT JOIN error_sessions e USING (session_key)
)
SELECT
    session_key,
    any_stream_id,
    any_request_id,
    orchestrator_address,
    orchestrator_url,
    known_stream,
    startup_success,
    no_orchestrators_available,
    ingest_closed,
    error_count,
    excusable_error_count,
    startup_status,
    error_messages,
    first_seen_ts,
    last_seen_ts
FROM classified
WHERE known_stream = 1
ORDER BY startup_status DESC, first_seen_ts DESC
LIMIT 500;

-- 9a) Swap rate (session-level)
-- Session key uses stream_id + request_id to avoid request_id reuse collisions.
-- Swap detection uses:
--   1) explicit trace events: trace_type = 'orchestrator_swap'
--   2) fallback: more than one orchestrator seen in same session
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  session_swaps AS (
    SELECT
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        ) AS session_key,
        stream_id,
        request_id,
        uniqExactIf(orchestrator_address, orchestrator_address != '') AS orch_count,
        countIf(trace_type = 'orchestrator_swap') AS explicit_swap_events
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND (stream_id != '' OR request_id != '')
    GROUP BY session_key, stream_id, request_id
)
SELECT
    count() AS sessions_total,
    countIf(orch_count > 0) AS sessions_with_orch,
    sum(explicit_swap_events) AS explicit_swap_events_total,
    countIf(explicit_swap_events > 0 OR orch_count > 1) AS sessions_with_swaps,
    countIf(explicit_swap_events > 0 OR orch_count > 1) / nullIf(count(), 0) AS swap_rate
FROM session_swaps;

-- 9b) Swaps by orchestrator (explicit swap events)
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts
  SELECT
    orchestrator_address,
    count() AS explicit_swap_events,
    uniqExact(
        multiIf(
            stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
            stream_id != '', concat(stream_id, '|_missing_request'),
            request_id != '', concat('_missing_stream|', request_id),
            ''
        )
    ) AS sessions_with_explicit_swap
FROM livepeer_analytics.stream_trace_events
WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
  AND trace_type = 'orchestrator_swap'
  AND orchestrator_address != ''
GROUP BY orchestrator_address
ORDER BY explicit_swap_events DESC, sessions_with_explicit_swap DESC;

-- 9c) Swaps per stream (explicit + fallback)
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts,
  per_stream AS (
    SELECT
        stream_id,
        countIf(trace_type = 'orchestrator_swap') AS explicit_swap_events,
        uniqExactIf(orchestrator_address, orchestrator_address != '') AS orch_count,
        count() AS trace_rows
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
      AND stream_id != ''
    GROUP BY stream_id
)
SELECT
    stream_id,
    trace_rows,
    orch_count,
    explicit_swap_events,
    (explicit_swap_events > 0 OR orch_count > 1) AS stream_had_swap
FROM per_stream
ORDER BY explicit_swap_events DESC, orch_count DESC, trace_rows DESC
LIMIT 200;

-- 10) Trace edge dictionary discovery
WITH
  toDateTime64('2026-02-10 00:00:00', 3, 'UTC') AS from_ts,
  toDateTime64('2026-02-11 00:00:00', 3, 'UTC') AS to_ts
  SELECT
    trace_type,
    count() AS cnt,
    uniqExact(request_id) AS req_cnt
FROM livepeer_analytics.stream_trace_events
WHERE event_timestamp >= from_ts AND event_timestamp < to_ts
GROUP BY trace_type
ORDER BY cnt DESC;

-- ============================================================
-- Serving Layer Alignment Checks (Step 3)
-- ============================================================

-- 11) Rollup parity: performance rollup vs raw status facts
-- Compares 1-minute avg output FPS from:
--   A) fact_stream_status_samples
--   B) v_api_gpu_metrics (backed by agg_stream_performance_1m)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts,
  raw AS (
    SELECT
      toStartOfInterval(sample_ts, INTERVAL 1 MINUTE) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      avg(output_fps) AS raw_avg_output_fps
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= from_ts AND sample_ts < to_ts
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id
  ),
  api AS (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      avg_output_fps AS api_avg_output_fps
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= from_ts AND window_start < to_ts
  )
SELECT
  count() AS joined_rows,
  avg(abs(raw.raw_avg_output_fps - api.api_avg_output_fps)) AS mean_abs_diff_fps,
  max(abs(raw.raw_avg_output_fps - api.api_avg_output_fps)) AS max_abs_diff_fps
FROM raw
INNER JOIN api USING (window_start, orchestrator_address, pipeline, model_id, gpu_id);

-- 12) Rollup parity: reliability rollup vs raw workflow sessions
-- Compares known/unexcused/swapped session counts at 1-hour grain.
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts,
  latest_sessions AS (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      argMax(model_id, version) AS model_id,
      argMax(gpu_id, version) AS gpu_id,
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
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64(swap_count > 0)) AS raw_swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id
  ),
  api AS (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      known_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= from_ts AND window_start < to_ts
  )
SELECT
  count() AS joined_rows,
  sum(abs(raw.raw_known_sessions - api.known_sessions)) AS total_known_diff,
  sum(abs(raw.raw_unexcused_sessions - api.unexcused_sessions)) AS total_unexcused_diff,
  sum(abs(raw.raw_swapped_sessions - api.swapped_sessions)) AS total_swapped_diff
FROM raw
INNER JOIN api USING (window_start, orchestrator_address, pipeline, model_id, gpu_id);

-- 13) API view sanity: null/empty key coverage for serving dimensions
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  'v_api_gpu_metrics' AS view_name,
  count() AS rows_total,
  countIf(orchestrator_address = '' OR orchestrator_address IS NULL) AS rows_missing_orchestrator,
  countIf(pipeline = '' OR pipeline IS NULL) AS rows_missing_pipeline
FROM livepeer_analytics.v_api_gpu_metrics
WHERE window_start >= from_ts AND window_start < to_ts
UNION ALL
SELECT
  'v_api_network_demand' AS view_name,
  count() AS rows_total,
  countIf(gateway = '' OR gateway IS NULL) AS rows_missing_gateway,
  countIf(pipeline = '' OR pipeline IS NULL) AS rows_missing_pipeline
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= from_ts AND window_start < to_ts
UNION ALL
SELECT
  'v_api_sla_compliance' AS view_name,
  count() AS rows_total,
  countIf(orchestrator_address = '' OR orchestrator_address IS NULL) AS rows_missing_orchestrator,
  countIf(pipeline = '' OR pipeline IS NULL) AS rows_missing_pipeline
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= from_ts AND window_start < to_ts;


=================== SCHEMA VALIDATION QUERIES ===================
-- Acceptance checks (last 24h UTC)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts

-- 1) Sessions: total rows vs unique session ids vs latest-version sessions
SELECT
  count() AS rows_total,
  uniqExact(workflow_session_id) AS sessions_unique,
  countIf(version = max_version) AS latest_rows
FROM (
  SELECT
    workflow_session_id,
    version,
    max(version) OVER (PARTITION BY workflow_session_id) AS max_version
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
);

-- 2) Session classification distribution (latest row per session)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  startup_success,
  startup_excused,
  startup_unexcused,
  count() AS sessions
FROM (
  SELECT
    workflow_session_id,
    argMax(startup_success, version) AS startup_success,
    argMax(startup_excused, version) AS startup_excused,
    argMax(startup_unexcused, version) AS startup_unexcused
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
  GROUP BY workflow_session_id
)
GROUP BY startup_success, startup_excused, startup_unexcused
ORDER BY sessions DESC;

-- 3) Swap distribution (latest row per session)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS sessions_total,
  countIf(swap_count > 0) AS sessions_with_swaps,
  countIf(swap_count > 0) / nullIf(count(), 0) AS swap_rate
FROM (
  SELECT
    workflow_session_id,
    argMax(swap_count, version) AS swap_count
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
  GROUP BY workflow_session_id
);

-- 4) Segment counts per session
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  quantileExact(0.5)(segment_count) AS p50_segments,
  quantileExact(0.95)(segment_count) AS p95_segments,
  max(segment_count) AS max_segments
FROM (
  SELECT
    workflow_session_id,
    uniqExact(segment_index) AS segment_count
  FROM livepeer_analytics.fact_workflow_session_segments
  WHERE segment_start_ts >= from_ts AND segment_start_ts < to_ts
  GROUP BY workflow_session_id
);

-- 5) Segment close quality
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS segment_rows,
  countIf(segment_end_ts IS NOT NULL) AS segment_closed_rows,
  countIf(segment_end_ts IS NULL) AS segment_open_rows
FROM livepeer_analytics.fact_workflow_session_segments
WHERE segment_start_ts >= from_ts AND segment_start_ts < to_ts;

-- 6) Param update marker counts
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS param_update_rows,
  uniqExact(workflow_session_id) AS sessions_with_param_updates
FROM livepeer_analytics.fact_workflow_param_updates
WHERE update_ts >= from_ts AND update_ts < to_ts;

-- 7) Param updates by model/gpu
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  ifNull(model_id, '_null') AS model_id,
  ifNull(gpu_id, '_null') AS gpu_id,
  count() AS updates
FROM livepeer_analytics.fact_workflow_param_updates
WHERE update_ts >= from_ts AND update_ts < to_ts
GROUP BY model_id, gpu_id
ORDER BY updates DESC
LIMIT 50;

-- 8) Attribution coverage (latest row per session)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS sessions_total,
  countIf(model_id IS NOT NULL AND model_id != '') AS sessions_with_model,
  countIf(gpu_id IS NOT NULL AND gpu_id != '') AS sessions_with_gpu,
  countIf(gpu_attribution_method != 'none') AS sessions_with_attribution,
  countIf(model_id IS NOT NULL AND model_id != '') / nullIf(count(),0) AS model_coverage,
  countIf(gpu_id IS NOT NULL AND gpu_id != '') / nullIf(count(),0) AS gpu_coverage
FROM (
  SELECT
    workflow_session_id,
    argMax(model_id, version) AS model_id,
    argMax(gpu_id, version) AS gpu_id,
    argMax(gpu_attribution_method, version) AS gpu_attribution_method
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
  GROUP BY workflow_session_id
);

-- 9) Session vs segment consistency (orchestrator changes vs segment count)
WITH
  now64(3, 'UTC') AS to_ts,
  to_ts - INTERVAL 24 HOUR AS from_ts
SELECT
  count() AS sessions_checked,
  countIf(seg_cnt > 1) AS sessions_multi_segment,
  countIf(session_swap_count > 0 AND seg_cnt = 1) AS swap_without_segment_change
FROM (
  SELECT
    s.workflow_session_id,
    s.session_swap_count,
    ifNull(g.seg_cnt, 0) AS seg_cnt
  FROM (
    SELECT
      workflow_session_id,
      argMax(swap_count, version) AS session_swap_count
    FROM livepeer_analytics.fact_workflow_sessions
    WHERE session_start_ts >= from_ts AND session_start_ts < to_ts
    GROUP BY workflow_session_id
  ) s
  LEFT JOIN (
    SELECT
      workflow_session_id,
      uniqExact(segment_index) AS seg_cnt
    FROM livepeer_analytics.fact_workflow_session_segments
    WHERE segment_start_ts >= from_ts AND segment_start_ts < to_ts
    GROUP BY workflow_session_id
  ) g USING (workflow_session_id)
);
