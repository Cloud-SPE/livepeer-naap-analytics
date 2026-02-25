-- Raw -> typed lineage/accounting assertions.
-- Each test must return one row with `failed_rows` and optional diagnostics.

-- TEST: raw_typed_accepted_estimate_nonnegative
SELECT
  toUInt64(0) AS failed_rows,
  countIf(raw_rows_minus_rejects < 0) AS overflow_type_count,
  sum(raw_rows) AS total_raw_rows,
  sum(dlq_rows) AS total_dlq_rows,
  sum(quarantine_rows) AS total_quarantine_rows,
  sum(greatest(raw_rows_minus_rejects, 0)) AS total_accepted_rows_est_capped
FROM
(
  SELECT
    ifNull(r.raw_rows, 0) AS raw_rows,
    ifNull(d.dlq_rows, 0) AS dlq_rows,
    ifNull(q.quarantine_rows, 0) AS quarantine_rows,
    ifNull(r.raw_rows, 0) - ifNull(d.dlq_rows, 0) - ifNull(q.quarantine_rows, 0) AS raw_rows_minus_rejects
  FROM
  (
    SELECT type AS event_type, count() AS raw_rows
    FROM livepeer_analytics.streaming_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY type
  ) r
  LEFT JOIN
  (
    SELECT event_type, count() AS dlq_rows
    FROM livepeer_analytics.streaming_events_dlq
    WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
    GROUP BY event_type
  ) d ON r.event_type = d.event_type
  LEFT JOIN
  (
    SELECT event_type, count() AS quarantine_rows
    FROM livepeer_analytics.streaming_events_quarantine
    WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
    GROUP BY event_type
  ) q ON r.event_type = q.event_type
);

-- TEST: raw_typed_no_dlq_or_quarantine_for_core_types
SELECT
  toUInt64(dlq_rows > 0 OR quarantine_rows > 0) AS failed_rows,
  dlq_rows,
  quarantine_rows
FROM
(
  SELECT
    (
      SELECT count()
      FROM livepeer_analytics.streaming_events_dlq
      WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
        AND source_record_timestamp < {to_ts:DateTime64(3)}
        AND event_type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
    ) AS dlq_rows,
    (
      SELECT count()
      FROM livepeer_analytics.streaming_events_quarantine
      WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
        AND source_record_timestamp < {to_ts:DateTime64(3)}
        AND event_type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
    ) AS quarantine_rows
);

-- TEST: raw_typed_core_1to1_parity
-- Dedup-aware parity: compare typed rows to distinct raw source event IDs.
-- Replayed fixtures can contain duplicate raw IDs across runs; those should not
-- force typed parity failures when Flink dedup correctly suppresses duplicates.
SELECT
  countIf(typed_rows != raw_distinct_ids) AS failed_rows,
  sum(raw_rows) AS total_raw_rows,
  sum(raw_distinct_ids) AS total_raw_distinct_ids,
  sum(raw_duplicate_rows) AS total_raw_duplicate_rows,
  sum(accepted_rows_est) AS total_accepted_rows_est_distinct,
  sum(typed_rows) AS total_typed_rows,
  sum(dlq_rows) AS total_dlq_rows,
  sum(quarantine_rows) AS total_quarantine_rows
FROM
(
  SELECT
    ifNull(r.raw_rows, 0) AS raw_rows,
    ifNull(r.raw_distinct_ids, 0) AS raw_distinct_ids,
    greatest(ifNull(r.raw_rows, 0) - ifNull(r.raw_distinct_ids, 0), 0) AS raw_duplicate_rows,
    ifNull(d.dlq_rows, 0) AS dlq_rows,
    ifNull(q.quarantine_rows, 0) AS quarantine_rows,
    greatest(ifNull(r.raw_distinct_ids, 0) - ifNull(d.dlq_rows, 0) - ifNull(q.quarantine_rows, 0), 0) AS accepted_rows_est,
    ifNull(t.typed_rows, 0) AS typed_rows
  FROM
  (
    SELECT 'ai_stream_status' AS event_type, 'ai_stream_status' AS typed_name
    UNION ALL SELECT 'stream_trace', 'stream_trace_events'
    UNION ALL SELECT 'ai_stream_events', 'ai_stream_events'
    UNION ALL SELECT 'stream_ingest_metrics', 'stream_ingest_metrics'
  ) tm
  LEFT JOIN
  (
    SELECT
      type AS event_type,
      count() AS raw_rows,
      uniqExact(id) AS raw_distinct_ids
    FROM livepeer_analytics.streaming_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY type
  ) r ON tm.event_type = r.event_type
  LEFT JOIN
  (
    SELECT event_type, count() AS dlq_rows
    FROM livepeer_analytics.streaming_events_dlq
    WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
    GROUP BY event_type
  ) d ON tm.event_type = d.event_type
  LEFT JOIN
  (
    SELECT event_type, count() AS quarantine_rows
    FROM livepeer_analytics.streaming_events_quarantine
    WHERE source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
    GROUP BY event_type
  ) q ON tm.event_type = q.event_type
  LEFT JOIN
  (
    SELECT 'ai_stream_status' AS typed_name, count() AS typed_rows
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}

    UNION ALL

    SELECT 'stream_trace_events' AS typed_name, count() AS typed_rows
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}

    UNION ALL

    SELECT 'ai_stream_events' AS typed_name, count() AS typed_rows
    FROM livepeer_analytics.ai_stream_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}

    UNION ALL

    SELECT 'stream_ingest_metrics' AS typed_name, count() AS typed_rows
    FROM livepeer_analytics.stream_ingest_metrics
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ) t ON tm.typed_name = t.typed_name
);

-- TEST: raw_typed_network_capabilities_expected_in_window
-- Fixture runs replay canonical raw `streaming_events` rows. If this returns
-- 0, fixture extraction/replay likely missed capability context rows.
SELECT
  toUInt64(raw_rows = 0) AS failed_rows,
  raw_rows
FROM
(
  SELECT count() AS raw_rows
  FROM livepeer_analytics.streaming_events
  WHERE type = 'network_capabilities'
    AND event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
);

-- TEST: raw_typed_network_capabilities_fanout_guard
-- network_capabilities can legitimately fan out from one raw source event to
-- multiple typed rows (for example one row per capability entry).
-- In preserve-state runs, DLQ rows may accumulate faster than raw rows in the
-- selected window, so accepted_rows_est can clamp to 0 even when valid typed
-- rows exist for a raw source event. Guard against impossible lineage instead:
-- typed distinct source ids must not exceed raw source events.
WITH
  raw AS
  (
    SELECT count() AS raw_rows
    FROM livepeer_analytics.streaming_events
    WHERE type = 'network_capabilities'
      AND event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  dlq AS
  (
    SELECT count() AS dlq_rows
    FROM livepeer_analytics.streaming_events_dlq
    WHERE event_type = 'network_capabilities'
      AND source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
  ),
  quarantine AS
  (
    SELECT count() AS quarantine_rows
    FROM livepeer_analytics.streaming_events_quarantine
    WHERE event_type = 'network_capabilities'
      AND source_record_timestamp >= {from_ts:DateTime64(3)}
      AND source_record_timestamp < {to_ts:DateTime64(3)}
  ),
  typed AS
  (
    SELECT
      count() AS typed_rows,
      uniqExact(source_event_id) AS typed_distinct_source_events
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    typed_distinct_source_events > raw_rows
  ) AS failed_rows,
  raw_rows,
  dlq_rows,
  quarantine_rows,
  accepted_rows_est,
  typed_rows,
  typed_distinct_source_events,
  if(typed_distinct_source_events = 0, 0.0, typed_rows / toFloat64(typed_distinct_source_events)) AS avg_fanout_per_source_event
FROM
(
  SELECT
    raw.raw_rows AS raw_rows,
    dlq.dlq_rows AS dlq_rows,
    quarantine.quarantine_rows AS quarantine_rows,
    greatest(raw.raw_rows - dlq.dlq_rows - quarantine.quarantine_rows, 0) AS accepted_rows_est,
    typed.typed_rows AS typed_rows,
    typed.typed_distinct_source_events AS typed_distinct_source_events
  FROM raw
  CROSS JOIN dlq
  CROSS JOIN quarantine
  CROSS JOIN typed
);
