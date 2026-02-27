-- Integration assertions for raw -> typed -> facts -> rollups -> API views.
-- Each test must return exactly one row with:
--   failed_rows UInt64/Int64
-- Additional columns are treated as diagnostics.

-- TEST: raw_events_present
-- Verifies that the core raw/typed ingress objects have at least one row in the validation window.
-- Requirement: the pipeline run must have observable input and typed event coverage before deeper checks.
SELECT
  countIf(rows_window = 0) AS failed_rows,
  groupArrayIf(object_name, rows_window = 0) AS missing_objects
FROM
(
  SELECT 'streaming_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.streaming_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'ai_stream_status' AS object_name, count() AS rows_window
  FROM livepeer_analytics.ai_stream_status
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'stream_trace_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.stream_trace_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
);

-- TEST: capability_dimension_mvs_present
-- Verifies required capability materialized views are present in ClickHouse system metadata.
-- Requirement: dimensional projection infrastructure must exist before validating capability outputs.
SELECT
  countIf(mv_exists = 0) AS failed_rows,
  groupArrayIf(mv_name, mv_exists = 0) AS missing_mvs
FROM
(
  SELECT
    mv_name,
    toUInt8(count() > 0) AS mv_exists
  FROM
  (
    SELECT 'mv_network_capabilities_to_dim_orchestrator_capability_snapshots' AS mv_name
    UNION ALL SELECT 'mv_network_capabilities_advertised_to_dim_orchestrator_capability_advertised_snapshots'
    UNION ALL SELECT 'mv_network_capabilities_model_constraints_to_dim_orchestrator_capability_model_constraints'
    UNION ALL SELECT 'mv_network_capabilities_prices_to_dim_orchestrator_capability_prices'
  ) required
  LEFT JOIN
  (
    SELECT name
    FROM system.tables
    WHERE database = 'livepeer_analytics'
      AND engine = 'MaterializedView'
  ) present
    ON required.mv_name = present.name
  GROUP BY mv_name
);

-- TEST: capability_dimensions_projecting
-- Verifies capability raw/typed tables are being projected into their dimension snapshot tables.
-- Requirement: when source rows exist, corresponding dimensional rows must also exist in-window.
WITH
  raw_rows AS
  (
    SELECT
      (SELECT count() FROM livepeer_analytics.network_capabilities WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS caps_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_advertised WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS adv_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_model_constraints WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS mc_rows,
      (SELECT count() FROM livepeer_analytics.network_capabilities_prices WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS price_rows
  ),
  dim_rows AS
  (
    SELECT
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_snapshots WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_caps_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_advertised_snapshots WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_adv_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_model_constraints WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_mc_rows,
      (SELECT count() FROM livepeer_analytics.dim_orchestrator_capability_prices WHERE snapshot_ts >= {from_ts:DateTime64(3)} AND snapshot_ts < {to_ts:DateTime64(3)}) AS dim_price_rows
  )
SELECT
  toUInt64(
    if(caps_rows = 0, 0, dim_caps_rows = 0)
    + if(adv_rows = 0, 0, dim_adv_rows = 0)
    + if(mc_rows = 0, 0, dim_mc_rows = 0)
    + if(price_rows = 0, 0, dim_price_rows = 0)
  ) AS failed_rows,
  caps_rows,
  dim_caps_rows,
  adv_rows,
  dim_adv_rows,
  mc_rows,
  dim_mc_rows,
  price_rows,
  dim_price_rows
FROM raw_rows
CROSS JOIN dim_rows;

-- TEST: capability_snapshot_gpu_coverage
-- Verifies capability snapshot dimensions preserve per-event GPU coverage from typed capabilities.
-- Requirement: per `(source_event_id, orchestrator, pipeline, model_id)` GPU cardinality must match.
WITH
  raw_gpu AS
  (
    SELECT
      source_event_id AS source_event_uid,
      lower(orchestrator_address) AS orchestrator_address,
      pipeline,
      model_id,
      countDistinctIf(gpu_id, ifNull(gpu_id, '') != '') AS raw_gpu_count
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid, orchestrator_address, pipeline, model_id
    HAVING raw_gpu_count > 0
  ),
  dim_gpu AS
  (
    SELECT
      source_event_uid,
      lower(orchestrator_address) AS orchestrator_address,
      pipeline,
      model_id,
      countDistinctIf(gpu_id, ifNull(gpu_id, '') != '') AS dim_gpu_count
    FROM livepeer_analytics.dim_orchestrator_capability_snapshots
    WHERE snapshot_ts >= {from_ts:DateTime64(3)}
      AND snapshot_ts < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid, orchestrator_address, pipeline, model_id
  ),
  joined AS
  (
    SELECT
      r.source_event_uid,
      r.orchestrator_address,
      r.pipeline,
      r.model_id,
      r.raw_gpu_count,
      ifNull(d.dim_gpu_count, toUInt64(0)) AS dim_gpu_count,
      toUInt8(d.source_event_uid IS NULL) AS missing_dim_key
    FROM raw_gpu r
    LEFT JOIN dim_gpu d
      ON d.source_event_uid = r.source_event_uid
     AND d.orchestrator_address = r.orchestrator_address
     AND d.pipeline = r.pipeline
     AND d.model_id = r.model_id
  )
SELECT
  toUInt64(if(raw_rows = 0, 0, loss_rows > 0 OR missing_dim_keys > 0)) AS failed_rows,
  raw_rows,
  dim_rows,
  loss_rows,
  missing_dim_keys,
  total_gpu_deficit
FROM
(
  SELECT
    (SELECT count() FROM raw_gpu) AS raw_rows,
    (SELECT count() FROM dim_gpu) AS dim_rows,
    (
      SELECT count()
      FROM joined
      WHERE dim_gpu_count < raw_gpu_count
    ) AS loss_rows,
    (
      SELECT countIf(missing_dim_key = 1)
      FROM joined
    ) AS missing_dim_keys,
    (
      SELECT ifNull(sum(greatest(toInt64(raw_gpu_count) - toInt64(dim_gpu_count), 0)), 0)
      FROM joined
    ) AS total_gpu_deficit
);

-- TEST: capability_prices_key_multiplicity_guard
-- Verifies price rows remain unique at current serving key grain.
-- Requirement: no duplicate price rows per `(source_event_id, orchestrator, capability_id, event_timestamp)`.
WITH duplicate_groups AS
(
  SELECT
    source_event_id,
    lower(orchestrator_address) AS orchestrator_address,
    capability_id,
    event_timestamp,
    count() AS rows_in_group
  FROM livepeer_analytics.network_capabilities_prices
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}
  GROUP BY source_event_id, orchestrator_address, capability_id, event_timestamp
  HAVING rows_in_group > 1
)
SELECT
  toUInt64(count() > 0) AS failed_rows,
  count() AS duplicate_groups,
  ifNull(sum(rows_in_group), 0) AS duplicate_rows,
  ifNull(max(rows_in_group), 0) AS max_rows_in_group
FROM duplicate_groups;

-- TEST: session_fact_present
-- Verifies lifecycle session fact rows exist in the window.
-- Requirement: Flink sessionization must emit `fact_workflow_sessions` for the replayed fixture period.
SELECT
  toUInt64(count() = 0) AS failed_rows,
  count() AS sessions_window
FROM livepeer_analytics.fact_workflow_sessions
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: core_raw_to_silver_gold_nonempty
-- Verifies core event flow has accepted raw rows and non-empty downstream silver/gold coverage.
-- Requirement: prevent false-green runs where all core events are rejected into DLQ/quarantine.
WITH
  raw_core AS
  (
    SELECT uniqExact(id) AS raw_distinct_ids
    FROM livepeer_analytics.streaming_events
    WHERE type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
      AND event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  rejected_core AS
  (
    SELECT uniqExact(event_id) AS rejected_distinct_ids
    FROM
    (
      SELECT event_id
      FROM livepeer_analytics.streaming_events_dlq
      WHERE event_type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
        AND ifNull(event_timestamp, source_record_timestamp) >= {from_ts:DateTime64(3)}
        AND ifNull(event_timestamp, source_record_timestamp) < {to_ts:DateTime64(3)}

      UNION ALL

      SELECT event_id
      FROM livepeer_analytics.streaming_events_quarantine
      WHERE event_type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
        AND ifNull(event_timestamp, source_record_timestamp) >= {from_ts:DateTime64(3)}
        AND ifNull(event_timestamp, source_record_timestamp) < {to_ts:DateTime64(3)}
    )
    WHERE event_id != ''
  ),
  silver_core AS
  (
    SELECT
      (SELECT count() FROM livepeer_analytics.fact_stream_status_samples WHERE sample_ts >= {from_ts:DateTime64(3)} AND sample_ts < {to_ts:DateTime64(3)}) AS status_rows,
      (SELECT count() FROM livepeer_analytics.fact_stream_trace_edges WHERE edge_ts >= {from_ts:DateTime64(3)} AND edge_ts < {to_ts:DateTime64(3)}) AS trace_rows
  ),
  gold_core AS
  (
    SELECT count() AS session_rows
    FROM livepeer_analytics.fact_workflow_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    raw_distinct_ids = 0
    OR accepted_distinct_ids_est = 0
    OR status_rows = 0
    OR trace_rows = 0
    OR session_rows = 0
  ) AS failed_rows,
  raw_distinct_ids,
  rejected_distinct_ids,
  accepted_distinct_ids_est,
  status_rows,
  trace_rows,
  session_rows
FROM
(
  SELECT
    raw_core.raw_distinct_ids AS raw_distinct_ids,
    rejected_core.rejected_distinct_ids AS rejected_distinct_ids,
    greatest(raw_core.raw_distinct_ids - rejected_core.rejected_distinct_ids, 0) AS accepted_distinct_ids_est,
    silver_core.status_rows AS status_rows,
    silver_core.trace_rows AS trace_rows,
    gold_core.session_rows AS session_rows
  FROM raw_core
  CROSS JOIN rejected_core
  CROSS JOIN silver_core
  CROSS JOIN gold_core
);

-- TEST: network_capabilities_raw_and_typed_present
-- Verifies network capabilities events are present in both raw and typed layers.
-- Requirement: fixture windows intended for attribution must include parseable capabilities coverage.
-- Capability assertions are explicit because fixture windows are expected to
-- include this event family; missing rows usually indicate window drift.
WITH
  raw AS
  (
    SELECT count() AS raw_rows
    FROM livepeer_analytics.streaming_events
    WHERE type = 'network_capabilities'
      AND event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  typed AS
  (
    SELECT count() AS typed_rows
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(raw_rows = 0 OR typed_rows = 0) AS failed_rows,
  raw_rows,
  typed_rows
FROM raw
CROSS JOIN typed;

-- TEST: status_raw_to_silver_projection
-- Verifies each typed `ai_stream_status` row is projected into silver fact status samples by source UID.
-- Requirement: non-stateful status projection must be lossless for rows in scope.
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.ai_stream_status
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: trace_raw_to_silver_projection
-- Verifies each typed `stream_trace_events` row is projected into silver trace edges by source UID.
-- Requirement: non-stateful trace projection must be lossless for rows in scope.
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: ingest_raw_to_silver_projection
-- Verifies each typed `stream_ingest_metrics` row is projected into silver ingest samples by source UID.
-- Requirement: non-stateful ingest projection must be lossless when ingest metrics are present.
WITH
  typed AS
  (
    SELECT toString(cityHash64(raw_json)) AS source_event_uid
    FROM livepeer_analytics.stream_ingest_metrics
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  silver AS
  (
    SELECT source_event_uid
    FROM livepeer_analytics.fact_stream_ingest_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(if(typed_rows = 0, 0, missing_in_silver)) AS failed_rows,
  typed_rows,
  projected_rows,
  missing_in_silver
FROM
(
  SELECT
    count() AS typed_rows,
    countIf(s.source_event_uid IS NOT NULL) AS projected_rows,
    countIf(s.source_event_uid IS NULL) AS missing_in_silver
  FROM typed t
  LEFT JOIN silver s USING (source_event_uid)
);

-- TEST: session_final_uniqueness
-- Verifies `FINAL` semantics yield exactly one latest row per workflow session id.
-- Requirement: versioned upsert behavior in `fact_workflow_sessions` must converge deterministically.
WITH window_rows AS
(
  SELECT workflow_session_id, version
  FROM livepeer_analytics.fact_workflow_sessions FINAL
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
),
latest_versions AS
(
  SELECT workflow_session_id, max(version) AS max_version
  FROM window_rows
  GROUP BY workflow_session_id
),
latest_rows AS
(
  SELECT
    w.workflow_session_id,
    count() AS latest_row_count
  FROM window_rows w
  INNER JOIN latest_versions lv
    ON lv.workflow_session_id = w.workflow_session_id
   AND lv.max_version = w.version
  GROUP BY w.workflow_session_id
)
SELECT
  countIf(latest_row_count != 1) AS failed_rows,
  count() AS unique_sessions_checked
FROM latest_rows;

-- TEST: workflow_session_has_identifier
-- Verifies session fact rows never have an empty workflow session id.
-- Requirement: session identity is mandatory for all downstream joins and scenario diagnostics.
SELECT
  countIf(workflow_session_id = '') AS failed_rows,
  count() AS total_rows
FROM livepeer_analytics.fact_workflow_sessions FINAL
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: swap_signal_split_consistency
-- Verifies split swap contract: legacy `swap_count` must equal `confirmed_swap_count`.
-- Requirement: compatibility field stays confirmed-only while inferred changes are tracked separately.
SELECT
  countIf(swap_count != confirmed_swap_count) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.fact_workflow_sessions FINAL
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: gold_sessions_use_canonical_orchestrator_identity
-- Verifies non-empty session orchestrator addresses align with canonical
-- orchestrator identities observed in network_capabilities for the same window.
-- Requirement: gold/session facts should expose canonical orchestrator identity,
-- not hot-wallet/local addresses from source event payloads.
WITH
  canonical_capability_wallets AS
  (
    SELECT DISTINCT lower(orchestrator_address) AS orchestrator_address
    FROM livepeer_analytics.network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
      AND orchestrator_address != ''
  ),
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      lower(argMax(orchestrator_address, version)) AS orchestrator_address
    FROM livepeer_analytics.fact_workflow_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  )
SELECT
  toUInt64(
    if(
      (SELECT count() FROM canonical_capability_wallets) = 0,
      0,
      noncanonical_session_orchestrators
    )
  ) AS failed_rows,
  noncanonical_session_orchestrators,
  session_orchestrators_checked
FROM
(
  SELECT
    countIf(
      s.orchestrator_address != ''
      AND c.orchestrator_address IS NULL
    ) AS noncanonical_session_orchestrators,
    countIf(s.orchestrator_address != '') AS session_orchestrators_checked
  FROM latest_sessions s
  LEFT JOIN canonical_capability_wallets c
    ON c.orchestrator_address = s.orchestrator_address
);

-- TEST: swapped_sessions_have_evidence
-- Verifies sessions with `swap_count > 0` (confirmed swap signal) have explicit swap evidence in either silver trace edges
-- (by workflow_session_id) or typed trace events (by stream_id/request_id fallback).
WITH
  swapped AS (
    SELECT
      workflow_session_id,
      stream_id,
      request_id
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND swap_count > 0
  ),
  seg AS (
    SELECT
      workflow_session_id,
      uniqExactIf(orchestrator_address, orchestrator_address != '') AS orch_count
    FROM livepeer_analytics.fact_workflow_session_segments FINAL
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  edge AS (
    SELECT
      workflow_session_id,
      max(toUInt8(is_swap_event > 0)) AS has_swap_edge
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
    GROUP BY workflow_session_id
  ),
  trace_by_ids AS (
    SELECT
      stream_id,
      request_id,
      max(toUInt8(trace_type = 'orchestrator_swap')) AS has_swap_trace
    FROM livepeer_analytics.stream_trace_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
      AND stream_id != ''
      AND request_id != ''
    GROUP BY stream_id, request_id
  )
SELECT
  count() AS failed_rows,
  count() AS swapped_without_evidence,
  groupArray(failures.workflow_session_id) AS failing_workflow_session_ids,
  groupArray(concat(failures.stream_id, '|', failures.request_id)) AS failing_stream_request_pairs
FROM
(
  SELECT
    s.workflow_session_id AS workflow_session_id,
    s.stream_id AS stream_id,
    s.request_id AS request_id
  FROM swapped s
  LEFT JOIN seg USING (workflow_session_id)
  LEFT JOIN edge USING (workflow_session_id)
  LEFT JOIN trace_by_ids t
    ON t.stream_id = s.stream_id
   AND t.request_id = s.request_id
  WHERE ifNull(seg.orch_count, 0) <= 1
    AND ifNull(edge.has_swap_edge, 0) = 0
    AND ifNull(t.has_swap_trace, 0) = 0
) failures;

-- TEST: param_updates_reference_existing_session
-- Verifies every param update fact references a known workflow session in the same window.
-- Requirement: no orphaned `fact_workflow_param_updates` rows.
WITH session_ids AS
(
  SELECT DISTINCT workflow_session_id
  FROM livepeer_analytics.fact_workflow_sessions FINAL
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
)
SELECT
  count() AS failed_rows,
  count() AS orphan_param_updates
FROM livepeer_analytics.fact_workflow_param_updates pu
LEFT JOIN session_ids s USING (workflow_session_id)
WHERE pu.update_ts >= {from_ts:DateTime64(3)}
  AND pu.update_ts < {to_ts:DateTime64(3)}
  AND s.workflow_session_id IS NULL;

-- TEST: lifecycle_session_pipeline_model_compatible
-- Verifies lifecycle attribution keeps pipeline/model semantics separated when both are present.
-- Requirement: canonical contract uses `pipeline` as workflow type and `model_id` as model label.
-- Guardrail for semantic correctness:
-- when both fields are present, pipeline and model_id should not collapse to the same label.
WITH latest_sessions AS
(
  SELECT
    workflow_session_id,
    argMax(session_start_ts, version) AS session_start_ts,
    argMax(pipeline, version) AS pipeline,
    argMax(model_id, version) AS model_id,
    argMax(gpu_attribution_method, version) AS gpu_attribution_method
  FROM livepeer_analytics.fact_workflow_sessions
  GROUP BY workflow_session_id
)
SELECT
  toUInt64(countIf(
    pipeline != ''
    AND ifNull(model_id, '') != ''
    AND gpu_attribution_method != 'none'
    AND lowerUTF8(pipeline) = lowerUTF8(ifNull(model_id, ''))
  ) > 0) AS failed_rows,
  countIf(
    pipeline != ''
    AND ifNull(model_id, '') != ''
    AND gpu_attribution_method != 'none'
    AND lowerUTF8(pipeline) = lowerUTF8(ifNull(model_id, ''))
  ) AS collapsed_semantic_rows,
  countIf(gpu_attribution_method = 'none') AS unattributed_rows,
  count() AS rows_checked
FROM latest_sessions
WHERE session_start_ts >= {from_ts:DateTime64(3)}
  AND session_start_ts < {to_ts:DateTime64(3)};

-- TEST: session_to_segment_pipeline_model_consistency
-- Verifies session<->segment model identity is consistent for attributed sessions.
-- Requirement: segment facts inherit the same model identity as their parent session.
-- Note: `fact_workflow_session_segments` does not currently expose `pipeline`, so this
-- assertion enforces model consistency and reports comparable-row coverage.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(gpu_attribution_method, version) AS gpu_attribution_method
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_segments AS
  (
    SELECT
      workflow_session_id,
      segment_index,
      argMax(segment_start_ts, version) AS segment_start_ts,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_session_segments
    GROUP BY workflow_session_id, segment_index
  )
SELECT
  toUInt64(session_segment_model_mismatches > 0) AS failed_rows,
  session_segment_model_mismatches,
  rows_checked,
  unattributed_rows,
  comparable_rows,
  missing_segment_model_rows
FROM
(
  SELECT
    countIf(
      s.gpu_attribution_method != 'none'
      AND s.model_id != ''
      AND seg.model_id != ''
      AND lowerUTF8(seg.model_id) != lowerUTF8(s.model_id)
    ) AS session_segment_model_mismatches,
    count() AS rows_checked,
    countIf(s.gpu_attribution_method = 'none') AS unattributed_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND s.model_id != ''
      AND seg.model_id != ''
    ) AS comparable_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND s.model_id != ''
      AND seg.model_id = ''
    ) AS missing_segment_model_rows
  FROM latest_segments seg
  INNER JOIN latest_sessions s USING (workflow_session_id)
  WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
    AND s.session_start_ts < {to_ts:DateTime64(3)}
    AND seg.segment_start_ts >= {from_ts:DateTime64(3)}
    AND seg.segment_start_ts < {to_ts:DateTime64(3)}
);

-- TEST: session_to_latency_pipeline_model_consistency
-- Verifies latency facts align with session pipeline/model identity for attributed sessions.
-- Requirement: latency lifecycle snapshots preserve canonical workflow+model semantics.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(gpu_attribution_method, version) AS gpu_attribution_method
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_latency AS
  (
    SELECT
      workflow_session_id,
      argMax(sample_ts, version) AS sample_ts,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_latency_samples
    GROUP BY workflow_session_id
  )
SELECT
  toUInt64(pipeline_mismatch_rows > 0 OR model_mismatch_rows > 0) AS failed_rows,
  pipeline_mismatch_rows,
  model_mismatch_rows,
  rows_checked,
  comparable_pipeline_rows,
  comparable_model_rows,
  missing_latency_rows
FROM
(
  SELECT
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id != ''
      AND s.pipeline != ''
      AND l.pipeline != ''
      AND lowerUTF8(l.pipeline) != lowerUTF8(s.pipeline)
    ) AS pipeline_mismatch_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id != ''
      AND s.model_id != ''
      AND l.model_id != ''
      AND lowerUTF8(l.model_id) != lowerUTF8(s.model_id)
    ) AS model_mismatch_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id != ''
    ) AS rows_checked,
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id != ''
      AND s.pipeline != ''
      AND l.pipeline != ''
    ) AS comparable_pipeline_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id != ''
      AND s.model_id != ''
      AND l.model_id != ''
    ) AS comparable_model_rows,
    countIf(
      s.gpu_attribution_method != 'none'
      AND l.workflow_session_id = ''
    ) AS missing_latency_rows
  FROM latest_sessions s
  LEFT JOIN latest_latency l USING (workflow_session_id)
  WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
    AND s.session_start_ts < {to_ts:DateTime64(3)}
    AND (l.sample_ts >= {from_ts:DateTime64(3)} AND l.sample_ts < {to_ts:DateTime64(3)} OR l.workflow_session_id = '')
);

-- TEST: session_to_status_projection_consistency
-- Verifies status samples keep canonical session pipeline/model identity and preserve
-- non-empty canonical pipeline for attributed sessions.
-- Requirement: silver status projection remains semantically aligned with lifecycle session facts.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(gpu_attribution_method, version) AS gpu_attribution_method
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  status_rows AS
  (
    SELECT
      workflow_session_id,
      pipeline,
      ifNull(model_id, '') AS model_id,
      sample_ts
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
  ),
  status_rows_resolved AS
  (
    SELECT
      st.workflow_session_id,
      st.sample_ts,
      st.model_id AS status_model_id,
      st.pipeline AS status_pipeline,
      s.pipeline AS session_pipeline,
      s.model_id AS session_model_id,
      s.gpu_attribution_method,
      if(
        st.pipeline != '',
        st.pipeline,
        if(
          st.model_id != ''
          AND s.model_id != ''
          AND lowerUTF8(st.model_id) = lowerUTF8(s.model_id),
          s.pipeline,
          ''
        )
      ) AS resolved_status_pipeline
    FROM status_rows st
    INNER JOIN latest_sessions s USING (workflow_session_id)
    WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
      AND s.session_start_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(pipeline_mismatch_rows > 0 OR model_mismatch_rows > 0 OR missing_status_pipeline_rows > 0) AS failed_rows,
  pipeline_mismatch_rows,
  model_mismatch_rows,
  missing_status_pipeline_rows,
  rows_checked,
  comparable_pipeline_rows,
  comparable_model_rows
FROM
(
  SELECT
    countIf(
      gpu_attribution_method != 'none'
      AND resolved_status_pipeline != ''
      AND session_pipeline != ''
      AND lowerUTF8(resolved_status_pipeline) != lowerUTF8(session_pipeline)
    ) AS pipeline_mismatch_rows,
    countIf(
      gpu_attribution_method != 'none'
      AND status_model_id != ''
      AND session_model_id != ''
      AND lowerUTF8(status_model_id) != lowerUTF8(session_model_id)
    ) AS model_mismatch_rows,
    countIf(
      gpu_attribution_method != 'none'
      AND session_pipeline != ''
      AND resolved_status_pipeline = ''
    ) AS missing_status_pipeline_rows,
    count() AS rows_checked,
    countIf(
      gpu_attribution_method != 'none'
      AND resolved_status_pipeline != ''
      AND session_pipeline != ''
    ) AS comparable_pipeline_rows,
    countIf(
      gpu_attribution_method != 'none'
      AND status_model_id != ''
      AND session_model_id != ''
    ) AS comparable_model_rows
  FROM status_rows_resolved
);

-- TEST: trace_edge_pipeline_model_coverage
-- Verifies attributed sessions have trace-edge coverage with non-empty pipeline/model.
-- Requirement: trace-edge facts must provide at least one joinable pipeline+model edge
-- per attributed session in-window.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gpu_attribution_method, version) AS gpu_attribution_method,
      argMax(pipeline, version) AS session_pipeline,
      ifNull(argMax(model_id, version), '') AS session_model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  attributed_sessions AS
  (
    SELECT workflow_session_id
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND gpu_attribution_method != 'none'
      AND session_pipeline != ''
      AND session_model_id != ''
  ),
  trace_rows AS
  (
    SELECT
      workflow_session_id,
      pipeline,
      ifNull(model_id, '') AS model_id,
      edge_ts
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
  ),
  coverage AS
  (
    SELECT
      s.workflow_session_id AS workflow_session_id,
      max(toUInt8(t.pipeline != '' AND t.model_id != '')) AS has_joinable_edge
    FROM attributed_sessions s
    LEFT JOIN trace_rows t USING (workflow_session_id)
    GROUP BY s.workflow_session_id
  )
SELECT
  toUInt64(if(attributed_sessions_checked = 0, 0, uncovered_sessions > 0)) AS failed_rows,
  uncovered_sessions,
  attributed_sessions_checked
FROM
(
  SELECT
    countIf(has_joinable_edge = 0) AS uncovered_sessions,
    count() AS attributed_sessions_checked
  FROM coverage
);

-- TEST: latest_sessions_vs_segment_session_ids
-- Verifies distinct session ids in segments do not exceed latest session ids.
-- Requirement: stateful session/segment identity remains coherent.
WITH
  sessions_latest AS
  (
    SELECT count() AS value
    FROM
    (
      SELECT workflow_session_id
      FROM
      (
        SELECT
          workflow_session_id,
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
  ),
  segment_session_ids AS
  (
    SELECT uniqExact(workflow_session_id) AS value
    FROM livepeer_analytics.fact_workflow_session_segments FINAL
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(segment_session_ids > sessions_latest) AS failed_rows,
  sessions_latest,
  segment_session_ids,
  sessions_latest - segment_session_ids AS delta,
  multiIf(
    segment_session_ids > sessions_latest, 'FAIL',
    sessions_latest - segment_session_ids <= 1, 'PASS',
    'WARN'
  ) AS status
FROM
(
  SELECT
    (SELECT value FROM sessions_latest) AS sessions_latest,
    (SELECT value FROM segment_session_ids) AS segment_session_ids
);

-- TEST: raw_session_rows_vs_latest_sessions
-- Verifies raw session rows are never fewer than latest-per-session rows.
-- Requirement: versioned session storage keeps latest rows derivable from raw rows.
WITH
  sessions_latest AS
  (
    SELECT count() AS value
    FROM
    (
      SELECT workflow_session_id
      FROM
      (
        SELECT
          workflow_session_id,
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
  ),
  sessions_raw AS
  (
    SELECT count() AS value
    FROM livepeer_analytics.fact_workflow_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(sessions_raw < sessions_latest) AS failed_rows,
  sessions_raw,
  sessions_latest,
  sessions_raw - sessions_latest AS delta,
  multiIf(
    sessions_raw < sessions_latest, 'FAIL',
    sessions_raw = sessions_latest, 'PASS',
    'INFO'
  ) AS status
FROM
(
  SELECT
    (SELECT value FROM sessions_raw) AS sessions_raw,
    (SELECT value FROM sessions_latest) AS sessions_latest
);

-- TEST: segment_rows_vs_segment_session_ids
-- Verifies segment row count is never smaller than distinct segment session ids.
-- Requirement: segment facts preserve at least one row per segmented session id.
WITH
  segment_session_ids AS
  (
    SELECT uniqExact(workflow_session_id) AS value
    FROM livepeer_analytics.fact_workflow_session_segments FINAL
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
  ),
  segment_rows AS
  (
    SELECT count() AS value
    FROM livepeer_analytics.fact_workflow_session_segments
    WHERE segment_start_ts >= {from_ts:DateTime64(3)}
      AND segment_start_ts < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(segment_rows < segment_session_ids) AS failed_rows,
  segment_rows,
  segment_session_ids,
  segment_rows - segment_session_ids AS delta,
  multiIf(segment_rows < segment_session_ids, 'FAIL', 'INFO') AS status
FROM
(
  SELECT
    (SELECT value FROM segment_rows) AS segment_rows,
    (SELECT value FROM segment_session_ids) AS segment_session_ids
);

-- TEST: mixed_known_stream_versions
-- Verifies known_stream version transitions are non-regressive.
-- Requirement: mixed versions are tolerated, but 1->0 regressions should not occur.
WITH per_session AS
(
  SELECT
    workflow_session_id,
    uniqExact(known_stream) AS known_stream_values,
    argMin(known_stream, version) AS earliest_known_stream,
    argMax(known_stream, version) AS latest_known_stream
  FROM livepeer_analytics.fact_workflow_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
  GROUP BY workflow_session_id
)
SELECT
  toUInt64(regressive_sessions > 0) AS failed_rows,
  sessions_with_known_stream_transitions,
  regressive_sessions,
  multiIf(
    regressive_sessions > 0, 'FAIL',
    sessions_with_known_stream_transitions > 0, 'WARN',
    'PASS'
  ) AS status
FROM
(
  SELECT
    countIf(known_stream_values > 1) AS sessions_with_known_stream_transitions,
    countIf(
      known_stream_values > 1
      AND earliest_known_stream = 1
      AND latest_known_stream = 0
    ) AS regressive_sessions
  FROM per_session
);

-- TEST: agg_stream_performance_1m_matches_status_samples
-- Verifies 1-minute performance rollup table matches recomputation from silver status samples.
-- Requirement: rollup lineage from silver facts to aggregate states is lossless and numerically consistent.
WITH
  expected AS
  (
    SELECT
      toStartOfInterval(sample_ts, INTERVAL 1 MINUTE) AS window_start,
      gateway,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      uniqExact(workflow_session_id) AS expected_sessions,
      uniqExact(stream_id) AS expected_streams,
      avg(output_fps) AS expected_avg_output_fps,
      count() AS expected_samples
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  rollup AS
  (
    SELECT
      window_start,
      gateway,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      uniqExactMerge(sessions_uniq_state) AS rollup_sessions,
      uniqExactMerge(streams_uniq_state) AS rollup_streams,
      avgMerge(output_fps_avg_state) AS rollup_avg_output_fps,
      countMerge(sample_count_state) AS rollup_samples
    FROM livepeer_analytics.agg_stream_performance_1m
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      sum(abs(toInt64(e.expected_sessions) - toInt64(r.rollup_sessions))) AS total_diff_sessions,
      sum(abs(toInt64(e.expected_streams) - toInt64(r.rollup_streams))) AS total_diff_streams,
      sum(abs(toInt64(e.expected_samples) - toInt64(r.rollup_samples))) AS total_diff_samples,
      max(abs(e.expected_avg_output_fps - r.rollup_avg_output_fps)) AS max_abs_diff_avg_output_fps
    FROM expected e
    INNER JOIN rollup r
      USING (window_start, gateway, orchestrator_address, pipeline, model_id, gpu_id, region)
  ),
  coverage AS
  (
    SELECT
      (SELECT count() FROM expected) AS expected_rows,
      (SELECT count() FROM rollup) AS rollup_rows,
      (SELECT joined_rows FROM joined) AS joined_rows,
      (
        SELECT count()
        FROM expected e
        LEFT JOIN rollup r
          USING (window_start, gateway, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE r.window_start IS NULL
      ) AS expected_only_keys,
      (
        SELECT count()
        FROM rollup r
        LEFT JOIN expected e
          USING (window_start, gateway, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE e.window_start IS NULL
      ) AS rollup_only_keys,
      (SELECT total_diff_sessions FROM joined) AS total_diff_sessions,
      (SELECT total_diff_streams FROM joined) AS total_diff_streams,
      (SELECT total_diff_samples FROM joined) AS total_diff_samples,
      (SELECT max_abs_diff_avg_output_fps FROM joined) AS max_abs_diff_avg_output_fps
  )
SELECT
  toUInt64(
    (expected_rows > 0 AND rollup_rows > 0 AND joined_rows = 0)
    OR expected_only_keys > 0
    OR rollup_only_keys > 0
    OR ifNull(total_diff_sessions, 0) > 0
    OR ifNull(total_diff_streams, 0) > 0
    OR ifNull(total_diff_samples, 0) > 0
    OR ifNull(max_abs_diff_avg_output_fps, 0) > 0.000001
  ) AS failed_rows,
  multiIf(
    expected_rows = 0 AND rollup_rows = 0, 'EMPTY_BOTH',
    expected_rows > 0 AND rollup_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    expected_only_keys > 0 OR rollup_only_keys > 0, 'KEY_MISMATCH',
    ifNull(total_diff_sessions, 0) > 0
      OR ifNull(total_diff_streams, 0) > 0
      OR ifNull(total_diff_samples, 0) > 0
      OR ifNull(max_abs_diff_avg_output_fps, 0) > 0.000001, 'VALUE_MISMATCH',
    'PASS'
  ) AS failure_mode,
  expected_rows,
  rollup_rows,
  joined_rows,
  expected_only_keys,
  rollup_only_keys,
  total_diff_sessions,
  total_diff_streams,
  total_diff_samples,
  max_abs_diff_avg_output_fps
FROM coverage;

-- TEST: gpu_view_covers_healthy_attributed_session_keys
-- Verifies healthy attributed session keys are represented in the GPU API view.
-- Requirement: gold GPU serving should cover successful attributable sessions.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id,
      ifNull(argMax(region, version), '') AS region,
      argMax(startup_success, version) AS startup_success
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  eligible_session_keys AS
  (
    SELECT DISTINCT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      model_id,
      gpu_id,
      region
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND startup_success = 1
      AND orchestrator_address != ''
      AND pipeline != ''
      AND gpu_id != ''
  ),
  gpu_view_keys AS
  (
    SELECT DISTINCT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(eligible_session_keys > 0 AND missing_gpu_keys > 0) AS failed_rows,
  multiIf(
    eligible_session_keys = 0, 'INFO',
    missing_gpu_keys = 0, 'PASS',
    'FAIL'
  ) AS status,
  eligible_session_keys,
  gpu_view_keys,
  missing_gpu_keys,
  missing_key_examples
FROM
(
  SELECT
    count() AS eligible_session_keys,
    (SELECT count() FROM gpu_view_keys) AS gpu_view_keys,
    countIf(v.window_start IS NULL) AS missing_gpu_keys,
    groupArrayIf(
      concat(
        toString(s.window_start), '|', s.orchestrator_address, '|', s.pipeline, '|', s.model_id, '|', s.gpu_id, '|', s.region
      ),
      v.window_start IS NULL
    ) AS missing_key_examples
  FROM eligible_session_keys s
  LEFT JOIN gpu_view_keys v
    USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
);

-- TEST: demand_has_rows_for_all_session_hours
-- Verifies each hour with sessions is represented in network demand view.
-- Requirement: gold demand serving should cover all active session hours.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  session_hours AS
  (
    SELECT DISTINCT toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS session_hour
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
  ),
  demand_hours AS
  (
    SELECT DISTINCT window_start AS demand_hour
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(session_hours > 0 AND missing_demand_hours > 0) AS failed_rows,
  multiIf(
    session_hours = 0, 'INFO',
    missing_demand_hours = 0, 'PASS',
    'FAIL'
  ) AS status,
  session_hours,
  demand_hours,
  missing_demand_hours,
  missing_hours
FROM
(
  SELECT
    count() AS session_hours,
    (SELECT count() FROM demand_hours) AS demand_hours,
    countIf(d.demand_hour IS NULL) AS missing_demand_hours,
    groupArrayIf(toString(s.session_hour), d.demand_hour IS NULL) AS missing_hours
  FROM session_hours s
  LEFT JOIN demand_hours d
    ON d.demand_hour = s.session_hour
);

-- TEST: gpu_count_delta_explained_by_key_overlap
-- Verifies GPU view-vs-rollup row delta is fully explained by key-overlap delta.
-- Requirement: non-empty key skew must be explainable by key coverage, not silent value drift.
WITH
  rollup AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region
    FROM livepeer_analytics.agg_stream_performance_1m
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  counts AS
  (
    SELECT
      (SELECT count() FROM rollup) AS rollup_rows,
      (SELECT count() FROM api) AS view_rows,
      (
        SELECT count()
        FROM rollup r
        LEFT JOIN api a
          USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE a.api_marker IS NULL
      ) AS rollup_only_keys,
      (
        SELECT count()
        FROM api a
        LEFT JOIN rollup r
          USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE r.rollup_marker IS NULL
      ) AS view_only_keys
  )
SELECT
  toUInt64((view_rows - rollup_rows) != (view_only_keys - rollup_only_keys)) AS failed_rows,
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'INFO',
    (view_rows - rollup_rows) = (view_only_keys - rollup_only_keys), 'PASS',
    'FAIL'
  ) AS status,
  rollup_rows,
  view_rows,
  rollup_only_keys,
  view_only_keys,
  view_rows - rollup_rows AS row_delta,
  view_only_keys - rollup_only_keys AS overlap_delta
FROM counts;

-- TEST: network_demand_counts_aligned_to_rollup
-- Verifies demand view has strict key/count alignment with recomputed rollup+demand keys.
-- Requirement: expected and served keyspaces must match at network demand grain.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  perf_pipeline_fallback AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      model_id,
      if(countDistinctIf(pipeline, pipeline != '') = 1, anyIf(pipeline, pipeline != ''), '') AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, model_id
  ),
  perf_1h AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(p.window_start, INTERVAL 1 HOUR) AS window_start,
      p.gateway,
      ifNull(p.region, '') AS region,
      if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
      ifNull(p.model_id, '') AS model_id
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE p.window_start >= {from_ts:DateTime64(3)}
      AND p.window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline, model_id
  ),
  demand_1h AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      pipeline,
      model_id
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  expected AS
  (
    SELECT
      p.rollup_marker AS expected_marker,
      p.window_start,
      p.gateway,
      p.region,
      p.pipeline,
      p.model_id
    FROM perf_1h p
    LEFT JOIN demand_1h d
      USING (window_start, gateway, region, pipeline, model_id)
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    if(
      expected_rows = 0 AND view_rows = 0,
      0,
      expected_rows != view_rows OR rollup_only_keys > 0 OR view_only_keys > 0
    )
  ) AS failed_rows,
  multiIf(
    expected_rows = 0 AND view_rows = 0, 'INFO',
    expected_rows = view_rows AND rollup_only_keys = 0 AND view_only_keys = 0, 'PASS',
    'FAIL'
  ) AS status,
  expected_rows AS rollup_rows,
  view_rows,
  rollup_only_keys,
  view_only_keys
FROM
(
  SELECT
    (SELECT count() FROM expected) AS expected_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM expected e
      LEFT JOIN api a
        USING (window_start, gateway, region, pipeline, model_id)
      WHERE a.api_marker IS NULL
    ) AS rollup_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN expected e
        USING (window_start, gateway, region, pipeline, model_id)
      WHERE e.expected_marker IS NULL
    ) AS view_only_keys
);

-- TEST: sla_counts_aligned_to_raw_latest_sessions
-- Verifies SLA view has strict key/count alignment with latest-session recompute
-- for attributed sessions (`orchestrator_address != ''`).
-- Requirement: expected and served keyspaces must match at attributed SLA grain.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id,
      ifNull(argMax(region, version), '') AS region
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  raw AS
  (
  SELECT
      toNullable(1) AS raw_marker,
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      model_id AS model_id_key,
      gpu_id AS gpu_id_key,
      region AS region_key
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND orchestrator_address != ''
    GROUP BY window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    if(
      raw_rows = 0 AND view_rows = 0,
      0,
      raw_rows != view_rows OR raw_only_keys > 0 OR view_only_keys > 0
    )
  ) AS failed_rows,
  multiIf(
    raw_rows = 0 AND view_rows = 0, 'INFO',
    raw_rows = view_rows AND raw_only_keys = 0 AND view_only_keys = 0, 'PASS',
    'FAIL'
  ) AS status,
  raw_rows,
  view_rows,
  raw_only_keys,
  view_only_keys
FROM
(
  SELECT
    (SELECT count() FROM raw) AS raw_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM raw r
      LEFT JOIN api a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE a.api_marker IS NULL
    ) AS raw_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN raw r
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE r.raw_marker IS NULL
    ) AS view_only_keys
);

-- TEST: view_count_grain_ordering
-- Informational check: compares serving-view row counts as a coarse grain sanity signal.
SELECT
  toUInt64(0) AS failed_rows,
  multiIf(
    demand_rows = 0 AND gpu_rows = 0 AND sla_rows = 0, 'INFO',
    demand_rows <= greatest(gpu_rows, sla_rows), 'PASS',
    'WARN'
  ) AS status,
  demand_rows,
  gpu_rows,
  sla_rows
FROM
(
  SELECT
    (
      SELECT count()
      FROM livepeer_analytics.v_api_network_demand
      WHERE window_start >= {from_ts:DateTime64(3)}
        AND window_start < {to_ts:DateTime64(3)}
    ) AS demand_rows,
    (
      SELECT count()
      FROM livepeer_analytics.v_api_gpu_metrics
      WHERE window_start >= {from_ts:DateTime64(3)}
        AND window_start < {to_ts:DateTime64(3)}
    ) AS gpu_rows,
    (
      SELECT count()
      FROM livepeer_analytics.v_api_sla_compliance
      WHERE window_start >= {from_ts:DateTime64(3)}
        AND window_start < {to_ts:DateTime64(3)}
    ) AS sla_rows
);

-- TEST: gpu_view_matches_rollup
-- Verifies `v_api_gpu_metrics` matches underlying rollup aggregates at the same dimensional grain.
-- Requirement: serving view must be numerically consistent with rollup source tables.
WITH
  rollup AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      avgMerge(output_fps_avg_state) AS avg_output_fps
    FROM livepeer_analytics.agg_stream_performance_1m
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      ifNull(region, '') AS region,
      avg_output_fps
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      avg(abs(r.avg_output_fps - a.avg_output_fps)) AS mean_abs_diff_fps,
      max(abs(r.avg_output_fps - a.avg_output_fps)) AS max_abs_diff_fps
    FROM rollup r
    INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
  ),
  coverage AS
  (
    SELECT
      (SELECT count() FROM rollup) AS rollup_rows,
      (SELECT count() FROM api) AS view_rows,
      (SELECT joined_rows FROM joined) AS joined_rows,
      (
        SELECT count()
        FROM rollup r
        LEFT JOIN api a
          USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE a.api_marker IS NULL
      ) AS rollup_only_keys,
      (
        SELECT count()
        FROM api a
        LEFT JOIN rollup r
          USING (window_start, orchestrator_address, pipeline, model_id, gpu_id, region)
        WHERE r.rollup_marker IS NULL
      ) AS view_only_keys,
      (
        SELECT countIf(orchestrator_address = '')
        FROM rollup
      ) AS rollup_empty_orch_rows,
      (
        SELECT countIf(gpu_id = '')
        FROM rollup
      ) AS rollup_empty_gpu_rows,
      (
        SELECT countIf(region = '')
        FROM rollup
      ) AS rollup_empty_region_rows,
      (
        SELECT countIf(orchestrator_address = '')
        FROM api
      ) AS view_empty_orch_rows,
      (
        SELECT countIf(gpu_id = '')
        FROM api
      ) AS view_empty_gpu_rows,
      (
        SELECT countIf(region = '')
        FROM api
      ) AS view_empty_region_rows,
      (SELECT mean_abs_diff_fps FROM joined) AS mean_abs_diff_fps,
      (SELECT max_abs_diff_fps FROM joined) AS max_abs_diff_fps
  )
SELECT
  toUInt64(
    (rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0)
    OR (joined_rows > 0 AND ifNull(max_abs_diff_fps, 0) > 0.000001)
  ) AS failed_rows,
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND ifNull(max_abs_diff_fps, 0) > 0.000001, 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  rollup_rows,
  view_rows,
  joined_rows,
  rollup_only_keys,
  view_only_keys,
  rollup_empty_orch_rows,
  rollup_empty_gpu_rows,
  rollup_empty_region_rows,
  view_empty_orch_rows,
  view_empty_gpu_rows,
  view_empty_region_rows,
  mean_abs_diff_fps,
  max_abs_diff_fps
FROM coverage;

-- TEST: network_demand_view_matches_rollup
-- Verifies `v_api_network_demand` matches independently recomputed hourly demand/perf aggregates.
-- Requirement: serving view must remain consistent with rollup and latest-session demand semantics.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  perf_pipeline_fallback AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      model_id,
      if(countDistinctIf(pipeline, pipeline != '') = 1, anyIf(pipeline, pipeline != ''), '') AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, model_id
  ),
  perf_1h AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(p.window_start, INTERVAL 1 HOUR) AS window_start,
      p.gateway,
      ifNull(p.region, '') AS region,
      if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
      ifNull(p.model_id, '') AS model_id,
      uniqExactMerge(p.sessions_uniq_state) AS total_sessions,
      uniqExactMerge(p.streams_uniq_state) AS total_streams,
      countMerge(p.sample_count_state) / 60.0 AS total_inference_minutes,
      avgMerge(p.output_fps_avg_state) AS avg_output_fps
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE p.window_start >= {from_ts:DateTime64(3)}
      AND p.window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline, model_id
  ),
  demand_1h AS
  (
    SELECT
      toNullable(1) AS demand_marker,
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      pipeline,
      model_id,
      sum(toUInt64(known_stream)) AS known_sessions,
      sum(toUInt64(known_stream AND orchestrator_address != '')) AS served_sessions,
      sum(toUInt64(known_stream AND orchestrator_address = '')) AS unserved_sessions,
      sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  expected AS
  (
    SELECT
      p.rollup_marker AS expected_marker,
      p.window_start,
      p.gateway,
      p.region,
      p.pipeline,
      p.model_id,
      p.total_sessions,
      p.total_streams,
      p.total_inference_minutes,
      p.avg_output_fps,
      ifNull(d.known_sessions, toUInt64(0)) AS known_sessions,
      ifNull(d.served_sessions, toUInt64(0)) AS served_sessions,
      ifNull(d.unserved_sessions, toUInt64(0)) AS unserved_sessions,
      ifNull(d.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
      ifNull(d.swapped_sessions, toUInt64(0)) AS swapped_sessions
    FROM perf_1h p
    LEFT JOIN demand_1h d
      USING (window_start, gateway, region, pipeline, model_id)
  ),
  api AS
  (
    SELECT
      toNullable(1) AS api_marker,
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id,
      total_sessions,
      total_streams,
      total_inference_minutes,
      avg_output_fps,
      known_sessions,
      served_sessions,
      unserved_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      avg(abs(e.avg_output_fps - a.avg_output_fps)) AS mean_abs_diff_fps,
      max(abs(e.avg_output_fps - a.avg_output_fps)) AS max_abs_diff_fps,
      avg(abs(e.total_inference_minutes - a.total_inference_minutes)) AS mean_abs_diff_minutes,
      max(abs(e.total_inference_minutes - a.total_inference_minutes)) AS max_abs_diff_minutes,
      sum(abs(toInt64(e.total_sessions) - toInt64(a.total_sessions))) AS total_diff_sessions,
      sum(abs(toInt64(e.total_streams) - toInt64(a.total_streams))) AS total_diff_streams,
      sum(abs(toInt64(e.known_sessions) - toInt64(a.known_sessions))) AS total_diff_known_sessions,
      sum(abs(toInt64(e.served_sessions) - toInt64(a.served_sessions))) AS total_diff_served_sessions,
      sum(abs(toInt64(e.unserved_sessions) - toInt64(a.unserved_sessions))) AS total_diff_unserved_sessions,
      sum(abs(toInt64(e.unexcused_sessions) - toInt64(a.unexcused_sessions))) AS total_diff_unexcused_sessions,
      sum(abs(toInt64(e.swapped_sessions) - toInt64(a.swapped_sessions))) AS total_diff_swapped_sessions
    FROM expected e
    INNER JOIN api a
      USING (window_start, gateway, region, pipeline, model_id)
  ),
  coverage AS
  (
    SELECT
      (SELECT count() FROM expected) AS rollup_rows,
      (SELECT count() FROM api) AS view_rows,
      (SELECT joined_rows FROM joined) AS joined_rows,
      (
        SELECT count()
        FROM expected e
        LEFT JOIN api a
          USING (window_start, gateway, region, pipeline, model_id)
        WHERE a.api_marker IS NULL
      ) AS rollup_only_keys,
      (
        SELECT count()
        FROM api a
        LEFT JOIN expected e
          USING (window_start, gateway, region, pipeline, model_id)
        WHERE e.expected_marker IS NULL
      ) AS view_only_keys,
      (SELECT countIf(gateway = '') FROM expected) AS rollup_empty_gateway_rows,
      (SELECT countIf(region = '') FROM expected) AS rollup_empty_region_rows,
      (SELECT countIf(pipeline = '') FROM expected) AS rollup_empty_pipeline_rows,
      (SELECT countIf(model_id = '') FROM expected) AS rollup_empty_model_rows,
      (SELECT countIf(gateway = '') FROM api) AS view_empty_gateway_rows,
      (SELECT countIf(region = '') FROM api) AS view_empty_region_rows,
      (SELECT countIf(pipeline = '') FROM api) AS view_empty_pipeline_rows,
      (SELECT countIf(model_id = '') FROM api) AS view_empty_model_rows,
      (SELECT mean_abs_diff_fps FROM joined) AS mean_abs_diff_fps,
      (SELECT max_abs_diff_fps FROM joined) AS max_abs_diff_fps,
      (SELECT mean_abs_diff_minutes FROM joined) AS mean_abs_diff_minutes,
      (SELECT max_abs_diff_minutes FROM joined) AS max_abs_diff_minutes,
      (SELECT total_diff_sessions FROM joined) AS total_diff_sessions,
      (SELECT total_diff_streams FROM joined) AS total_diff_streams,
      (SELECT total_diff_known_sessions FROM joined) AS total_diff_known_sessions,
      (SELECT total_diff_served_sessions FROM joined) AS total_diff_served_sessions,
      (SELECT total_diff_unserved_sessions FROM joined) AS total_diff_unserved_sessions,
      (SELECT total_diff_unexcused_sessions FROM joined) AS total_diff_unexcused_sessions,
      (SELECT total_diff_swapped_sessions FROM joined) AS total_diff_swapped_sessions
  )
SELECT
  toUInt64(
    (rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0)
    OR (joined_rows > 0 AND (
      ifNull(max_abs_diff_fps, 0) > 0.000001
      OR ifNull(max_abs_diff_minutes, 0) > 0.000001
      OR ifNull(total_diff_sessions, 0) > 0
      OR ifNull(total_diff_streams, 0) > 0
      OR ifNull(total_diff_known_sessions, 0) > 0
      OR ifNull(total_diff_served_sessions, 0) > 0
      OR ifNull(total_diff_unserved_sessions, 0) > 0
      OR ifNull(total_diff_unexcused_sessions, 0) > 0
      OR ifNull(total_diff_swapped_sessions, 0) > 0
    ))
  ) AS failed_rows,
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND (
      ifNull(max_abs_diff_fps, 0) > 0.000001
      OR ifNull(max_abs_diff_minutes, 0) > 0.000001
      OR ifNull(total_diff_sessions, 0) > 0
      OR ifNull(total_diff_streams, 0) > 0
      OR ifNull(total_diff_known_sessions, 0) > 0
      OR ifNull(total_diff_served_sessions, 0) > 0
      OR ifNull(total_diff_unserved_sessions, 0) > 0
      OR ifNull(total_diff_unexcused_sessions, 0) > 0
      OR ifNull(total_diff_swapped_sessions, 0) > 0
    ), 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  rollup_rows,
  view_rows,
  joined_rows,
  rollup_only_keys,
  view_only_keys,
  rollup_empty_gateway_rows,
  rollup_empty_region_rows,
  rollup_empty_pipeline_rows,
  rollup_empty_model_rows,
  view_empty_gateway_rows,
  view_empty_region_rows,
  view_empty_pipeline_rows,
  view_empty_model_rows,
  mean_abs_diff_fps,
  max_abs_diff_fps,
  mean_abs_diff_minutes,
  max_abs_diff_minutes,
  total_diff_sessions,
  total_diff_streams,
  total_diff_known_sessions,
  total_diff_served_sessions,
  total_diff_unserved_sessions,
  total_diff_unexcused_sessions,
  total_diff_swapped_sessions
FROM coverage;

-- TEST: network_demand_model_split_conservation
-- Verifies model-grain demand rows conserve totals when re-aggregated to pipeline grain.
-- Requirement: adding model_id to demand grain must not change pipeline-level totals.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  perf_pipeline_fallback AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      model_id,
      if(countDistinctIf(pipeline, pipeline != '') = 1, anyIf(pipeline, pipeline != ''), '') AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, model_id
  ),
  pipeline_baseline AS
  (
    SELECT
      toStartOfInterval(p.window_start, INTERVAL 1 HOUR) AS window_start,
      p.gateway,
      ifNull(p.region, '') AS region,
      if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
      uniqExactMerge(p.sessions_uniq_state) AS total_sessions,
      uniqExactMerge(p.streams_uniq_state) AS total_streams,
      countMerge(p.sample_count_state) / 60.0 AS total_inference_minutes
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE p.window_start >= {from_ts:DateTime64(3)}
      AND p.window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline
  ),
  api_reaggregated AS
  (
    SELECT
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      sum(total_sessions) AS total_sessions,
      sum(total_streams) AS total_streams,
      sum(total_inference_minutes) AS total_inference_minutes
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline
  ),
  joined AS
  (
    SELECT
      count() AS joined_rows,
      sum(abs(toInt64(b.total_sessions) - toInt64(a.total_sessions))) AS total_diff_sessions,
      sum(abs(toInt64(b.total_streams) - toInt64(a.total_streams))) AS total_diff_streams,
      max(abs(b.total_inference_minutes - a.total_inference_minutes)) AS max_abs_diff_minutes
    FROM pipeline_baseline b
    INNER JOIN api_reaggregated a
      USING (window_start, gateway, region, pipeline)
  )
SELECT
  toUInt64(
    (baseline_rows > 0 AND api_rows > 0 AND joined_rows = 0)
    OR baseline_only_keys > 0
    OR api_only_keys > 0
    OR ifNull(total_diff_sessions, 0) > 0
    OR ifNull(total_diff_streams, 0) > 0
    OR ifNull(max_abs_diff_minutes, 0) > 0.000001
  ) AS failed_rows,
  baseline_rows,
  api_rows,
  joined_rows,
  baseline_only_keys,
  api_only_keys,
  total_diff_sessions,
  total_diff_streams,
  max_abs_diff_minutes
FROM
(
  SELECT
    (SELECT count() FROM pipeline_baseline) AS baseline_rows,
    (SELECT count() FROM api_reaggregated) AS api_rows,
    (SELECT joined_rows FROM joined) AS joined_rows,
    (
      SELECT count()
      FROM pipeline_baseline b
      LEFT JOIN api_reaggregated a
        USING (window_start, gateway, region, pipeline)
      WHERE a.window_start IS NULL
    ) AS baseline_only_keys,
    (
      SELECT count()
      FROM api_reaggregated a
      LEFT JOIN pipeline_baseline b
        USING (window_start, gateway, region, pipeline)
      WHERE b.window_start IS NULL
    ) AS api_only_keys,
    (SELECT total_diff_sessions FROM joined) AS total_diff_sessions,
    (SELECT total_diff_streams FROM joined) AS total_diff_streams,
    (SELECT max_abs_diff_minutes FROM joined) AS max_abs_diff_minutes
);

-- TEST: network_demand_join_multiplicity_guard
-- Verifies expected and API keyspaces are unique at model-grain demand keys.
-- Requirement: parity joins must be 1:1 at (hour,gateway,region,pipeline,model_id).
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  perf_pipeline_fallback AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      model_id,
      if(countDistinctIf(pipeline, pipeline != '') = 1, anyIf(pipeline, pipeline != ''), '') AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, model_id
  ),
  perf_1h AS
  (
    SELECT
      toStartOfInterval(p.window_start, INTERVAL 1 HOUR) AS window_start,
      p.gateway,
      ifNull(p.region, '') AS region,
      if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
      ifNull(p.model_id, '') AS model_id
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE p.window_start >= {from_ts:DateTime64(3)}
      AND p.window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline, model_id
  ),
  demand_1h AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      region,
      pipeline,
      model_id
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  expected AS
  (
    SELECT
      p.window_start,
      p.gateway,
      p.region,
      p.pipeline,
      p.model_id
    FROM perf_1h p
    LEFT JOIN demand_1h d
      USING (window_start, gateway, region, pipeline, model_id)
  ),
  expected_keys AS
  (
    SELECT
      window_start,
      gateway,
      region,
      pipeline,
      model_id,
      count() AS key_rows
    FROM expected
    GROUP BY window_start, gateway, region, pipeline, model_id
  ),
  api_keys AS
  (
    SELECT
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id,
      count() AS key_rows
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, gateway, region, pipeline, model_id
  )
SELECT
  toUInt64(expected_nonunique_keys > 0 OR api_nonunique_keys > 0) AS failed_rows,
  expected_nonunique_keys,
  api_nonunique_keys
FROM
(
  SELECT
    (SELECT countIf(key_rows > 1) FROM expected_keys) AS expected_nonunique_keys,
    (SELECT countIf(key_rows > 1) FROM api_keys) AS api_nonunique_keys
);

-- TEST: network_demand_pipeline_join_inflation_guard
-- Verifies naive pipeline-only joins against model-grain demand inflate row counts.
-- Requirement: detect accidental join patterns that would duplicate metrics.
WITH
  pipeline_reference AS
  (
    SELECT
      toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      sum(total_sessions) AS total_sessions
    FROM
    (
      SELECT
        window_start,
        gateway,
        ifNull(region, '') AS region,
        pipeline,
        total_sessions
      FROM livepeer_analytics.v_api_network_demand
      WHERE window_start >= {from_ts:DateTime64(3)}
        AND window_start < {to_ts:DateTime64(3)}
    )
    GROUP BY window_start, gateway, region, pipeline
  ),
  model_grain AS
  (
    SELECT
      window_start,
      gateway,
      ifNull(region, '') AS region,
      pipeline,
      ifNull(model_id, '') AS model_id,
      total_sessions
    FROM livepeer_analytics.v_api_network_demand
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  ),
  naive_join AS
  (
    SELECT
      sum(toFloat64(r.total_sessions)) AS naive_sum
    FROM pipeline_reference r
    INNER JOIN model_grain m
      USING (window_start, gateway, region, pipeline)
  ),
  safe_total AS
  (
    SELECT
      sum(toFloat64(total_sessions)) AS safe_sum
    FROM pipeline_reference
  )
SELECT
  toUInt64(0) AS failed_rows,
  naive_sum,
  safe_sum,
  if(safe_sum = 0, 0.0, naive_sum / safe_sum) AS inflation_factor,
  multiIf(
    safe_sum = 0, 'INFO',
    naive_sum > safe_sum * 1.000001, 'WARN',
    'PASS'
  ) AS status
FROM naive_join
CROSS JOIN safe_total;

-- TEST: preexisting_grain_drift_baseline_gpu_sla
-- Verifies existing model-grain views conserve totals when collapsed to pipeline grain.
-- Requirement: detect pre-existing drift in model-aware views independent of demand changes.
WITH
  gpu_rollup_pipeline AS
  (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      sum(status_samples) AS expected_status_samples
    FROM
    (
      SELECT
        toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
        orchestrator_address,
        pipeline,
        ifNull(model_id, '') AS model_id,
        ifNull(gpu_id, '') AS gpu_id,
        ifNull(region, '') AS region,
        countMerge(sample_count_state) AS status_samples
      FROM livepeer_analytics.agg_stream_performance_1m
      WHERE window_start >= {from_ts:DateTime64(3)}
        AND window_start < {to_ts:DateTime64(3)}
      GROUP BY window_start, orchestrator_address, pipeline, model_id, gpu_id, region
    )
    GROUP BY window_start, orchestrator_address, pipeline
  ),
  gpu_view_pipeline AS
  (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      sum(status_samples) AS view_status_samples
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline
  ),
  sla_raw_pipeline AS
  (
    WITH latest_sessions AS
    (
      SELECT
        workflow_session_id,
        argMax(session_start_ts, version) AS session_start_ts,
        argMax(orchestrator_address, version) AS orchestrator_address,
        argMax(pipeline, version) AS pipeline,
        argMax(known_stream, version) AS known_stream,
        argMax(startup_unexcused, version) AS startup_unexcused,
        argMax(confirmed_swap_count, version) AS confirmed_swap_count,
        argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
      FROM livepeer_analytics.fact_workflow_sessions
      GROUP BY workflow_session_id
    )
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline
  ),
  sla_view_pipeline AS
  (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      sum(known_sessions) AS view_known_sessions,
      sum(unexcused_sessions) AS view_unexcused_sessions,
      sum(swapped_sessions) AS view_swapped_sessions
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline
  )
SELECT
  toUInt64(
    gpu_status_sample_diff > 0
    OR sla_known_diff > 0
    OR sla_unexcused_diff > 0
    OR sla_swapped_diff > 0
  ) AS failed_rows,
  gpu_status_sample_diff,
  sla_known_diff,
  sla_unexcused_diff,
  sla_swapped_diff
FROM
(
  SELECT
    ifNull(sum(abs(toInt64(r.expected_status_samples) - toInt64(v.view_status_samples))), 0) AS gpu_status_sample_diff
  FROM gpu_rollup_pipeline r
  INNER JOIN gpu_view_pipeline v
    USING (window_start, orchestrator_address, pipeline)
) gpu
CROSS JOIN
(
  SELECT
    ifNull(sum(abs(toInt64(r.raw_known_sessions) - toInt64(v.view_known_sessions))), 0) AS sla_known_diff,
    ifNull(sum(abs(toInt64(r.raw_unexcused_sessions) - toInt64(v.view_unexcused_sessions))), 0) AS sla_unexcused_diff,
    ifNull(sum(abs(toInt64(r.raw_swapped_sessions) - toInt64(v.view_swapped_sessions))), 0) AS sla_swapped_diff
  FROM sla_raw_pipeline r
  INNER JOIN sla_view_pipeline v
    USING (window_start, orchestrator_address, pipeline)
) sla;

-- TEST: sla_view_matches_session_fact
-- Verifies SLA API view counts match independently recomputed counts from latest session facts.
-- Requirement: `v_api_sla_compliance` must preserve known/unexcused/swapped semantics
-- for attributed sessions (`orchestrator_address != ''`).
WITH
  latest_sessions AS (
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
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  raw AS (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key,
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND orchestrator_address != ''
    GROUP BY window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
  ),
  api AS (
    SELECT
      window_start,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      ifNull(region, '') AS region_key,
      known_sessions,
      unexcused_sessions,
      swapped_sessions
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    (raw_rows > 0 AND view_rows > 0 AND joined_rows = 0)
    OR (joined_rows > 0 AND (total_known_diff > 0 OR total_unexcused_diff > 0 OR total_swapped_diff > 0))
  ) AS failed_rows,
  multiIf(
    raw_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    raw_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    joined_rows > 0 AND (total_known_diff > 0 OR total_unexcused_diff > 0 OR total_swapped_diff > 0), 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  raw_rows,
  view_rows,
  joined_rows,
  raw_only_keys,
  view_only_keys,
  total_known_diff,
  total_unexcused_diff,
  total_swapped_diff
FROM
(
  SELECT
    (SELECT count() FROM raw) AS raw_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM raw
      INNER JOIN api
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS joined_rows,
    (
      SELECT count()
      FROM raw r
      LEFT JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE a.window_start IS NULL
    ) AS raw_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN raw r
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE r.window_start IS NULL
    ) AS view_only_keys,
    (
      SELECT sum(abs(r.raw_known_sessions - a.known_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_known_diff,
    (
      SELECT sum(abs(r.raw_unexcused_sessions - a.unexcused_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_unexcused_diff,
    (
      SELECT sum(abs(r.raw_swapped_sessions - a.swapped_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_swapped_diff
);

-- TEST: sla_ratios_in_bounds
-- Verifies SLA ratio fields are bounded between 0 and 1.
-- Requirement: API-facing compliance ratios must remain mathematically valid.
SELECT
  countIf(success_ratio < 0 OR success_ratio > 1)
  + countIf(no_swap_ratio < 0 OR no_swap_ratio > 1) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};
