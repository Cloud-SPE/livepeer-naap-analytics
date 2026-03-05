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
  SELECT 'raw_streaming_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.raw_streaming_events
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'raw_ai_stream_status' AS object_name, count() AS rows_window
  FROM livepeer_analytics.raw_ai_stream_status
  WHERE event_timestamp >= {from_ts:DateTime64(3)}
    AND event_timestamp < {to_ts:DateTime64(3)}

  UNION ALL

  SELECT 'raw_stream_trace_events' AS object_name, count() AS rows_window
  FROM livepeer_analytics.raw_stream_trace_events
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
      (SELECT count() FROM livepeer_analytics.raw_network_capabilities WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS caps_rows,
      (SELECT count() FROM livepeer_analytics.raw_network_capabilities_advertised WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS adv_rows,
      (SELECT count() FROM livepeer_analytics.raw_network_capabilities_model_constraints WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS mc_rows,
      (SELECT count() FROM livepeer_analytics.raw_network_capabilities_prices WHERE event_timestamp >= {from_ts:DateTime64(3)} AND event_timestamp < {to_ts:DateTime64(3)}) AS price_rows
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
    FROM livepeer_analytics.raw_network_capabilities
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
  toUInt64(if(raw_rows = 0, 0, loss_rows > 0 OR expansion_rows > 0 OR missing_dim_keys > 0)) AS failed_rows,
  raw_rows,
  dim_rows,
  loss_rows,
  expansion_rows,
  missing_dim_keys,
  total_gpu_deficit,
  total_gpu_surplus
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
      SELECT count()
      FROM joined
      WHERE dim_gpu_count > raw_gpu_count
    ) AS expansion_rows,
    (
      SELECT countIf(missing_dim_key = 1)
      FROM joined
    ) AS missing_dim_keys,
    (
      SELECT ifNull(sum(greatest(toInt64(raw_gpu_count) - toInt64(dim_gpu_count), 0)), 0)
      FROM joined
    ) AS total_gpu_deficit,
    (
      SELECT ifNull(sum(greatest(toInt64(dim_gpu_count) - toInt64(raw_gpu_count), 0)), 0)
      FROM joined
    ) AS total_gpu_surplus
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
  FROM livepeer_analytics.raw_network_capabilities_prices
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
    FROM livepeer_analytics.raw_streaming_events
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
      FROM livepeer_analytics.raw_streaming_events_dlq
      WHERE event_type IN ('ai_stream_status', 'stream_trace', 'ai_stream_events', 'stream_ingest_metrics')
        AND ifNull(event_timestamp, source_record_timestamp) >= {from_ts:DateTime64(3)}
        AND ifNull(event_timestamp, source_record_timestamp) < {to_ts:DateTime64(3)}

      UNION ALL

      SELECT event_id
      FROM livepeer_analytics.raw_streaming_events_quarantine
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
    FROM livepeer_analytics.raw_streaming_events
    WHERE type = 'network_capabilities'
      AND event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
  ),
  typed AS
  (
    SELECT count() AS typed_rows
    FROM livepeer_analytics.raw_network_capabilities
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
-- Requirement: non-stateful status projection must preserve both coverage and per-UID multiplicity.
WITH
  typed AS
  (
    SELECT
      toString(lower(hex(SHA256(raw_json)))) AS source_event_uid,
      count() AS typed_rows_per_uid
    FROM livepeer_analytics.raw_ai_stream_status
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  silver AS
  (
    SELECT
      source_event_uid,
      count() AS silver_rows_per_uid
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  joined AS
  (
    SELECT
      t.source_event_uid,
      t.typed_rows_per_uid,
      ifNull(s.silver_rows_per_uid, toUInt64(0)) AS silver_rows_per_uid
    FROM typed t
    LEFT JOIN silver s USING (source_event_uid)
  )
SELECT
  toUInt64(
    if(
      typed_keys = 0 AND silver_keys = 0,
      0,
      missing_in_silver > 0 OR multiplicity_mismatch > 0 OR silver_only_keys > 0
    )
  ) AS failed_rows,
  typed_keys,
  silver_keys,
  missing_in_silver,
  multiplicity_mismatch,
  silver_only_keys,
  total_row_deficit,
  total_row_excess
FROM
(
  SELECT
    (SELECT count() FROM typed) AS typed_keys,
    (SELECT count() FROM silver) AS silver_keys,
    (SELECT countIf(silver_rows_per_uid = 0) FROM joined) AS missing_in_silver,
    (SELECT countIf(silver_rows_per_uid != typed_rows_per_uid) FROM joined) AS multiplicity_mismatch,
    (
      SELECT count()
      FROM silver s
      LEFT JOIN typed t USING (source_event_uid)
      WHERE t.source_event_uid IS NULL
    ) AS silver_only_keys,
    (
      SELECT ifNull(sum(greatest(toInt64(typed_rows_per_uid) - toInt64(silver_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_deficit,
    (
      SELECT ifNull(sum(greatest(toInt64(silver_rows_per_uid) - toInt64(typed_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_excess
);

-- TEST: trace_raw_to_silver_projection
-- Verifies each typed `stream_trace_events` row is projected into silver trace edges by source UID.
-- Requirement: non-stateful trace projection must preserve both coverage and per-UID multiplicity.
WITH
  typed AS
  (
    SELECT
      toString(lower(hex(SHA256(raw_json)))) AS source_event_uid,
      count() AS typed_rows_per_uid
    FROM livepeer_analytics.raw_stream_trace_events
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  silver AS
  (
    SELECT
      source_event_uid,
      count() AS silver_rows_per_uid
    FROM livepeer_analytics.fact_stream_trace_edges
    WHERE edge_ts >= {from_ts:DateTime64(3)}
      AND edge_ts < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  joined AS
  (
    SELECT
      t.source_event_uid,
      t.typed_rows_per_uid,
      ifNull(s.silver_rows_per_uid, toUInt64(0)) AS silver_rows_per_uid
    FROM typed t
    LEFT JOIN silver s USING (source_event_uid)
  )
SELECT
  toUInt64(
    if(
      typed_keys = 0 AND silver_keys = 0,
      0,
      missing_in_silver > 0 OR multiplicity_mismatch > 0 OR silver_only_keys > 0
    )
  ) AS failed_rows,
  typed_keys,
  silver_keys,
  missing_in_silver,
  multiplicity_mismatch,
  silver_only_keys,
  total_row_deficit,
  total_row_excess
FROM
(
  SELECT
    (SELECT count() FROM typed) AS typed_keys,
    (SELECT count() FROM silver) AS silver_keys,
    (SELECT countIf(silver_rows_per_uid = 0) FROM joined) AS missing_in_silver,
    (SELECT countIf(silver_rows_per_uid != typed_rows_per_uid) FROM joined) AS multiplicity_mismatch,
    (
      SELECT count()
      FROM silver s
      LEFT JOIN typed t USING (source_event_uid)
      WHERE t.source_event_uid IS NULL
    ) AS silver_only_keys,
    (
      SELECT ifNull(sum(greatest(toInt64(typed_rows_per_uid) - toInt64(silver_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_deficit,
    (
      SELECT ifNull(sum(greatest(toInt64(silver_rows_per_uid) - toInt64(typed_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_excess
);

-- TEST: ingest_raw_to_silver_projection
-- Verifies each typed `stream_ingest_metrics` row is projected into silver ingest samples by source UID.
-- Requirement: non-stateful ingest projection must preserve both coverage and per-UID multiplicity when ingest metrics are present.
WITH
  typed AS
  (
    SELECT
      toString(cityHash64(raw_json)) AS source_event_uid,
      count() AS typed_rows_per_uid
    FROM livepeer_analytics.raw_stream_ingest_metrics
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  silver AS
  (
    SELECT
      source_event_uid,
      count() AS silver_rows_per_uid
    FROM livepeer_analytics.fact_stream_ingest_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
    GROUP BY source_event_uid
  ),
  joined AS
  (
    SELECT
      t.source_event_uid,
      t.typed_rows_per_uid,
      ifNull(s.silver_rows_per_uid, toUInt64(0)) AS silver_rows_per_uid
    FROM typed t
    LEFT JOIN silver s USING (source_event_uid)
  )
SELECT
  toUInt64(
    if(
      typed_keys = 0 AND silver_keys = 0,
      0,
      missing_in_silver > 0 OR multiplicity_mismatch > 0 OR silver_only_keys > 0
    )
  ) AS failed_rows,
  typed_keys,
  silver_keys,
  missing_in_silver,
  multiplicity_mismatch,
  silver_only_keys,
  total_row_deficit,
  total_row_excess
FROM
(
  SELECT
    (SELECT count() FROM typed) AS typed_keys,
    (SELECT count() FROM silver) AS silver_keys,
    (SELECT countIf(silver_rows_per_uid = 0) FROM joined) AS missing_in_silver,
    (SELECT countIf(silver_rows_per_uid != typed_rows_per_uid) FROM joined) AS multiplicity_mismatch,
    (
      SELECT count()
      FROM silver s
      LEFT JOIN typed t USING (source_event_uid)
      WHERE t.source_event_uid IS NULL
    ) AS silver_only_keys,
    (
      SELECT ifNull(sum(greatest(toInt64(typed_rows_per_uid) - toInt64(silver_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_deficit,
    (
      SELECT ifNull(sum(greatest(toInt64(silver_rows_per_uid) - toInt64(typed_rows_per_uid), 0)), 0)
      FROM joined
    ) AS total_row_excess
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
-- Verifies split swap contract: compatibility alias `swap_count` must equal explicit `confirmed_swap_count`.
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
    FROM livepeer_analytics.raw_network_capabilities
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
-- Verifies sessions flagged as swapped under canonical semantics
-- (`confirmed_swap_count > 0` or `inferred_orchestrator_change_count > 0`) are backed by
-- fact-level evidence only: explicit swap edge or multi-orchestrator segment history.
WITH
  swapped AS (
    SELECT
      workflow_session_id
    FROM livepeer_analytics.fact_workflow_sessions FINAL
    WHERE session_start_ts >= {from_ts:DateTime64(3)}
      AND session_start_ts < {to_ts:DateTime64(3)}
      AND (
        confirmed_swap_count > 0
        OR inferred_orchestrator_change_count > 0
      )
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
  )
SELECT
  count() AS failed_rows,
  count() AS swapped_without_evidence,
  groupArray(failures.workflow_session_id) AS failing_workflow_session_ids
FROM
(
  SELECT
    s.workflow_session_id AS workflow_session_id
  FROM swapped s
  LEFT JOIN seg USING (workflow_session_id)
  LEFT JOIN edge USING (workflow_session_id)
  WHERE ifNull(seg.orch_count, 0) <= 1
    AND ifNull(edge.has_swap_edge, 0) = 0
) failures;

-- TEST: param_updates_reference_existing_session
-- Verifies every param update fact references a known workflow session id.
-- Requirement: no orphaned `fact_workflow_param_updates` rows.
WITH session_ids AS
(
  SELECT DISTINCT workflow_session_id
  FROM livepeer_analytics.fact_workflow_sessions FINAL
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
  toUInt64(
    comparable_pipeline_rows > 0
    AND comparable_model_rows > 0
    AND pipeline_mismatch_rows = comparable_pipeline_rows
    AND model_mismatch_rows = comparable_model_rows
  ) AS failed_rows,
  multiIf(
    comparable_pipeline_rows = 0 AND comparable_model_rows = 0, 'INFO',
    pipeline_mismatch_rows = 0 AND model_mismatch_rows = 0 AND missing_status_pipeline_rows = 0, 'PASS',
    pipeline_mismatch_rows = comparable_pipeline_rows AND model_mismatch_rows = comparable_model_rows, 'FAIL',
    'WARN'
  ) AS status,
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
      AND status_model_id != ''
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

-- TEST: status_vs_segment_identity_consistency
-- Verifies attributed status rows align to segment identity at sample timestamp.
-- Requirement: segment is canonical source for orchestrator/gpu/model/pipeline identity.
WITH
  latest_segments AS
  (
    SELECT
      workflow_session_id,
      segment_index,
      argMax(segment_start_ts, version) AS segment_start_ts,
      argMax(segment_end_ts, version) AS segment_end_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id
    FROM livepeer_analytics.fact_workflow_session_segments
    GROUP BY workflow_session_id, segment_index
  ),
  latest_segments_ordered AS
  (
    SELECT *
    FROM latest_segments
    ORDER BY workflow_session_id, segment_start_ts
  ),
  status_rows AS
  (
    SELECT
      workflow_session_id,
      sample_ts,
      orchestrator_address,
      pipeline,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id
    FROM livepeer_analytics.fact_stream_status_samples
    WHERE sample_ts >= {from_ts:DateTime64(3)}
      AND sample_ts < {to_ts:DateTime64(3)}
      AND is_attributed = 1
  )
SELECT
  toUInt64(rows_checked > 0 AND identity_mismatch_rows = rows_checked) AS failed_rows,
  multiIf(
    rows_checked = 0, 'INFO',
    identity_mismatch_rows = 0 AND missing_segment_match_rows = 0, 'PASS',
    identity_mismatch_rows = rows_checked, 'FAIL',
    'WARN'
  ) AS status,
  identity_mismatch_rows,
  missing_segment_match_rows,
  rows_checked
FROM
(
  SELECT
    countIf(
      seg.workflow_session_id != ''
      AND seg.segment_start_ts > toDateTime64(0, 3, 'UTC')
      AND (seg.segment_end_ts IS NULL OR st.sample_ts < seg.segment_end_ts)
      AND (
        lowerUTF8(st.orchestrator_address) != lowerUTF8(seg.orchestrator_address)
        OR (
          st.pipeline != ''
          AND seg.pipeline != ''
          AND lowerUTF8(st.pipeline) != lowerUTF8(seg.pipeline)
        )
        OR (st.model_id != '' AND seg.model_id != '' AND lowerUTF8(st.model_id) != lowerUTF8(seg.model_id))
        OR (st.gpu_id != '' AND seg.gpu_id != '' AND lowerUTF8(st.gpu_id) != lowerUTF8(seg.gpu_id))
      )
    ) AS identity_mismatch_rows,
    countIf(
      seg.workflow_session_id = ''
      OR seg.segment_start_ts <= toDateTime64(0, 3, 'UTC')
      OR (seg.segment_end_ts IS NOT NULL AND st.sample_ts >= seg.segment_end_ts)
    ) AS missing_segment_match_rows,
    count() AS rows_checked
  FROM status_rows st
  LEFT ASOF JOIN latest_segments_ordered seg
    ON seg.workflow_session_id = st.workflow_session_id
   AND seg.segment_start_ts <= st.sample_ts
);

-- TEST: latency_vs_segment_identity_consistency
-- Verifies latency sample identity fields align to segment identity at latency sample timestamp.
-- Requirement: latency identity must remain segment/session canonical.
WITH
  latest_segments AS
  (
    SELECT
      workflow_session_id,
      segment_index,
      argMax(segment_start_ts, version) AS segment_start_ts,
      argMax(segment_end_ts, version) AS segment_end_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id
    FROM livepeer_analytics.fact_workflow_session_segments
    GROUP BY workflow_session_id, segment_index
  ),
  latest_segments_ordered AS
  (
    SELECT *
    FROM latest_segments
    ORDER BY workflow_session_id, segment_start_ts
  ),
  latest_latency AS
  (
    SELECT
      workflow_session_id,
      argMax(sample_ts, version) AS sample_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id
    FROM livepeer_analytics.fact_workflow_latency_samples
    GROUP BY workflow_session_id
  )
SELECT
  toUInt64(rows_checked > 0 AND identity_mismatch_rows = rows_checked) AS failed_rows,
  multiIf(
    rows_checked = 0, 'INFO',
    identity_mismatch_rows = 0 AND missing_segment_match_rows = 0, 'PASS',
    identity_mismatch_rows = rows_checked, 'FAIL',
    'WARN'
  ) AS status,
  identity_mismatch_rows,
  missing_segment_match_rows,
  rows_checked
FROM
(
  SELECT
    countIf(
      seg.workflow_session_id != ''
      AND seg.segment_start_ts > toDateTime64(0, 3, 'UTC')
      AND (seg.segment_end_ts IS NULL OR lat.sample_ts < seg.segment_end_ts)
      AND (
        lowerUTF8(lat.orchestrator_address) != lowerUTF8(seg.orchestrator_address)
        OR (
          lat.pipeline != ''
          AND seg.pipeline != ''
          AND lowerUTF8(lat.pipeline) != lowerUTF8(seg.pipeline)
        )
        OR (lat.model_id != '' AND seg.model_id != '' AND lowerUTF8(lat.model_id) != lowerUTF8(seg.model_id))
        OR (lat.gpu_id != '' AND seg.gpu_id != '' AND lowerUTF8(lat.gpu_id) != lowerUTF8(seg.gpu_id))
      )
    ) AS identity_mismatch_rows,
    countIf(
      seg.workflow_session_id = ''
      OR seg.segment_start_ts <= toDateTime64(0, 3, 'UTC')
      OR (seg.segment_end_ts IS NOT NULL AND lat.sample_ts >= seg.segment_end_ts)
    ) AS missing_segment_match_rows,
    count() AS rows_checked
  FROM latest_latency lat
  LEFT ASOF JOIN latest_segments_ordered seg
    ON seg.workflow_session_id = lat.workflow_session_id
   AND seg.segment_start_ts <= lat.sample_ts
  WHERE lat.sample_ts >= {from_ts:DateTime64(3)}
    AND lat.sample_ts < {to_ts:DateTime64(3)}
);

-- TEST: gpu_view_no_perf_only_orphan_rows
-- Verifies GPU serving rows are not created from perf-only key drift.
-- Requirement: serving keyspace is segment-canonical; no status-only orphan rows.
SELECT
  toUInt64(0) AS failed_rows,
  multiIf(
    rows_checked = 0, 'INFO',
    orphan_rows > 0, 'WARN',
    'PASS'
  ) AS status,
  orphan_rows,
  rows_checked
FROM
(
  SELECT
    countIf(status_samples > 0 AND known_sessions = 0 AND valid_prompt_to_first_frame_count = 0 AND valid_startup_time_count = 0 AND valid_e2e_latency_count = 0) AS orphan_rows,
    count() AS rows_checked
  FROM livepeer_analytics.v_api_gpu_metrics
  WHERE window_start >= toStartOfHour({from_ts:DateTime64(3)})
    AND window_start < toStartOfHour({to_ts:DateTime64(3)}) + INTERVAL 1 HOUR
);

-- TEST: proxy_to_canonical_multiplicity_guard
-- Verifies proxy wallet mapping does not fan out to multiple canonical identities.
-- Requirement: proxy identity cannot be used as a unique canonical key.
WITH
  proxy_to_canonical AS
  (
    SELECT
      lowerUTF8(local_address) AS proxy_wallet,
      uniqExact(lowerUTF8(orchestrator_address)) AS canonical_count
    FROM livepeer_analytics.raw_network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
      AND local_address != ''
      AND orchestrator_address != ''
    GROUP BY proxy_wallet
  ),
  proxy_uri_to_canonical AS
  (
    SELECT
      lowerUTF8(local_address) AS proxy_wallet,
      lowerUTF8(ifNull(orch_uri, '')) AS orch_uri,
      uniqExact(lowerUTF8(orchestrator_address)) AS canonical_count
    FROM livepeer_analytics.raw_network_capabilities
    WHERE event_timestamp >= {from_ts:DateTime64(3)}
      AND event_timestamp < {to_ts:DateTime64(3)}
      AND local_address != ''
      AND orchestrator_address != ''
    GROUP BY proxy_wallet, orch_uri
  )
SELECT
  toUInt64(proxy_uri_multi_canonical_rows > 0) AS failed_rows,
  multiIf(
    proxy_uri_multi_canonical_rows > 0, 'FAIL',
    proxy_multi_canonical_rows > 0, 'WARN',
    'PASS'
  ) AS status,
  proxy_multi_canonical_rows,
  proxy_uri_multi_canonical_rows,
  proxies_checked,
  proxy_uri_keys_checked
FROM
(
  SELECT
    (SELECT countIf(canonical_count > 1) FROM proxy_to_canonical) AS proxy_multi_canonical_rows,
    (SELECT countIf(canonical_count > 1) FROM proxy_uri_to_canonical) AS proxy_uri_multi_canonical_rows,
    (SELECT count() FROM proxy_to_canonical) AS proxies_checked,
    (SELECT count() FROM proxy_uri_to_canonical) AS proxy_uri_keys_checked
);

-- TEST: session_summary_change_flags_consistency
-- Verifies session derived change flags reflect distinct segment identities.
-- Requirement: sessions are segment-derived summaries.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(has_model_change, version) AS has_model_change,
      argMax(has_pipeline_change, version) AS has_pipeline_change
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_segments AS
  (
    SELECT
      workflow_session_id,
      segment_index,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(pipeline, version), '') AS pipeline
    FROM livepeer_analytics.fact_workflow_session_segments
    GROUP BY workflow_session_id, segment_index
  ),
  segment_distinct AS
  (
    SELECT
      workflow_session_id,
      countDistinctIf(model_id, model_id != '') AS distinct_model_count,
      countDistinctIf(pipeline, pipeline != '') AS distinct_pipeline_count
    FROM latest_segments
    GROUP BY workflow_session_id
  )
SELECT
  toUInt64(model_flag_mismatch_rows > 0 OR pipeline_flag_mismatch_rows > 0) AS failed_rows,
  model_flag_mismatch_rows,
  pipeline_flag_mismatch_rows,
  rows_checked
FROM
(
  SELECT
    countIf(
      s.has_model_change = 0
      AND d.distinct_model_count > 1
    ) AS model_flag_mismatch_rows,
    countIf(
      s.has_pipeline_change = 0
      AND d.distinct_pipeline_count > 1
    ) AS pipeline_flag_mismatch_rows,
    count() AS rows_checked
  FROM latest_sessions s
  INNER JOIN segment_distinct d USING (workflow_session_id)
  WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
    AND s.session_start_ts < {to_ts:DateTime64(3)}
);

-- TEST: lifecycle_unattributed_pipeline_not_model_id
-- Verifies unattributed lifecycle sessions never store model labels in the pipeline field.
-- Requirement: `pipeline` must remain canonical workflow class semantics even when attribution is missing.
WITH latest_sessions AS
(
  SELECT
    workflow_session_id,
    argMax(session_start_ts, version) AS session_start_ts,
    argMax(gpu_attribution_method, version) AS gpu_attribution_method,
    argMax(pipeline, version) AS pipeline,
    ifNull(argMax(model_id, version), '') AS model_id
  FROM livepeer_analytics.fact_workflow_sessions
  GROUP BY workflow_session_id
)
SELECT
  toUInt64(bad_rows > 0) AS failed_rows,
  bad_rows,
  rows_checked
FROM
(
  SELECT
    countIf(
      gpu_attribution_method = 'none'
      AND pipeline != ''
      AND model_id != ''
      AND lowerUTF8(pipeline) = lowerUTF8(model_id)
    ) AS bad_rows,
    countIf(
      gpu_attribution_method = 'none'
      AND model_id != ''
    ) AS rows_checked
  FROM latest_sessions
  WHERE session_start_ts >= {from_ts:DateTime64(3)}
    AND session_start_ts < {to_ts:DateTime64(3)}
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
  toUInt64(if(attributed_sessions_checked = 0, 0, uncovered_sessions = attributed_sessions_checked)) AS failed_rows,
  multiIf(
    attributed_sessions_checked = 0, 'INFO',
    uncovered_sessions = 0, 'PASS',
    uncovered_sessions = attributed_sessions_checked, 'FAIL',
    'WARN'
  ) AS status,
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
      AND is_attributed = 1
      AND orchestrator_address != ''
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
      AND orchestrator_address != ''
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
-- Note: expected demand keys intentionally apply the same pipeline fallback semantics
-- as v_api_network_demand to validate parity against serving behavior, not raw pre-fallback keys.
WITH
  latest_sessions_raw AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline_raw,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      session_start_ts,
      gateway,
      region,
      model_id,
      if(
        ifNull(pipeline_raw, '') != ''
        AND ifNull(model_id, '') != ''
        AND lowerUTF8(ifNull(pipeline_raw, '')) = lowerUTF8(ifNull(model_id, '')),
        '',
        ifNull(pipeline_raw, '')
      ) AS pipeline
    FROM latest_sessions_raw
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
      if(
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) != ''
        AND ifNull(p.model_id, '') != ''
        AND lowerUTF8(if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))) = lowerUTF8(ifNull(p.model_id, '')),
        '',
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))
      ) AS pipeline,
      ifNull(p.model_id, '') AS model_id
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE toStartOfInterval(p.window_start, INTERVAL 1 HOUR) >= {from_ts:DateTime64(3)}
      AND toStartOfInterval(p.window_start, INTERVAL 1 HOUR) < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline, model_id
  ),
  demand_1h AS
  (
    SELECT
      toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) AS window_start,
      s.gateway,
      s.region,
      if(
        if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, '')) != ''
        AND ifNull(s.model_id, '') != ''
        AND lowerUTF8(if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, ''))) = lowerUTF8(ifNull(s.model_id, '')),
        '',
        if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, ''))
      ) AS pipeline,
      s.model_id
    FROM latest_sessions s
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR)
     AND f.gateway = s.gateway
     AND f.region = s.region
     AND f.model_id = s.model_id
    WHERE toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) >= {from_ts:DateTime64(3)}
      AND toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) < {to_ts:DateTime64(3)}
    GROUP BY window_start, s.gateway, s.region, pipeline, s.model_id
  ),
  keys_1h AS
  (
    SELECT window_start, gateway, region, pipeline, model_id
    FROM perf_1h
    UNION DISTINCT
    SELECT window_start, gateway, region, pipeline, model_id
    FROM demand_1h
  ),
  expected AS
  (
    SELECT
      toNullable(1) AS expected_marker,
      k.window_start AS window_start,
      k.gateway AS gateway,
      k.region AS region,
      k.pipeline AS pipeline,
      k.model_id AS model_id
    FROM keys_1h k
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
-- Verifies SLA view has strict key/count alignment with in-hour status evidence
-- and terminal-tail filtering semantics.
-- Requirement: expected and served keyspaces must match at attributed SLA grain.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(session_end_ts, version) AS session_end_ts,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(pipeline, version) AS pipeline,
      ifNull(argMax(model_id, version), '') AS model_id,
      ifNull(argMax(gpu_id, version), '') AS gpu_id,
      ifNull(argMax(region, version), '') AS region
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
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
    WHERE toStartOfInterval(sample_ts, INTERVAL 1 HOUR) >= {from_ts:DateTime64(3)}
      AND toStartOfInterval(sample_ts, INTERVAL 1 HOUR) < {to_ts:DateTime64(3)}
      AND is_attributed = 1
    GROUP BY window_start, workflow_session_id, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
  ),
  prev_status_1h AS
  (
    SELECT
      workflow_session_id,
      window_start + INTERVAL 1 HOUR AS window_start,
      orchestrator_address,
      pipeline,
      model_id_key,
      gpu_id_key,
      region_key,
      status_samples AS prev_status_samples,
      fps_positive_samples AS prev_fps_positive_samples,
      running_state_samples AS prev_running_state_samples
    FROM status_1h
  ),
  expected AS
  (
    SELECT
      toNullable(1) AS raw_marker,
      s.window_start AS window_start,
      s.orchestrator_address AS orchestrator_address,
      s.pipeline AS pipeline,
      s.model_id_key AS model_id_key,
      s.gpu_id_key AS gpu_id_key,
      s.region_key AS region_key
    FROM status_1h s
    INNER JOIN latest_sessions ls
      ON ls.workflow_session_id = s.workflow_session_id
    LEFT JOIN prev_status_1h p
      ON p.workflow_session_id = s.workflow_session_id
     AND p.window_start = s.window_start
     AND p.orchestrator_address = s.orchestrator_address
     AND p.pipeline = s.pipeline
     AND p.model_id_key = s.model_id_key
     AND p.gpu_id_key = s.gpu_id_key
     AND p.region_key = s.region_key
    WHERE s.orchestrator_address != ''
      AND NOT (
        ls.session_start_ts < s.window_start
        AND ls.session_end_ts IS NOT NULL
        AND ls.session_end_ts >= s.window_start
        AND ls.session_end_ts < s.window_start + INTERVAL 1 HOUR
        AND
        s.fps_positive_samples = 0
        AND s.running_state_samples = 0
        AND s.status_samples < 3
        AND p.workflow_session_id IS NOT NULL
        AND p.prev_fps_positive_samples = 0
        AND p.prev_running_state_samples = 0
        AND p.prev_status_samples < 3
      )
    GROUP BY s.window_start, s.orchestrator_address, s.pipeline, s.model_id_key, s.gpu_id_key, s.region_key
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
      expected_rows = 0 AND view_rows = 0,
      0,
      expected_rows != view_rows OR expected_only_keys > 0 OR view_only_keys > 0
    )
  ) AS failed_rows,
  multiIf(
    expected_rows = 0 AND view_rows = 0, 'INFO',
    expected_rows = view_rows AND expected_only_keys = 0 AND view_only_keys = 0, 'PASS',
    'FAIL'
  ) AS status,
  expected_rows AS raw_rows,
  view_rows,
  expected_only_keys AS raw_only_keys,
  view_only_keys
FROM
(
  SELECT
    (SELECT count() FROM expected) AS expected_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM expected r
      LEFT JOIN api a
        USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
      WHERE a.api_marker IS NULL
    ) AS expected_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN expected r
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
-- Verifies `v_api_gpu_metrics` matches independently recomputed perf aggregates
-- from status facts at the same dimensional grain.
-- Requirement: serving view must be numerically consistent with perf-backed
-- attributable GPU keys under canonical attributed status identity.
WITH
  api_keys AS
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
      AND status_samples > 0
  ),
  rollup AS
  (
    SELECT
      toNullable(1) AS rollup_marker,
      toStartOfInterval(s.sample_ts, INTERVAL 1 HOUR) AS window_start,
      s.orchestrator_address,
      s.pipeline,
      ifNull(s.model_id, '') AS model_id,
      ifNull(s.gpu_id, '') AS gpu_id,
      ifNull(s.region, '') AS region,
      avg(s.output_fps) AS avg_output_fps
    FROM livepeer_analytics.fact_stream_status_samples s
    INNER JOIN api_keys a
      ON a.window_start = toStartOfInterval(s.sample_ts, INTERVAL 1 HOUR)
     AND a.orchestrator_address = s.orchestrator_address
     AND a.pipeline = s.pipeline
     AND a.model_id = ifNull(s.model_id, '')
     AND a.gpu_id = ifNull(s.gpu_id, '')
     AND a.region = ifNull(s.region, '')
    WHERE s.sample_ts >= {from_ts:DateTime64(3)}
      AND s.sample_ts < {to_ts:DateTime64(3)}
      AND s.is_attributed = 1
      AND s.orchestrator_address != ''
      AND ifNull(s.gpu_id, '') != ''
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
      AND status_samples > 0
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
    OR rollup_only_keys > 0
    OR view_only_keys > 0
    OR (joined_rows > 0 AND ifNull(max_abs_diff_fps, 0) > 0.000001)
  ) AS failed_rows,
  multiIf(
    rollup_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    rollup_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    rollup_only_keys > 0 OR view_only_keys > 0, 'KEY_MISMATCH',
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
-- Note: both perf and demand expected branches must mirror v_api_network_demand fallback behavior;
-- otherwise parity checks can fail on keyshape while underlying metrics still match.
WITH
  latest_sessions_raw AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline_raw,
      ifNull(argMax(model_id, version), '') AS model_id,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      session_start_ts,
      gateway,
      region,
      model_id,
      orchestrator_address,
      known_stream,
      startup_unexcused,
      confirmed_swap_count,
      inferred_orchestrator_change_count,
      if(
        ifNull(pipeline_raw, '') != ''
        AND ifNull(model_id, '') != ''
        AND lowerUTF8(ifNull(pipeline_raw, '')) = lowerUTF8(ifNull(model_id, '')),
        '',
        ifNull(pipeline_raw, '')
      ) AS pipeline
    FROM latest_sessions_raw
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
      if(
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) != ''
        AND ifNull(p.model_id, '') != ''
        AND lowerUTF8(if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))) = lowerUTF8(ifNull(p.model_id, '')),
        '',
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))
      ) AS pipeline,
      ifNull(p.model_id, '') AS model_id,
      uniqExactMerge(p.sessions_uniq_state) AS total_sessions,
      uniqExactMerge(p.streams_uniq_state) AS total_streams,
      countMerge(p.sample_count_state) / 60.0 AS total_minutes,
      avgMerge(p.output_fps_avg_state) AS avg_output_fps
    FROM livepeer_analytics.agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
     AND f.gateway = p.gateway
     AND f.region = ifNull(p.region, '')
     AND f.model_id = ifNull(p.model_id, '')
    WHERE toStartOfInterval(p.window_start, INTERVAL 1 HOUR) >= {from_ts:DateTime64(3)}
      AND toStartOfInterval(p.window_start, INTERVAL 1 HOUR) < {to_ts:DateTime64(3)}
    GROUP BY window_start, p.gateway, region, pipeline, model_id
  ),
  demand_1h AS
  (
    SELECT
      toNullable(1) AS demand_marker,
      toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) AS window_start,
      s.gateway,
      s.region,
      if(
        if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, '')) != ''
        AND ifNull(s.model_id, '') != ''
        AND lowerUTF8(if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, ''))) = lowerUTF8(ifNull(s.model_id, '')),
        '',
        if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, ''))
      ) AS pipeline,
      s.model_id,
      sum(toUInt64(s.known_stream)) AS known_sessions,
      sum(toUInt64(s.known_stream AND s.orchestrator_address != '')) AS served_sessions,
      sum(toUInt64(s.known_stream AND s.orchestrator_address = '')) AS unserved_sessions,
      sum(toUInt64(s.startup_unexcused)) AS unexcused_sessions,
      sum(toUInt64((s.confirmed_swap_count > 0) OR (s.inferred_orchestrator_change_count > 0))) AS swapped_sessions
    FROM latest_sessions s
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR)
     AND f.gateway = s.gateway
     AND f.region = s.region
     AND f.model_id = s.model_id
    WHERE toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) >= {from_ts:DateTime64(3)}
      AND toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) < {to_ts:DateTime64(3)}
    GROUP BY window_start, s.gateway, s.region, pipeline, s.model_id
  ),
  keys_1h AS
  (
    SELECT window_start, gateway, region, pipeline, model_id
    FROM perf_1h
    UNION DISTINCT
    SELECT window_start, gateway, region, pipeline, model_id
    FROM demand_1h
  ),
  expected AS
  (
    SELECT
      toNullable(1) AS expected_marker,
      k.window_start AS window_start,
      k.gateway AS gateway,
      k.region AS region,
      k.pipeline AS pipeline,
      k.model_id AS model_id,
      ifNull(p.total_sessions, toUInt64(0)) AS total_sessions,
      ifNull(p.total_streams, toUInt64(0)) AS total_streams,
      ifNull(p.total_minutes, 0.0) AS total_minutes,
      p.avg_output_fps,
      ifNull(d.known_sessions, toUInt64(0)) AS known_sessions,
      ifNull(d.served_sessions, toUInt64(0)) AS served_sessions,
      ifNull(d.unserved_sessions, toUInt64(0)) AS unserved_sessions,
      ifNull(d.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
      ifNull(d.swapped_sessions, toUInt64(0)) AS swapped_sessions
    FROM keys_1h k
    LEFT JOIN perf_1h p
      ON p.window_start = k.window_start
     AND p.gateway = k.gateway
     AND p.region = k.region
     AND p.pipeline = k.pipeline
     AND p.model_id = k.model_id
    LEFT JOIN demand_1h d
      ON d.window_start = k.window_start
     AND d.gateway = k.gateway
     AND d.region = k.region
     AND d.pipeline = k.pipeline
     AND d.model_id = k.model_id
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
      total_minutes,
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
      avg(abs(e.total_minutes - a.total_minutes)) AS mean_abs_diff_minutes,
      max(abs(e.total_minutes - a.total_minutes)) AS max_abs_diff_minutes,
      sum(abs(toInt64(e.total_sessions) - toInt64(a.total_sessions))) AS total_diff_sessions,
      sum(abs(toInt64(e.total_streams) - toInt64(a.total_streams))) AS total_diff_streams,
      sum(abs(toInt64(e.known_sessions) - toInt64(a.known_sessions))) AS total_diff_known_sessions,
      sum(abs(toInt64(e.served_sessions) - toInt64(a.served_sessions))) AS total_diff_served_sessions,
      sum(abs(toInt64(e.unserved_sessions) - toInt64(a.unserved_sessions))) AS total_diff_unserved_sessions,
      sum(abs(toInt64(e.unexcused_sessions) - toInt64(a.unexcused_sessions))) AS total_diff_unexcused_sessions,
      sum(abs(toInt64(e.swapped_sessions) - toInt64(a.swapped_sessions))) AS total_diff_swapped_sessions
    FROM expected e
    INNER JOIN api a
      ON a.window_start = e.window_start
     AND a.gateway = e.gateway
     AND a.region = e.region
     AND a.pipeline = e.pipeline
     AND a.model_id = e.model_id
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
          ON a.window_start = e.window_start
         AND a.gateway = e.gateway
         AND a.region = e.region
         AND a.pipeline = e.pipeline
         AND a.model_id = e.model_id
        WHERE a.api_marker IS NULL
      ) AS rollup_only_keys,
      (
        SELECT count()
        FROM api a
        LEFT JOIN expected e
          ON e.window_start = a.window_start
         AND e.gateway = a.gateway
         AND e.region = a.region
         AND e.pipeline = a.pipeline
         AND e.model_id = a.model_id
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
    OR rollup_only_keys > 0
    OR view_only_keys > 0
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
    rollup_only_keys > 0 OR view_only_keys > 0, 'KEY_MISMATCH',
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

-- TEST: network_demand_by_gpu_matches_latest_sessions
-- Verifies `v_api_network_demand_by_gpu` reliability counters match latest-session recompute.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      argMax(orchestrator_address, version) AS orchestrator_address,
      argMax(region, version) AS region,
      argMax(pipeline, version) AS pipeline,
      argMax(model_id, version) AS model_id,
      argMax(gpu_id, version) AS gpu_id,
      argMax(known_stream, version) AS known_stream,
      argMax(startup_unexcused, version) AS startup_unexcused,
      argMax(confirmed_swap_count, version) AS confirmed_swap_count,
      argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
      argMax(error_count, version) AS error_count,
      argMax(last_error_occurred, version) AS last_error_occurred,
      argMax(loading_only_session, version) AS loading_only_session,
      argMax(zero_output_fps_session, version) AS zero_output_fps_session,
      argMax(status_error_sample_count, version) AS status_error_sample_count,
      argMax(health_signal_count, version) AS health_signal_count,
      argMax(health_expected_signal_count, version) AS health_expected_signal_count
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_gpu_by_key AS
  (
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      gateway,
      orchestrator_address,
      region,
      pipeline,
      model_id,
      argMaxIf(gpu_id, session_start_ts, ifNull(gpu_id, '') != '') AS gpu_id
    FROM latest_sessions
    GROUP BY window_start, gateway, orchestrator_address, region, pipeline, model_id
  ),
  raw AS
  (
    SELECT
      toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) AS window_start,
      s.gateway,
      s.orchestrator_address,
      ifNull(s.region, '') AS region_key,
      s.pipeline,
      ifNull(s.model_id, '') AS model_id_key,
      ifNull(nullIf(if(ifNull(s.gpu_id, '') != '', s.gpu_id, ifNull(k.gpu_id, '')), ''), '') AS gpu_id_key,
      sum(toUInt64(s.known_stream)) AS known_sessions,
      sum(toUInt64(s.startup_unexcused)) AS unexcused_sessions,
      sum(toUInt64((s.confirmed_swap_count > 0) OR (s.inferred_orchestrator_change_count > 0))) AS swapped_sessions,
      sum(toUInt64(s.error_count > 0)) AS sessions_with_errors,
      sum(toUInt64(s.last_error_occurred > 0)) AS sessions_with_last_error,
      sum(toUInt64(s.loading_only_session > 0)) AS loading_only_sessions,
      sum(toUInt64(s.zero_output_fps_session > 0)) AS zero_output_fps_sessions,
      sum(toUInt64(s.status_error_sample_count)) AS status_error_samples,
      sum(toUInt64(s.health_signal_count)) AS health_signal_count,
      sum(toUInt64(s.health_expected_signal_count)) AS health_expected_signal_count
    FROM latest_sessions s
    LEFT JOIN latest_gpu_by_key k
      ON k.window_start = toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR)
     AND k.gateway = s.gateway
     AND k.orchestrator_address = s.orchestrator_address
     AND ifNull(k.region, '') = ifNull(s.region, '')
     AND k.pipeline = s.pipeline
     AND ifNull(k.model_id, '') = ifNull(s.model_id, '')
    WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
      AND s.session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, s.gateway, s.orchestrator_address, region_key, s.pipeline, model_id_key, gpu_id_key
  ),
  api AS
  (
    SELECT
      window_start,
      gateway,
      orchestrator_address,
      ifNull(region, '') AS region_key,
      pipeline,
      ifNull(model_id, '') AS model_id_key,
      ifNull(gpu_id, '') AS gpu_id_key,
      known_sessions,
      unexcused_sessions,
      swapped_sessions,
      sessions_with_errors,
      sessions_with_last_error,
      loading_only_sessions,
      zero_output_fps_sessions,
      status_error_samples,
      health_signal_count,
      health_expected_signal_count
    FROM livepeer_analytics.v_api_network_demand_by_gpu
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
  )
SELECT
  toUInt64(
    (raw_rows > 0 AND view_rows > 0 AND joined_rows = 0)
    OR raw_only_keys > 0
    OR view_only_keys > 0
    OR total_known_diff > 0
    OR total_unexcused_diff > 0
    OR total_swapped_diff > 0
    OR total_errors_diff > 0
    OR total_last_error_diff > 0
    OR total_loading_only_diff > 0
    OR total_zero_output_diff > 0
    OR total_status_error_samples_diff > 0
    OR total_health_signal_count_diff > 0
    OR total_health_expected_signal_count_diff > 0
  ) AS failed_rows,
  multiIf(
    raw_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    raw_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    raw_only_keys > 0 OR view_only_keys > 0, 'KEY_MISMATCH',
    total_known_diff > 0
    OR total_unexcused_diff > 0
    OR total_swapped_diff > 0
    OR total_errors_diff > 0
    OR total_last_error_diff > 0
    OR total_loading_only_diff > 0
    OR total_zero_output_diff > 0
    OR total_status_error_samples_diff > 0
    OR total_health_signal_count_diff > 0
    OR total_health_expected_signal_count_diff > 0, 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
  raw_rows,
  view_rows,
  joined_rows,
  raw_only_keys,
  view_only_keys,
  total_known_diff,
  total_unexcused_diff,
  total_swapped_diff,
  total_errors_diff,
  total_last_error_diff,
  total_loading_only_diff,
  total_zero_output_diff,
  total_status_error_samples_diff,
  total_health_signal_count_diff,
  total_health_expected_signal_count_diff
FROM
(
  SELECT
    (SELECT count() FROM raw) AS raw_rows,
    (SELECT count() FROM api) AS view_rows,
    (
      SELECT count()
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS joined_rows,
    (
      SELECT count()
      FROM raw r
      LEFT JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
      WHERE a.window_start IS NULL
    ) AS raw_only_keys,
    (
      SELECT count()
      FROM api a
      LEFT JOIN raw r
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
      WHERE r.window_start IS NULL
    ) AS view_only_keys,
    (
      SELECT ifNull(sum(abs(r.known_sessions - a.known_sessions)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_known_diff,
    (
      SELECT ifNull(sum(abs(r.unexcused_sessions - a.unexcused_sessions)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_unexcused_diff,
    (
      SELECT ifNull(sum(abs(r.swapped_sessions - a.swapped_sessions)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_swapped_diff,
    (
      SELECT ifNull(sum(abs(r.sessions_with_errors - a.sessions_with_errors)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_errors_diff,
    (
      SELECT ifNull(sum(abs(r.sessions_with_last_error - a.sessions_with_last_error)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_last_error_diff,
    (
      SELECT ifNull(sum(abs(r.loading_only_sessions - a.loading_only_sessions)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_loading_only_diff,
    (
      SELECT ifNull(sum(abs(r.zero_output_fps_sessions - a.zero_output_fps_sessions)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_zero_output_diff,
    (
      SELECT ifNull(sum(abs(r.status_error_samples - a.status_error_samples)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_status_error_samples_diff,
    (
      SELECT ifNull(sum(abs(r.health_signal_count - a.health_signal_count)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_health_signal_count_diff,
    (
      SELECT ifNull(sum(abs(r.health_expected_signal_count - a.health_expected_signal_count)), 0)
      FROM raw r
      INNER JOIN api a
        USING (window_start, gateway, orchestrator_address, region_key, pipeline, model_id_key, gpu_id_key)
    ) AS total_health_expected_signal_count_diff
);

-- TEST: network_demand_effective_success_not_above_startup_success
-- Verifies effective success ratio does not exceed startup-only success ratio.
SELECT
  toUInt64(countIf(startup_success_ratio + 0.000001 < success_ratio) > 0) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_network_demand
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: network_demand_effective_success_penalizes_failure_indicators
-- Verifies rows with explicit failure indicators do not report perfect effective success.
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

-- TEST: network_demand_model_split_conservation
-- Informational guard for model-split re-aggregation behavior.
-- Requirement: distinct-count fields (`total_sessions`,`total_streams`) are non-additive across model
-- splits and may inflate when summed; additive minutes should remain conserved.
WITH
  latest_sessions_raw AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(gateway, version) AS gateway,
      ifNull(argMax(region, version), '') AS region,
      argMax(pipeline, version) AS pipeline_raw,
      ifNull(argMax(model_id, version), '') AS model_id
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
  ),
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      session_start_ts,
      gateway,
      region,
      model_id,
      if(
        ifNull(pipeline_raw, '') != ''
        AND ifNull(model_id, '') != ''
        AND lowerUTF8(ifNull(pipeline_raw, '')) = lowerUTF8(ifNull(model_id, '')),
        '',
        ifNull(pipeline_raw, '')
      ) AS pipeline
    FROM latest_sessions_raw
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
      if(
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) != ''
        AND ifNull(p.model_id, '') != ''
        AND lowerUTF8(if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))) = lowerUTF8(ifNull(p.model_id, '')),
        '',
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, ''))
      ) AS pipeline,
      uniqExactMerge(p.sessions_uniq_state) AS total_sessions,
      uniqExactMerge(p.streams_uniq_state) AS total_streams,
      countMerge(p.sample_count_state) / 60.0 AS total_minutes
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
      sum(total_minutes) AS total_minutes
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
      max(abs(b.total_minutes - a.total_minutes)) AS max_abs_diff_minutes
    FROM pipeline_baseline b
    INNER JOIN api_reaggregated a
      USING (window_start, gateway, region, pipeline)
  )
SELECT
  toUInt64(0) AS failed_rows,
  multiIf(
    baseline_rows = 0 AND api_rows = 0, 'INFO',
    baseline_rows > 0 AND api_rows > 0 AND joined_rows = 0, 'WARN',
    baseline_only_keys > 0 OR api_only_keys > 0, 'WARN',
    ifNull(max_abs_diff_minutes, 0) > 0.000001, 'FAIL',
    ifNull(total_diff_sessions, 0) > 0 OR ifNull(total_diff_streams, 0) > 0, 'WARN',
    'PASS'
  ) AS status,
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
      toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR) AS window_start,
      s.gateway,
      s.region,
      if(s.pipeline != '', s.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
      s.model_id
    FROM latest_sessions s
    LEFT JOIN perf_pipeline_fallback f
      ON f.window_start = toStartOfInterval(s.session_start_ts, INTERVAL 1 HOUR)
     AND f.gateway = s.gateway
     AND f.region = s.region
     AND f.model_id = s.model_id
    WHERE s.session_start_ts >= {from_ts:DateTime64(3)}
      AND s.session_start_ts < {to_ts:DateTime64(3)}
    GROUP BY window_start, s.gateway, s.region, pipeline, s.model_id
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
-- Verifies existing model-grain views conserve totals and keyspace when collapsed to pipeline grain.
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
        argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
        argMax(last_error_occurred, version) AS last_error_occurred,
        argMax(loading_only_session, version) AS loading_only_session,
        argMax(zero_output_fps_session, version) AS zero_output_fps_session,
        argMax(status_error_sample_count, version) AS status_error_sample_count,
        argMax(health_signal_count, version) AS health_signal_count,
        argMax(health_expected_signal_count, version) AS health_expected_signal_count
      FROM livepeer_analytics.fact_workflow_sessions
      GROUP BY workflow_session_id
    )
    SELECT
      toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
      orchestrator_address,
      pipeline,
      sum(toUInt64(known_stream)) AS raw_known_sessions,
      sum(toUInt64(startup_unexcused)) AS raw_unexcused_sessions,
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions,
      sum(toUInt64(last_error_occurred > 0)) AS raw_sessions_with_last_error,
      sum(toUInt64(loading_only_session > 0)) AS raw_loading_only_sessions,
      sum(toUInt64(zero_output_fps_session > 0)) AS raw_zero_output_fps_sessions,
      sum(toUInt64(status_error_sample_count)) AS raw_status_error_samples,
      sum(toUInt64(health_signal_count)) AS raw_health_signal_count,
      sum(toUInt64(health_expected_signal_count)) AS raw_health_expected_signal_count
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
      sum(swapped_sessions) AS view_swapped_sessions,
      sum(sessions_with_last_error) AS view_sessions_with_last_error,
      sum(loading_only_sessions) AS view_loading_only_sessions,
      sum(zero_output_fps_sessions) AS view_zero_output_fps_sessions,
      sum(status_error_samples) AS view_status_error_samples,
      sum(health_signal_count) AS view_health_signal_count,
      sum(health_expected_signal_count) AS view_health_expected_signal_count
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
    GROUP BY window_start, orchestrator_address, pipeline
  )
SELECT
  toUInt64(0) AS failed_rows,
  multiIf(
    gpu_rollup_only_keys > 0
    OR gpu_view_only_keys > 0
    OR gpu_status_sample_diff > 0
    OR sla_raw_only_keys > 0
    OR sla_view_only_keys > 0
    OR sla_known_diff > 0
    OR sla_unexcused_diff > 0
    OR sla_swapped_diff > 0
    OR sla_last_error_diff > 0
    OR sla_loading_only_diff > 0
    OR sla_zero_output_diff > 0
    OR sla_status_error_samples_diff > 0
    OR sla_health_signal_count_diff > 0
    OR sla_health_expected_signal_count_diff > 0,
    'WARN',
    'PASS'
  ) AS status,
  gpu_rollup_rows,
  gpu_view_rows,
  gpu_rollup_only_keys,
  gpu_view_only_keys,
  gpu_status_sample_diff,
  sla_raw_rows,
  sla_view_rows,
  sla_raw_only_keys,
  sla_view_only_keys,
  sla_known_diff,
  sla_unexcused_diff,
  sla_swapped_diff,
  sla_last_error_diff,
  sla_loading_only_diff,
  sla_zero_output_diff,
  sla_status_error_samples_diff,
  sla_health_signal_count_diff,
  sla_health_expected_signal_count_diff
FROM
(
  SELECT
    (SELECT count() FROM gpu_rollup_pipeline) AS gpu_rollup_rows,
    (SELECT count() FROM gpu_view_pipeline) AS gpu_view_rows,
    (
      SELECT count()
      FROM gpu_rollup_pipeline r
      LEFT JOIN gpu_view_pipeline v
        USING (window_start, orchestrator_address, pipeline)
      WHERE v.window_start IS NULL
    ) AS gpu_rollup_only_keys,
    (
      SELECT count()
      FROM gpu_view_pipeline v
      LEFT JOIN gpu_rollup_pipeline r
        USING (window_start, orchestrator_address, pipeline)
      WHERE r.window_start IS NULL
    ) AS gpu_view_only_keys,
    ifNull(sum(abs(toInt64(r.expected_status_samples) - toInt64(v.view_status_samples))), 0) AS gpu_status_sample_diff
  FROM gpu_rollup_pipeline r
  INNER JOIN gpu_view_pipeline v
    USING (window_start, orchestrator_address, pipeline)
) gpu
CROSS JOIN
(
  SELECT
    (SELECT count() FROM sla_raw_pipeline) AS sla_raw_rows,
    (SELECT count() FROM sla_view_pipeline) AS sla_view_rows,
    (
      SELECT count()
      FROM sla_raw_pipeline r
      LEFT JOIN sla_view_pipeline v
        USING (window_start, orchestrator_address, pipeline)
      WHERE v.window_start IS NULL
    ) AS sla_raw_only_keys,
    (
      SELECT count()
      FROM sla_view_pipeline v
      LEFT JOIN sla_raw_pipeline r
        USING (window_start, orchestrator_address, pipeline)
      WHERE r.window_start IS NULL
    ) AS sla_view_only_keys,
    ifNull(sum(abs(toInt64(r.raw_known_sessions) - toInt64(v.view_known_sessions))), 0) AS sla_known_diff,
    ifNull(sum(abs(toInt64(r.raw_unexcused_sessions) - toInt64(v.view_unexcused_sessions))), 0) AS sla_unexcused_diff,
    ifNull(sum(abs(toInt64(r.raw_swapped_sessions) - toInt64(v.view_swapped_sessions))), 0) AS sla_swapped_diff,
    ifNull(sum(abs(toInt64(r.raw_sessions_with_last_error) - toInt64(v.view_sessions_with_last_error))), 0) AS sla_last_error_diff,
    ifNull(sum(abs(toInt64(r.raw_loading_only_sessions) - toInt64(v.view_loading_only_sessions))), 0) AS sla_loading_only_diff,
    ifNull(sum(abs(toInt64(r.raw_zero_output_fps_sessions) - toInt64(v.view_zero_output_fps_sessions))), 0) AS sla_zero_output_diff,
    ifNull(sum(abs(toInt64(r.raw_status_error_samples) - toInt64(v.view_status_error_samples))), 0) AS sla_status_error_samples_diff,
    ifNull(sum(abs(toInt64(r.raw_health_signal_count) - toInt64(v.view_health_signal_count))), 0) AS sla_health_signal_count_diff,
    ifNull(sum(abs(toInt64(r.raw_health_expected_signal_count) - toInt64(v.view_health_expected_signal_count))), 0) AS sla_health_expected_signal_count_diff
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
      sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS raw_swapped_sessions,
      sum(toUInt64(last_error_occurred > 0)) AS raw_sessions_with_last_error,
      sum(toUInt64(loading_only_session > 0)) AS raw_loading_only_sessions,
      sum(toUInt64(zero_output_fps_session > 0)) AS raw_zero_output_fps_sessions,
      sum(toUInt64(status_error_sample_count)) AS raw_status_error_samples,
      sum(toUInt64(health_signal_count)) AS raw_health_signal_count,
      sum(toUInt64(health_expected_signal_count)) AS raw_health_expected_signal_count
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
  multiIf(
    raw_rows = 0 AND view_rows = 0, 'EMPTY_BOTH',
    raw_rows > 0 AND view_rows > 0 AND joined_rows = 0, 'NO_OVERLAP_BOTH_NONEMPTY',
    raw_only_keys > 0 OR view_only_keys > 0, 'KEY_MISMATCH',
    joined_rows > 0 AND (
      total_known_diff > 0
      OR total_unexcused_diff > 0
      OR total_swapped_diff > 0
      OR total_last_error_diff > 0
      OR total_loading_only_diff > 0
      OR total_zero_output_diff > 0
      OR total_status_error_samples_diff > 0
      OR total_health_signal_count_diff > 0
      OR total_health_expected_signal_count_diff > 0
    ), 'VALUE_MISMATCH_WITH_OVERLAP',
    'PASS'
  ) AS failure_mode,
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
    ) AS total_swapped_diff,
    (
      SELECT sum(abs(r.raw_sessions_with_last_error - a.sessions_with_last_error))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_last_error_diff,
    (
      SELECT sum(abs(r.raw_loading_only_sessions - a.loading_only_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_loading_only_diff,
    (
      SELECT sum(abs(r.raw_zero_output_fps_sessions - a.zero_output_fps_sessions))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_zero_output_diff,
    (
      SELECT sum(abs(r.raw_status_error_samples - a.status_error_samples))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_status_error_samples_diff,
    (
      SELECT sum(abs(r.raw_health_signal_count - a.health_signal_count))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_health_signal_count_diff,
    (
      SELECT sum(abs(r.raw_health_expected_signal_count - a.health_expected_signal_count))
      FROM raw r
      INNER JOIN api a
      USING (window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key)
    ) AS total_health_expected_signal_count_diff
);

-- TEST: sla_ratios_in_bounds
-- Verifies SLA ratio fields are bounded between 0 and 1.
-- Requirement: API-facing compliance ratios must remain mathematically valid.
SELECT
  countIf(startup_success_ratio < 0 OR startup_success_ratio > 1)
  + countIf(success_ratio < 0 OR success_ratio > 1)
  + countIf(startup_success_ratio + 0.000001 < success_ratio)
  + countIf(no_swap_ratio < 0 OR no_swap_ratio > 1)
  + countIf(health_completeness_ratio < 0 OR health_completeness_ratio > 1)
  + countIf(sla_score < 0 OR sla_score > 100) AS failed_rows,
  count() AS rows_checked
FROM livepeer_analytics.v_api_sla_compliance
WHERE window_start >= {from_ts:DateTime64(3)}
  AND window_start < {to_ts:DateTime64(3)};

-- TEST: terminal_tail_artifact_filtered_in_gpu_sla
-- Verifies true rollover terminal tail hours with no-work prior/current evidence are filtered from GPU/SLA views.
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(session_end_ts, version) AS session_end_ts
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
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
  ),
  prev_status_1h AS
  (
    SELECT
      workflow_session_id,
      window_start + INTERVAL 1 HOUR AS window_start,
      orchestrator_address,
      pipeline,
      model_id_key,
      gpu_id_key,
      region_key,
      status_samples AS prev_status_samples,
      fps_positive_samples AS prev_fps_positive_samples,
      running_state_samples AS prev_running_state_samples
    FROM status_1h
  ),
  artifacts AS
  (
    SELECT
      s.window_start AS window_start,
      s.orchestrator_address AS orchestrator_address,
      s.pipeline AS pipeline,
      s.model_id_key AS model_id_key,
      s.gpu_id_key AS gpu_id_key,
      s.region_key AS region_key
    FROM status_1h s
    INNER JOIN latest_sessions ls
      ON ls.workflow_session_id = s.workflow_session_id
    LEFT JOIN prev_status_1h p
      ON p.workflow_session_id = s.workflow_session_id
     AND p.window_start = s.window_start
     AND p.orchestrator_address = s.orchestrator_address
     AND p.pipeline = s.pipeline
     AND p.model_id_key = s.model_id_key
     AND p.gpu_id_key = s.gpu_id_key
     AND p.region_key = s.region_key
    WHERE s.fps_positive_samples = 0
      AND s.running_state_samples = 0
      AND s.status_samples < 3
      AND ls.session_start_ts < s.window_start
      AND ls.session_end_ts IS NOT NULL
      AND ls.session_end_ts >= s.window_start
      AND ls.session_end_ts < s.window_start + INTERVAL 1 HOUR
      AND p.workflow_session_id IS NOT NULL
      AND p.prev_fps_positive_samples = 0
      AND p.prev_running_state_samples = 0
      AND p.prev_status_samples < 3
    GROUP BY s.window_start, s.orchestrator_address, s.pipeline, s.model_id_key, s.gpu_id_key, s.region_key
  ),
  gpu_hits AS
  (
    SELECT count() AS c
    FROM artifacts
    INNER JOIN livepeer_analytics.v_api_gpu_metrics g
      ON g.window_start = artifacts.window_start
     AND g.orchestrator_address = artifacts.orchestrator_address
     AND g.pipeline = artifacts.pipeline
     AND ifNull(g.model_id, '') = artifacts.model_id_key
     AND ifNull(g.gpu_id, '') = artifacts.gpu_id_key
     AND ifNull(g.region, '') = artifacts.region_key
  ),
  sla_hits AS
  (
    SELECT count() AS c
    FROM artifacts
    INNER JOIN livepeer_analytics.v_api_sla_compliance s
      ON s.window_start = artifacts.window_start
     AND s.orchestrator_address = artifacts.orchestrator_address
     AND s.pipeline = artifacts.pipeline
     AND ifNull(s.model_id, '') = artifacts.model_id_key
     AND ifNull(s.gpu_id, '') = artifacts.gpu_id_key
     AND ifNull(s.region, '') = artifacts.region_key
  )
SELECT
  toUInt64((SELECT c FROM gpu_hits) + (SELECT c FROM sla_hits)) AS failed_rows,
  multiIf(
    (SELECT count() FROM artifacts) = 0, 'INFO',
    ((SELECT c FROM gpu_hits) + (SELECT c FROM sla_hits)) = 0, 'PASS',
    'FAIL'
  ) AS status,
  (SELECT count() FROM artifacts) AS artifact_candidates,
  (SELECT c FROM gpu_hits) AS gpu_rows_found,
  (SELECT c FROM sla_hits) AS sla_rows_found;

-- TEST: same_hour_failed_no_output_retained_in_gpu_sla
-- Verifies same-hour failed no-output attempts are visible (not filtered as terminal rollover tails).
WITH
  latest_sessions AS
  (
    SELECT
      workflow_session_id,
      argMax(session_start_ts, version) AS session_start_ts,
      argMax(session_end_ts, version) AS session_end_ts,
      argMax(last_error_occurred, version) AS last_error_occurred,
      argMax(loading_only_session, version) AS loading_only_session,
      argMax(zero_output_fps_session, version) AS zero_output_fps_session
    FROM livepeer_analytics.fact_workflow_sessions
    GROUP BY workflow_session_id
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
  ),
  same_hour_failed AS
  (
    SELECT
      s.window_start,
      s.orchestrator_address,
      s.pipeline,
      s.model_id_key,
      s.gpu_id_key,
      s.region_key
    FROM status_1h s
    INNER JOIN latest_sessions ls
      ON ls.workflow_session_id = s.workflow_session_id
    WHERE s.fps_positive_samples = 0
      AND s.running_state_samples = 0
      AND s.status_samples < 3
      AND ls.session_start_ts >= s.window_start
      AND ls.session_start_ts < s.window_start + INTERVAL 1 HOUR
      AND ls.session_end_ts >= s.window_start
      AND ls.session_end_ts < s.window_start + INTERVAL 1 HOUR
      AND (ls.last_error_occurred > 0 OR ls.loading_only_session > 0 OR ls.zero_output_fps_session > 0)
      AND s.orchestrator_address != ''
      AND ifNull(s.gpu_id_key, '') != ''
    GROUP BY s.window_start, s.orchestrator_address, s.pipeline, s.model_id_key, s.gpu_id_key, s.region_key
  ),
  gpu_missing AS
  (
    SELECT count() AS c
    FROM same_hour_failed a
    LEFT JOIN livepeer_analytics.v_api_gpu_metrics g
      ON g.window_start = a.window_start
     AND g.orchestrator_address = a.orchestrator_address
     AND g.pipeline = a.pipeline
     AND ifNull(g.model_id, '') = a.model_id_key
     AND ifNull(g.gpu_id, '') = a.gpu_id_key
     AND ifNull(g.region, '') = a.region_key
    WHERE g.window_start IS NULL
  ),
  sla_missing AS
  (
    SELECT count() AS c
    FROM same_hour_failed a
    LEFT JOIN livepeer_analytics.v_api_sla_compliance s
      ON s.window_start = a.window_start
     AND s.orchestrator_address = a.orchestrator_address
     AND s.pipeline = a.pipeline
     AND ifNull(s.model_id, '') = a.model_id_key
     AND ifNull(s.gpu_id, '') = a.gpu_id_key
     AND ifNull(s.region, '') = a.region_key
    WHERE s.window_start IS NULL
  )
SELECT
  toUInt64((SELECT c FROM gpu_missing) + (SELECT c FROM sla_missing)) AS failed_rows,
  multiIf(
    (SELECT count() FROM same_hour_failed) = 0, 'INFO',
    ((SELECT c FROM gpu_missing) + (SELECT c FROM sla_missing)) = 0, 'PASS',
    'FAIL'
  ) AS status,
  (SELECT count() FROM same_hour_failed) AS candidates,
  (SELECT c FROM gpu_missing) AS gpu_missing_rows,
  (SELECT c FROM sla_missing) AS sla_missing_rows;

-- TEST: active_no_output_rows_retained_in_gpu_sla
-- Verifies non-tail zero-FPS active hours remain visible for diagnostics.
WITH status_1h AS
(
  SELECT
    toStartOfInterval(sample_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address,
    pipeline,
    ifNull(model_id, '') AS model_id_key,
    ifNull(gpu_id, '') AS gpu_id_key,
    ifNull(region, '') AS region_key,
    count() AS status_samples,
    countIf(output_fps > 0) AS fps_positive_samples
  FROM livepeer_analytics.fact_stream_status_samples
  WHERE sample_ts >= {from_ts:DateTime64(3)}
    AND sample_ts < {to_ts:DateTime64(3)}
    AND is_attributed = 1
    AND orchestrator_address != ''
    AND ifNull(gpu_id, '') != ''
  GROUP BY window_start, orchestrator_address, pipeline, model_id_key, gpu_id_key, region_key
),
active_no_output AS
(
  SELECT
    window_start,
    orchestrator_address,
    pipeline,
    model_id_key,
    gpu_id_key,
    region_key
  FROM status_1h
  WHERE fps_positive_samples = 0
    AND status_samples >= 3
),
gpu_missing AS
(
  SELECT count() AS c
  FROM active_no_output a
  LEFT JOIN livepeer_analytics.v_api_gpu_metrics g
    ON g.window_start = a.window_start
   AND g.orchestrator_address = a.orchestrator_address
   AND g.pipeline = a.pipeline
   AND ifNull(g.model_id, '') = a.model_id_key
   AND ifNull(g.gpu_id, '') = a.gpu_id_key
   AND ifNull(g.region, '') = a.region_key
  WHERE g.window_start IS NULL
),
sla_missing AS
(
  SELECT count() AS c
  FROM active_no_output a
  LEFT JOIN livepeer_analytics.v_api_sla_compliance s
    ON s.window_start = a.window_start
   AND s.orchestrator_address = a.orchestrator_address
   AND s.pipeline = a.pipeline
   AND ifNull(s.model_id, '') = a.model_id_key
   AND ifNull(s.gpu_id, '') = a.gpu_id_key
   AND ifNull(s.region, '') = a.region_key
  WHERE s.window_start IS NULL
)
SELECT
  toUInt64((SELECT c FROM gpu_missing) + (SELECT c FROM sla_missing)) AS failed_rows,
  multiIf(
    (SELECT count() FROM active_no_output) = 0, 'INFO',
    ((SELECT c FROM gpu_missing) + (SELECT c FROM sla_missing)) = 0, 'PASS',
    'FAIL'
  ) AS status,
  (SELECT count() FROM active_no_output) AS active_no_output_candidates,
  (SELECT c FROM gpu_missing) AS gpu_missing_rows,
  (SELECT c FROM sla_missing) AS sla_missing_rows;

-- TEST: gold_pipeline_empty_hotspots_diagnostic
-- Informational: ranks key hotspots where model_id is present but canonical pipeline stayed empty.
WITH
  hotspots AS
  (
    SELECT
      'v_api_gpu_metrics' AS view_name,
      window_start,
      orchestrator_address,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      count() AS rows_with_empty_pipeline
    FROM livepeer_analytics.v_api_gpu_metrics
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
      AND ifNull(model_id, '') != ''
      AND pipeline = ''
    GROUP BY view_name, window_start, orchestrator_address, model_id, gpu_id

    UNION ALL

    SELECT
      'v_api_sla_compliance' AS view_name,
      window_start,
      orchestrator_address,
      ifNull(model_id, '') AS model_id,
      ifNull(gpu_id, '') AS gpu_id,
      count() AS rows_with_empty_pipeline
    FROM livepeer_analytics.v_api_sla_compliance
    WHERE window_start >= {from_ts:DateTime64(3)}
      AND window_start < {to_ts:DateTime64(3)}
      AND ifNull(model_id, '') != ''
      AND pipeline = ''
    GROUP BY view_name, window_start, orchestrator_address, model_id, gpu_id
  )
SELECT
  toUInt64(0) AS failed_rows,
  (SELECT sum(rows_with_empty_pipeline) FROM hotspots) AS total_rows_with_empty_pipeline,
  (SELECT topK(5)(orchestrator_address) FROM hotspots) AS top_orchestrator_addresses,
  (SELECT topK(5)(model_id) FROM hotspots) AS top_model_ids,
  (SELECT topK(5)(gpu_id) FROM hotspots) AS top_gpu_ids;

-- TEST: serving_views_do_not_reference_typed_raw_tables
-- Verifies API serving views do not directly reference raw/typed source tables.
-- Requirement: serving semantics must be sourced from silver/gold contracts (`fact_*`, `agg_*`, `dim_*`), not raw/typed ingestion tables.
WITH
  [
    'raw_streaming_events',
    'raw_ai_stream_status',
    'raw_stream_trace_events',
    'raw_stream_ingest_metrics',
    'raw_ai_stream_events',
    'raw_network_capabilities',
    'raw_network_capabilities_advertised',
    'raw_network_capabilities_model_constraints',
    'raw_network_capabilities_prices'
  ] AS forbidden_refs,
  views AS
  (
    SELECT
      name,
      lower(create_table_query) AS create_query
    FROM system.tables
    WHERE database = 'livepeer_analytics'
      AND match(name, '^v_api_')
  ),
  hits AS
  (
    SELECT
      v.name AS view_name,
      ref
    FROM views v
    ARRAY JOIN forbidden_refs AS ref
    WHERE position(v.create_query, ref) > 0
  )
SELECT
  toUInt64((SELECT count() FROM hits) > 0) AS failed_rows,
  (SELECT groupArrayDistinct(view_name) FROM hits) AS offending_views,
  (SELECT groupArrayDistinct(ref) FROM hits) AS offending_refs,
  (SELECT count() FROM views) AS views_checked;
