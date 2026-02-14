# Data Flow and Processing

## Pipeline
1. Producers emit events to Kafka (`streaming-events`).
2. Flink quality gate validates, deduplicates, and routes invalid events to DLQ/quarantine.
3. Flink parses typed events and writes base typed tables to ClickHouse.
4. ClickHouse MVs build non-stateful silver facts.
5. Flink lifecycle operators consume typed streams + capability broadcast enrichment and emit stateful lifecycle facts.
6. ClickHouse rollups (`agg_*`) and API views (`v_api_*`) provide serving surfaces.

## Stateful Lifecycle Processing (Flink)
- Input signals:
  - `ai_stream_status`
  - `stream_trace`
  - `ai_stream_events`
- Broadcast enrichment:
  - `network_capabilities` snapshots by hot-wallet key.
- Outputs:
  - `fact_workflow_sessions`
  - `fact_workflow_session_segments`
  - `fact_workflow_param_updates`

## Segment and Marker Semantics
- Segment boundary (v1): orchestrator identity change only.
- Segment close: next segment start or explicit close edge.
- Param update marker: emitted from `ai_stream_events.type='params_update'`.
