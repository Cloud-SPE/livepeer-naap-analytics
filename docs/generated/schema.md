# Generated Schema

This reference is generated from the checked-in bootstrap baseline in [`v1.sql`](../../infra/clickhouse/bootstrap/v1.sql).

Regenerate this inventory with `make bootstrap-extract`. If the physical bootstrap changes, update [`v1.sql`](../../infra/clickhouse/bootstrap/v1.sql) first and rerun the generator in the same change.

The documented `raw_*`, `normalized_*`, `canonical_*`, `operational_*`, `api_base_*`, and `api_*` tiers are semantic/modeling guidance. This inventory is the supported physical schema, so it also includes infrastructure/runtime objects such as `accepted_raw_events`, `ignored_raw_events`, `kafka_*`, `resolver_*`, `agg_*`, metadata tables, and materialized views.

## Included Objects

| Object | Engine |
| --- | --- |
| `kafka_network_events` | `Kafka` |
| `kafka_streaming_events` | `Kafka` |
| `accepted_raw_events` | `ReplacingMergeTree` |
| `agg_discovery_latency_hourly` | `SummingMergeTree` |
| `agg_fps_hourly` | `SummingMergeTree` |
| `agg_orch_reliability_hourly` | `SummingMergeTree` |
| `agg_orch_state` | `ReplacingMergeTree` |
| `agg_payment_hourly` | `SummingMergeTree` |
| `agg_stream_hourly` | `SummingMergeTree` |
| `agg_stream_state` | `ReplacingMergeTree` |
| `agg_stream_status_samples` | `MergeTree` |
| `agg_webrtc_hourly` | `SummingMergeTree` |
| `canonical_streaming_demand_hourly_store` | `MergeTree` |
| `canonical_streaming_gpu_metrics_hourly_store` | `MergeTree` |
| `canonical_streaming_sla_hourly_store` | `MergeTree` |
| `canonical_streaming_sla_input_hourly_store` | `MergeTree` |
| `canonical_active_stream_state_latest_store` | `MergeTree` |
| `canonical_ai_batch_job_store` | `ReplacingMergeTree` |
| `canonical_byoc_job_store` | `ReplacingMergeTree` |
| `canonical_capability_hardware_inventory` | `ReplacingMergeTree` |
| `canonical_capability_hardware_inventory_by_snapshot` | `ReplacingMergeTree` |
| `canonical_capability_offer_inventory_store` | `ReplacingMergeTree` |
| `canonical_capability_pricing_inventory_store` | `ReplacingMergeTree` |
| `canonical_capability_snapshot_latest` | `AggregatingMergeTree` |
| `canonical_capability_snapshots_store` | `ReplacingMergeTree` |
| `canonical_orch_capability_intervals` | `ReplacingMergeTree` |
| `canonical_orch_capability_versions` | `ReplacingMergeTree` |
| `canonical_payment_links_store` | `ReplacingMergeTree` |
| `canonical_selection_attribution_current` | `ReplacingMergeTree` |
| `canonical_selection_attribution_decisions` | `MergeTree` |
| `canonical_selection_events` | `ReplacingMergeTree` |
| `canonical_session_current_store` | `ReplacingMergeTree` |
| `canonical_session_demand_input_current` | `ReplacingMergeTree` |
| `canonical_status_hours_store` | `MergeTree` |
| `canonical_status_samples_recent_store` | `MergeTree` |
| `gateway_metadata` | `ReplacingMergeTree` |
| `ignored_raw_events` | `ReplacingMergeTree` |
| `normalized_ai_stream_events` | `ReplacingMergeTree` |
| `normalized_ai_stream_status` | `ReplacingMergeTree` |
| `normalized_network_capabilities` | `ReplacingMergeTree` |
| `normalized_session_attribution_input_latest_store` | `AggregatingMergeTree` |
| `normalized_session_event_rollup_latest` | `AggregatingMergeTree` |
| `normalized_session_orchestrator_observation_rollup_latest` | `AggregatingMergeTree` |
| `normalized_session_status_hour_rollup` | `AggregatingMergeTree` |
| `normalized_session_status_rollup_latest` | `AggregatingMergeTree` |
| `normalized_session_trace_rollup_latest` | `AggregatingMergeTree` |
| `normalized_stream_trace` | `ReplacingMergeTree` |
| `orch_metadata` | `ReplacingMergeTree` |
| `resolver_backfill_runs` | `MergeTree` |
| `resolver_dead_letters` | `ReplacingMergeTree` |
| `resolver_dirty_orchestrators` | `ReplacingMergeTree` |
| `resolver_dirty_partitions` | `ReplacingMergeTree` |
| `resolver_dirty_windows` | `ReplacingMergeTree` |
| `resolver_dirty_selection_events` | `ReplacingMergeTree` |
| `resolver_dirty_sessions` | `ReplacingMergeTree` |
| `resolver_query_event_ids` | `MergeTree` |
| `resolver_query_identities` | `MergeTree` |
| `resolver_query_selection_event_ids` | `MergeTree` |
| `resolver_query_session_keys` | `MergeTree` |
| `resolver_query_window_slices` | `MergeTree` |
| `resolver_runs` | `MergeTree` |
| `resolver_repair_requests` | `ReplacingMergeTree` |
| `resolver_runtime_state` | `ReplacingMergeTree` |
| `resolver_window_claims` | `ReplacingMergeTree` |
| `mv_canonical_capability_hardware_inventory` | `MaterializedView` |
| `mv_canonical_capability_hardware_inventory_by_snapshot` | `MaterializedView` |
| `mv_canonical_capability_offer_inventory_store_builtin` | `MaterializedView` |
| `mv_canonical_capability_offer_inventory_store_hardware` | `MaterializedView` |
| `mv_canonical_capability_pricing_inventory_store_capability` | `MaterializedView` |
| `mv_canonical_capability_pricing_inventory_store_global` | `MaterializedView` |
| `mv_canonical_capability_snapshot_latest` | `MaterializedView` |
| `mv_canonical_capability_snapshots_store` | `MaterializedView` |
| `mv_discovery_latency_hourly` | `MaterializedView` |
| `mv_fps_hourly` | `MaterializedView` |
| `mv_ingest_network_events_accepted` | `MaterializedView` |
| `mv_ingest_network_events_ignored` | `MaterializedView` |
| `mv_ingest_streaming_events_accepted` | `MaterializedView` |
| `mv_ingest_streaming_events_ignored` | `MaterializedView` |
| `mv_normalized_ai_stream_events` | `MaterializedView` |
| `mv_normalized_ai_stream_status` | `MaterializedView` |
| `mv_normalized_network_capabilities` | `MaterializedView` |
| `mv_normalized_session_attribution_input_from_events` | `MaterializedView` |
| `mv_normalized_session_attribution_input_from_status` | `MaterializedView` |
| `mv_normalized_session_attribution_input_from_trace` | `MaterializedView` |
| `mv_normalized_session_event_rollup_latest` | `MaterializedView` |
| `mv_normalized_session_orch_observation_from_events` | `MaterializedView` |
| `mv_normalized_session_orch_observation_from_status` | `MaterializedView` |
| `mv_normalized_session_orch_observation_from_trace` | `MaterializedView` |
| `mv_normalized_session_status_hour_rollup` | `MaterializedView` |
| `mv_normalized_session_status_rollup_latest` | `MaterializedView` |
| `mv_normalized_session_trace_rollup_latest` | `MaterializedView` |
| `mv_normalized_stream_trace` | `MaterializedView` |
| `mv_orch_reliability_hourly` | `MaterializedView` |
| `mv_orch_state` | `MaterializedView` |
| `mv_payment_hourly` | `MaterializedView` |
| `mv_stream_hourly` | `MaterializedView` |
| `mv_stream_state` | `MaterializedView` |
| `mv_stream_status_samples` | `MaterializedView` |
| `mv_webrtc_hourly` | `MaterializedView` |
| `ignored_raw_event_diagnostics` | `View` |
| `raw_events` | `View` |

## Excluded From The V1 Bootstrap

- `schema_migrations` stays migration-path bookkeeping only.
- `events`, `typed_*`, and `mv_typed_*` are removed; dbt staging now parses directly from `accepted_raw_events`.
- `canonical_refresh_*` is removed from the supported schema surface.
- `canonical_session_latest_store` is removed with the hard cutover to `canonical_session_current`.
- `api_status_samples_store` and `api_active_stream_state_store` are not part of the supported serving spine.
- `raw_events` and `ignored_raw_event_diagnostics` stay in the bootstrap as supported operational compatibility views.
- `v_api_*` compatibility views are not part of the supported semantic contract.
