-- Canonical DDL for naap.normalized_session_attribution_input_latest_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- `make lint-store-ddl`.

CREATE TABLE IF NOT EXISTS naap.normalized_session_attribution_input_latest_store (`org` LowCardinality(String), `canonical_session_key` String, `status_pipeline_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `event_pipeline_hint_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `trace_pipeline_id_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `trace_raw_pipeline_hint_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `observed_orch_address_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `observed_orch_url_state` AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8), `observed_orch_address_uniq` AggregateFunction(uniqExactIf, String, UInt8), `observed_orch_url_uniq` AggregateFunction(uniqExactIf, String, UInt8), `last_seen_state` AggregateFunction(max, DateTime64(3, 'UTC')), `swap_count_state` AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree PARTITION BY org ORDER BY (org, canonical_session_key) SETTINGS index_granularity = 8192;
