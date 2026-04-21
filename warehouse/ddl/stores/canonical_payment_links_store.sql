-- Canonical DDL for naap.canonical_payment_links_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- `make lint-store-ddl`.

CREATE TABLE IF NOT EXISTS naap.canonical_payment_links_store (`event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String), `gateway` String, `session_id` String, `request_id` String, `manifest_id` String, `pipeline_hint` String, `sender_address` String, `recipient_address` String, `orchestrator_url` String, `face_value_wei` UInt64, `price_wei_per_pixel` Float64, `win_prob` Float64, `num_tickets` UInt64, `canonical_session_key` Nullable(String), `link_method` LowCardinality(String), `link_status` LowCardinality(String), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = ReplacingMergeTree(refreshed_at) PARTITION BY (org, toYYYYMM(event_ts)) ORDER BY (org, toStartOfHour(event_ts), event_id) SETTINGS index_granularity = 8192;
