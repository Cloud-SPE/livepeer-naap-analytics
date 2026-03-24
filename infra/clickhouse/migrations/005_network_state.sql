-- Migration 005: network state aggregate views (R1 — NET-001 through NET-004)
--
-- Source events: network_capabilities (daydream org, ~periodic full snapshots)
--
-- Design: agg_orch_state stores one row per (orch_address, event occurrence),
-- using ReplacingMergeTree(last_seen) so queries with FINAL return only the
-- latest state per orchestrator. The full raw capabilities JSON is stored in
-- raw_capabilities to avoid re-extracting deeply nested GPU/model/pricing data
-- at query time.
--
-- network_capabilities data field is a JSON array where each element is an
-- orchestrator object. The MV uses arrayJoin to produce one row per orch per
-- event batch.
--
-- Known risk: data field may be '[]' or 'null' for malformed events.
-- The WHERE clause guards against this.

CREATE TABLE IF NOT EXISTS naap.agg_orch_state
(
    orch_address      String,
    org               LowCardinality(String),
    -- local_address is the human-readable name; falls back to address if absent.
    name              String,
    uri               String,
    version           String,
    last_seen         DateTime64(3, 'UTC'),
    -- Full JSON of this orchestrator's entry in the capabilities array.
    -- Contains: hardware (GPUs), capabilities (models, constraints), pricing.
    -- The Go API extracts GPU count, VRAM, pipelines, and prices from this blob.
    raw_capabilities  String
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY orch_address
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_orch_state
TO naap.agg_orch_state
AS
SELECT
    lower(JSONExtractString(orch_json, 'address'))                        AS orch_address,
    org,
    -- Prefer local_address as the display name; fall back to address.
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    )                                                                      AS name,
    JSONExtractString(orch_json, 'uri')                                   AS uri,
    JSONExtractString(orch_json, 'version')                               AS version,
    event_ts                                                               AS last_seen,
    orch_json                                                              AS raw_capabilities
FROM (
    -- Unnest the capabilities array: one row per orchestrator per event batch.
    SELECT
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';
