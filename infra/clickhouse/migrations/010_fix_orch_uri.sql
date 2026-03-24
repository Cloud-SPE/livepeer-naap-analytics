-- Migration 010: fix orch_uri extraction in mv_orch_state
--
-- Migration 005 extracted `uri` from the orch capabilities JSON, but the actual
-- field name is `orch_uri`. This caused the `uri` column in agg_orch_state to
-- always be empty.
--
-- Fix: drop and recreate the MV with the correct field name.
-- Existing data is cleared and repopulates within minutes from live events.
-- Discovered during Phase 3 live data inspection (2026-03-24).

DROP VIEW IF EXISTS naap.mv_orch_state;

ALTER TABLE naap.agg_orch_state
    UPDATE uri = JSONExtractString(raw_capabilities, 'orch_uri') WHERE 1=1;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_orch_state
TO naap.agg_orch_state
AS
SELECT
    lower(JSONExtractString(orch_json, 'address'))                        AS orch_address,
    org,
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    )                                                                      AS name,
    -- Corrected: field is orch_uri not uri
    JSONExtractString(orch_json, 'orch_uri')                              AS uri,
    JSONExtractString(orch_json, 'version')                               AS version,
    event_ts                                                               AS last_seen,
    orch_json                                                              AS raw_capabilities
FROM (
    SELECT
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';
