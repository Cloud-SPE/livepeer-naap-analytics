-- Migration 029: refresh-aligned projection for canonical status samples.
--
-- The refresh worker filters canonical status samples by refresh_run_id and
-- canonical_session_key. The base table is ordered by event_id, which is a poor
-- match for those chunked refresh scans. This projection gives ClickHouse a
-- run/session-oriented layout for downstream refresh models such as
-- refresh_api_network_demand_by_org and refresh_api_active_stream_state.

ALTER TABLE naap.canonical_status_samples_recent_store
ADD PROJECTION IF NOT EXISTS by_refresh_run_session
(
    SELECT
        refresh_run_id,
        canonical_session_key,
        sample_ts,
        gateway,
        output_fps,
        org,
        stream_id,
        request_id,
        orch_address,
        pipeline,
        model_id,
        attribution_status,
        attribution_reason,
        state,
        input_fps,
        e2e_latency_ms,
        event_id
    ORDER BY (refresh_run_id, canonical_session_key, sample_ts)
);
