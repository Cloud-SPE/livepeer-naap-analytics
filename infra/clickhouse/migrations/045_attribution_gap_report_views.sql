-- Migration 045: attribution gap diagnostics over the selection-centered spine.
--
-- These views are diagnostic only. They classify unresolved / ambiguous /
-- stale selection decisions into mutually exclusive buckets without changing
-- the resolver hot path.

DROP VIEW IF EXISTS naap.canonical_selection_attribution_gap_samples;
DROP VIEW IF EXISTS naap.canonical_selection_attribution_gap_report;

CREATE VIEW naap.canonical_selection_attribution_gap_report AS
WITH base AS (
    SELECT
        c.selection_event_id AS selection_event_id,
        c.org AS org,
        toDate(c.selection_ts) AS event_date,
        c.selection_ts AS selection_ts,
        c.attribution_status AS attribution_status,
        c.attribution_reason AS attribution_reason,
        lowerUTF8(ifNull(s.observed_orch_raw_address, '')) AS observed_orch_raw_address,
        lowerUTF8(ifNull(s.observed_orch_url, '')) AS observed_orch_url,
        lowerUTF8(ifNull(s.observed_pipeline_hint, '')) AS observed_pipeline_hint_raw,
        lowerUTF8(ifNull(s.observed_model_hint, '')) AS observed_model_hint,
        multiIf(
            lowerUTF8(ifNull(s.observed_pipeline_hint, '')) IN (
                'live-video-to-video',
                'text-to-image',
                'image-to-image',
                'text-to-video',
                'audio-to-text',
                'text-to-speech',
                'noop'
            ),
            lowerUTF8(ifNull(s.observed_pipeline_hint, '')),
            ''
        ) AS canonical_pipeline_hint
    FROM (
        SELECT * FROM naap.canonical_selection_attribution_current FINAL
    ) c
    INNER JOIN (
        SELECT * FROM naap.canonical_selection_events FINAL
    ) s USING (selection_event_id)
    WHERE c.attribution_status IN ('unresolved', 'ambiguous', 'stale')
),
interval_inventory AS (
    SELECT
        i.org AS org,
        i.interval_hash AS interval_hash,
        lowerUTF8(i.orch_address) AS orch_address,
        lowerUTF8(i.orch_uri_norm) AS orch_uri_norm,
        lowerUTF8(ifNull(v.local_address, '')) AS alias_address,
        i.valid_from_ts AS valid_from_ts,
        lowerUTF8(ifNull(i.canonical_pipeline, '')) AS canonical_pipeline,
        lowerUTF8(ifNull(i.canonical_model, '')) AS canonical_model
    FROM (
        SELECT * FROM naap.canonical_orch_capability_intervals FINAL
    ) i
    LEFT JOIN (
        SELECT * FROM naap.canonical_orch_capability_versions FINAL
    ) v
        ON v.org = i.org
       AND v.capability_version_id = i.capability_version_id
),
all_matches AS (
    SELECT
        b.selection_event_id,
        b.selection_ts,
        b.canonical_pipeline_hint,
        b.observed_model_hint,
        i.interval_hash,
        toUInt8(1) AS canonical_address_hit,
        toUInt8(0) AS alias_address_hit,
        toUInt8(0) AS uri_hit,
        toUInt8(
            (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
            OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
        ) AS time_window_hit,
        toUInt8(i.valid_from_ts < b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) > 600) AS stale_hit,
        toUInt8(b.canonical_pipeline_hint != '' AND i.canonical_pipeline = b.canonical_pipeline_hint) AS pipeline_hit,
        toUInt8(b.observed_model_hint != '' AND i.canonical_model = b.observed_model_hint) AS model_hit,
        toUInt8(
            (
                (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
                OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
            )
            AND (b.canonical_pipeline_hint = '' OR (i.canonical_pipeline != '' AND i.canonical_pipeline = b.canonical_pipeline_hint))
            AND (b.observed_model_hint = '' OR (i.canonical_model != '' AND i.canonical_model = b.observed_model_hint))
        ) AS compatible_hit
    FROM base b
    INNER JOIN interval_inventory i
        ON i.org = b.org
       AND b.observed_orch_raw_address != ''
       AND i.orch_address = b.observed_orch_raw_address

    UNION ALL

    SELECT
        b.selection_event_id,
        b.selection_ts,
        b.canonical_pipeline_hint,
        b.observed_model_hint,
        i.interval_hash,
        toUInt8(0) AS canonical_address_hit,
        toUInt8(1) AS alias_address_hit,
        toUInt8(0) AS uri_hit,
        toUInt8(
            (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
            OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
        ) AS time_window_hit,
        toUInt8(i.valid_from_ts < b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) > 600) AS stale_hit,
        toUInt8(b.canonical_pipeline_hint != '' AND i.canonical_pipeline = b.canonical_pipeline_hint) AS pipeline_hit,
        toUInt8(b.observed_model_hint != '' AND i.canonical_model = b.observed_model_hint) AS model_hit,
        toUInt8(
            (
                (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
                OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
            )
            AND (b.canonical_pipeline_hint = '' OR (i.canonical_pipeline != '' AND i.canonical_pipeline = b.canonical_pipeline_hint))
            AND (b.observed_model_hint = '' OR (i.canonical_model != '' AND i.canonical_model = b.observed_model_hint))
        ) AS compatible_hit
    FROM base b
    INNER JOIN interval_inventory i
        ON i.org = b.org
       AND b.observed_orch_raw_address != ''
       AND i.alias_address = b.observed_orch_raw_address

    UNION ALL

    SELECT
        b.selection_event_id,
        b.selection_ts,
        b.canonical_pipeline_hint,
        b.observed_model_hint,
        i.interval_hash,
        toUInt8(0) AS canonical_address_hit,
        toUInt8(0) AS alias_address_hit,
        toUInt8(1) AS uri_hit,
        toUInt8(
            (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
            OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
        ) AS time_window_hit,
        toUInt8(i.valid_from_ts < b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) > 600) AS stale_hit,
        toUInt8(b.canonical_pipeline_hint != '' AND i.canonical_pipeline = b.canonical_pipeline_hint) AS pipeline_hit,
        toUInt8(b.observed_model_hint != '' AND i.canonical_model = b.observed_model_hint) AS model_hit,
        toUInt8(
            (
                (i.valid_from_ts <= b.selection_ts AND dateDiff('second', i.valid_from_ts, b.selection_ts) <= 600)
                OR (i.valid_from_ts > b.selection_ts AND dateDiff('second', b.selection_ts, i.valid_from_ts) <= 30)
            )
            AND (b.canonical_pipeline_hint = '' OR (i.canonical_pipeline != '' AND i.canonical_pipeline = b.canonical_pipeline_hint))
            AND (b.observed_model_hint = '' OR (i.canonical_model != '' AND i.canonical_model = b.observed_model_hint))
        ) AS compatible_hit
    FROM base b
    INNER JOIN interval_inventory i
        ON i.org = b.org
       AND b.observed_orch_url != ''
       AND i.orch_uri_norm = b.observed_orch_url
),
interval_matches AS (
    SELECT
        selection_event_id,
        uniqExactIf(interval_hash, canonical_address_hit = 1) AS canonical_address_match_count,
        uniqExactIf(interval_hash, alias_address_hit = 1) AS alias_address_match_count,
        uniqExactIf(interval_hash, uri_hit = 1) AS uri_match_count,
        uniqExactIf(interval_hash, time_window_hit = 1) AS time_window_match_count,
        uniqExactIf(interval_hash, stale_hit = 1) AS stale_match_count,
        uniqExactIf(interval_hash, pipeline_hit = 1) AS pipeline_match_count,
        uniqExactIf(interval_hash, model_hit = 1) AS model_match_count,
        uniqExactIf(interval_hash, compatible_hit = 1) AS compatible_candidate_count
    FROM all_matches
    GROUP BY selection_event_id
)
SELECT
    b.selection_event_id,
    b.org,
    b.event_date,
    b.selection_ts,
    b.attribution_status,
    b.attribution_reason,
    b.observed_orch_raw_address,
    b.observed_orch_url,
    b.observed_pipeline_hint_raw,
    b.canonical_pipeline_hint,
    b.observed_model_hint,
    ifNull(m.canonical_address_match_count, 0) AS canonical_address_match_count,
    ifNull(m.alias_address_match_count, 0) AS alias_address_match_count,
    ifNull(m.uri_match_count, 0) AS uri_match_count,
    ifNull(m.time_window_match_count, 0) AS time_window_match_count,
    ifNull(m.stale_match_count, 0) AS stale_match_count,
    ifNull(m.pipeline_match_count, 0) AS pipeline_match_count,
    ifNull(m.model_match_count, 0) AS model_match_count,
    ifNull(m.compatible_candidate_count, 0) AS compatible_candidate_count,
    multiIf(
        b.observed_orch_raw_address = '' AND b.observed_orch_url = '', 'no_usable_selection_identity',
        b.attribution_status = 'ambiguous', 'multi_candidate_ambiguity_after_canonical_collapse',
        ifNull(m.canonical_address_match_count, 0) = 0 AND ifNull(m.alias_address_match_count, 0) = 0 AND ifNull(m.uri_match_count, 0) = 0, 'no_capability_snapshots_for_identity',
        ifNull(m.time_window_match_count, 0) = 0 AND ifNull(m.stale_match_count, 0) > 0, 'only_stale_candidates_outside_window',
        ifNull(m.uri_match_count, 0) > 0 AND ifNull(m.canonical_address_match_count, 0) = 0 AND ifNull(m.alias_address_match_count, 0) = 0, 'uri_proxy_match_exists_but_canonical_address_mapping_fails',
        (ifNull(m.canonical_address_match_count, 0) > 0 OR ifNull(m.alias_address_match_count, 0) > 0) AND ifNull(m.uri_match_count, 0) = 0, 'canonical_address_match_exists_but_uri_mapping_fails',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0
            AND b.canonical_pipeline_hint = ''
            AND b.observed_model_hint != ''
            AND ifNull(m.model_match_count, 0) = 0, 'model_mismatch_only',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0
            AND b.canonical_pipeline_hint != ''
            AND ifNull(m.pipeline_match_count, 0) = 0
            AND b.observed_model_hint = '', 'pipeline_mismatch_only',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0
            AND b.canonical_pipeline_hint != ''
            AND b.observed_model_hint != ''
            AND ifNull(m.pipeline_match_count, 0) = 0
            AND ifNull(m.model_match_count, 0) = 0, 'both_pipeline_and_model_mismatch',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0
            AND b.canonical_pipeline_hint != ''
            AND ifNull(m.pipeline_match_count, 0) > 0
            AND b.observed_model_hint != ''
            AND ifNull(m.model_match_count, 0) = 0, 'model_mismatch_only',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0
            AND b.canonical_pipeline_hint != ''
            AND ifNull(m.pipeline_match_count, 0) = 0
            AND (b.observed_model_hint = '' OR ifNull(m.model_match_count, 0) > 0), 'pipeline_mismatch_only',
        ifNull(m.time_window_match_count, 0) > 0
            AND ifNull(m.compatible_candidate_count, 0) = 0, 'intervalization_produced_candidates_but_none_survived_compatibility',
        'unclassified_gap'
    ) AS gap_bucket
FROM base b
LEFT JOIN interval_matches m USING (selection_event_id);

CREATE VIEW naap.canonical_selection_attribution_gap_samples AS
SELECT *
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY gap_bucket
            ORDER BY org, event_date, selection_ts, selection_event_id
        ) AS sample_rank
    FROM naap.canonical_selection_attribution_gap_report
)
WHERE sample_rank <= 20
ORDER BY gap_bucket, sample_rank;
