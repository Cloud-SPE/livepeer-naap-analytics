-- Migration 048: hard-cutover lifecycle semantics.

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS requested_seen UInt8 DEFAULT 0
    AFTER last_seen;

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS selection_outcome LowCardinality(String) DEFAULT 'unknown'
    AFTER playable_seen;

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS excusal_reason LowCardinality(String) DEFAULT 'none'
    AFTER startup_outcome;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS requested_seen UInt8 DEFAULT 0
    AFTER model_id;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS selection_outcome LowCardinality(String) DEFAULT 'unknown'
    AFTER requested_seen;

ALTER TABLE naap.canonical_session_demand_input_current
    ADD COLUMN IF NOT EXISTS excusal_reason LowCardinality(String) DEFAULT 'none'
    AFTER startup_outcome;
