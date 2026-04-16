-- Migration 011: remove legacy session-attribution current compatibility state.

DROP TABLE IF EXISTS naap.canonical_session_attribution_current;
