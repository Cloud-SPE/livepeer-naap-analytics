-- Migration 020: grant DDL privileges required by dbt to naap_writer
--
-- PROBLEM
-- ───────
-- dbt drops and recreates views/tables on every run. Migration 001 only granted
-- CREATE TABLE and CREATE VIEW to naap_writer but not DROP TABLE or DROP VIEW.
-- As a result the warehouse stack failed with ACCESS_DENIED (code 497) on every
-- staging model when run on infra2.
--
-- FIX
-- ───
-- Grant the missing DROP privileges so dbt can perform atomic model swaps.
-- naap_writer still cannot DROP the database itself — only objects inside naap.

GRANT DROP VIEW ON naap.* TO naap_writer;
GRANT DROP TABLE ON naap.* TO naap_writer;
