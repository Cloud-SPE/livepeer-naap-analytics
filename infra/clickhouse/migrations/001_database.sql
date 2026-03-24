-- Migration 001: database and user setup
-- Creates the naap database and application users.
-- The naap_admin user is created by docker-compose env vars; this migration
-- creates the lower-privilege application users.

CREATE DATABASE IF NOT EXISTS naap;

-- naap_writer: used by ClickHouse Kafka engine and materialized views (internal).
CREATE USER IF NOT EXISTS naap_writer
    IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_WRITER_PASSWORD}'
    DEFAULT DATABASE naap;

GRANT INSERT, SELECT, CREATE TABLE, CREATE VIEW ON naap.* TO naap_writer;

-- naap_reader: used by the Go API (read-only queries).
CREATE USER IF NOT EXISTS naap_reader
    IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_READER_PASSWORD}'
    DEFAULT DATABASE naap;

GRANT SELECT ON naap.* TO naap_reader;
