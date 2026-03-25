ALTER TABLE naap.typed_ai_stream_status
    MODIFY COLUMN last_error_ts Nullable(DateTime64(3, 'UTC'));
