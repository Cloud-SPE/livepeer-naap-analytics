package replay

import (
	"context"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// ingestMVs names the materialized views that pull events out of the Kafka
// engine tables and into `accepted_raw_events` / `ignored_raw_events`.
// Detaching all four freezes both sides of the ingest split so the Kafka
// engine tables stop consuming (no MV reader → no poll) and fixture state
// remains pristine for the duration of the harness run.
//
// These names are stable in the bootstrap schema; if additional ingest
// topic families are added later, extend this list.
var ingestMVs = []string{
	"mv_ingest_network_events_accepted",
	"mv_ingest_network_events_ignored",
	"mv_ingest_streaming_events_accepted",
	"mv_ingest_streaming_events_ignored",
}

// PauseIngestion detaches every ingest MV so live Kafka traffic cannot
// contaminate the fixture state during a harness run. The paired
// ResumeIngestion reattaches them and preserves committed consumer-group
// offsets, so post-replay catch-up resumes from where we paused.
//
// Safe to call on a clean system. Idempotent at the granularity of one
// harness run — if an MV is already detached the DETACH here errors and
// we surface it (rather than silently skip) so the operator notices.
func PauseIngestion(ctx context.Context, conn ch.Conn, database string) error {
	for _, name := range ingestMVs {
		stmt := fmt.Sprintf("DETACH TABLE IF EXISTS %s.%s SYNC", database, name)
		if err := conn.Exec(ctx, stmt); err != nil {
			// If the table genuinely does not exist, DETACH IF EXISTS
			// returns success silently; any other error is a real problem.
			return fmt.Errorf("pause ingestion (%s): %w", name, err)
		}
	}
	return nil
}

// ResumeIngestion reattaches every ingest MV. ATTACH can race with a
// just-completed DETACH that has in-flight cleanup, so we retry up to five
// times on the well-known "TABLE_ALREADY_EXISTS" transient error.
//
// This must run even when the harness returns early — callers should use
// `defer ResumeIngestion(...)` on the pipeline entry so a failure in the
// load or checksum phase does not leave the stack with ingestion paused.
func ResumeIngestion(ctx context.Context, conn ch.Conn, database string) error {
	var lastErr error
	for _, name := range ingestMVs {
		stmt := fmt.Sprintf("ATTACH TABLE IF NOT EXISTS %s.%s", database, name)
		attached := false
		for attempt := 0; attempt < 5; attempt++ {
			err := conn.Exec(ctx, stmt)
			if err == nil {
				attached = true
				break
			}
			if !isTransientAttachError(err) {
				lastErr = fmt.Errorf("resume ingestion (%s): %w", name, err)
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(500*(attempt+1)) * time.Millisecond):
			}
		}
		if !attached && lastErr == nil {
			lastErr = fmt.Errorf("resume ingestion (%s): exhausted retries", name)
		}
	}
	return lastErr
}

// isTransientAttachError detects ClickHouse 57 ("TABLE_ALREADY_EXISTS")
// messages that occur when ATTACH races with the in-flight cleanup of a
// prior DETACH. The error surface is a text message, not a typed code, so
// we pattern-match against the stable substring.
func isTransientAttachError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "TABLE_ALREADY_EXISTS") ||
		strings.Contains(s, "still used by some query")
}
