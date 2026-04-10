package resolver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
)

type repo struct {
	cfg   *config.Config
	log   *zap.Logger
	conn  driver.Conn
	owner string
}

type backfillPartition struct {
	Org       string
	EventDate time.Time
	Start     time.Time
	End       time.Time
}

type sessionKeyRef struct {
	Org        string
	SessionKey string
}

type windowSliceRef struct {
	Org         string
	WindowStart time.Time
}

func newRepo(cfg *config.Config, log *zap.Logger, ownerID string) (*repo, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: ch.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseWriterUser,
			Password: cfg.ClickHouseWriterPassword,
		},
		Settings: ch.Settings{
			"max_execution_time": int(cfg.ClickHouseTimeout.Seconds()),
		},
		DialTimeout:      5 * time.Second,
		MaxOpenConns:     4,
		MaxIdleConns:     2,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: ch.ConnOpenInOrder,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return &repo{cfg: cfg, log: log, conn: conn, owner: ownerID}, nil
}

func (r *repo) close() error {
	if r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

func (r *repo) ping(ctx context.Context) error {
	return r.conn.Ping(ctx)
}

func (r *repo) currentUTC(ctx context.Context) (time.Time, error) {
	var ts time.Time
	if err := r.conn.QueryRow(ctx, "SELECT now64(3)").Scan(&ts); err != nil {
		return time.Time{}, fmt.Errorf("query current time: %w", err)
	}
	return ts.UTC(), nil
}

func (r *repo) acceptedRawBounds(ctx context.Context, org string, excludedPrefixes []string) (*time.Time, *time.Time, error) {
	orgClause, orgArgs := orgPredicate("org", org, excludedPrefixes)
	query := `
		SELECT min(event_ts), max(event_ts)
		FROM naap.accepted_raw_events
		WHERE ` + orgClause
	args := append([]any{}, orgArgs...)
	var minTS, maxTS sql.NullTime
	if err := r.conn.QueryRow(ctx, query, args...).Scan(&minTS, &maxTS); err != nil {
		return nil, nil, fmt.Errorf("query accepted raw bounds: %w", err)
	}
	var minPtr, maxPtr *time.Time
	if minTS.Valid {
		ts := minTS.Time.UTC()
		minPtr = &ts
	}
	if maxTS.Valid {
		ts := maxTS.Time.UTC()
		maxPtr = &ts
	}
	return minPtr, maxPtr, nil
}

func (r *repo) dirtyScanWatermark(ctx context.Context, stateKey string) (*dirtyScanWatermark, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT last_ingested_at, last_event_id
		FROM naap.resolver_runtime_state FINAL
		WHERE state_key = ?
		LIMIT 1
	`, stateKey)
	if err != nil {
		return nil, fmt.Errorf("query dirty scan watermark: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var mark dirtyScanWatermark
	if err := rows.Scan(&mark.IngestedAt, &mark.EventID); err != nil {
		return nil, fmt.Errorf("scan dirty scan watermark: %w", err)
	}
	mark.IngestedAt = mark.IngestedAt.UTC()
	return &mark, rows.Err()
}

func (r *repo) upsertDirtyScanWatermark(ctx context.Context, stateKey string, watermark dirtyScanWatermark) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.resolver_runtime_state
		(
			state_key, last_ingested_at, last_event_id, updated_at
		)
		VALUES (?, ?, ?, ?)
	`, stateKey, watermark.IngestedAt.UTC(), watermark.EventID, time.Now().UTC())
}

func acceptedRawDirtyScanPredicate() string {
	return `event_type IN (
		'stream_trace', 'ai_stream_status', 'ai_stream_events', 'network_capabilities',
		'ai_batch_request', 'ai_llm_request',
		'job_gateway', 'job_orchestrator', 'worker_lifecycle'
	)`
}

func dirtyWatermarkPredicate(watermark *dirtyScanWatermark) (string, []any) {
	if watermark == nil || watermark.EventID == "" || watermark.IngestedAt.IsZero() {
		return "1 = 1", nil
	}
	return "(ingested_at > ? OR (ingested_at = ? AND event_id > ?))", []any{
		watermark.IngestedAt.UTC(),
		watermark.IngestedAt.UTC(),
		watermark.EventID,
	}
}

func (r *repo) scanDirtyAcceptedRawPartitions(ctx context.Context, org string, excludedPrefixes []string, historicalCutoff time.Time, watermark *dirtyScanWatermark) ([]backfillPartition, *dirtyScanWatermark, error) {
	orgClause, orgArgs := orgPredicate("org", org, excludedPrefixes)
	watermarkClause, watermarkArgs := dirtyWatermarkPredicate(watermark)

	maxArgs := append([]any{}, orgArgs...)
	maxArgs = append(maxArgs, watermarkArgs...)
	maxRows, err := r.conn.Query(ctx, `
		SELECT ingested_at, event_id
		FROM naap.accepted_raw_events
		WHERE `+acceptedRawDirtyScanPredicate()+`
		  AND `+orgClause+`
		  AND `+watermarkClause+`
		ORDER BY ingested_at DESC, event_id DESC
		LIMIT 1
	`, maxArgs...)
	if err != nil {
		return nil, watermark, fmt.Errorf("query dirty accepted raw watermark: %w", err)
	}
	defer maxRows.Close()

	var nextWatermark *dirtyScanWatermark
	if maxRows.Next() {
		var mark dirtyScanWatermark
		if err := maxRows.Scan(&mark.IngestedAt, &mark.EventID); err != nil {
			return nil, watermark, fmt.Errorf("scan dirty accepted raw watermark: %w", err)
		}
		mark.IngestedAt = mark.IngestedAt.UTC()
		nextWatermark = &mark
	}
	if err := maxRows.Err(); err != nil {
		return nil, watermark, err
	}
	if nextWatermark == nil {
		return nil, watermark, nil
	}

	args := []any{historicalCutoff.UTC()}
	args = append(args, orgArgs...)
	args = append(args, watermarkArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT org, toDate(event_ts) AS event_date
		FROM naap.accepted_raw_events
		WHERE event_ts < ?
		  AND `+acceptedRawDirtyScanPredicate()+`
		  AND `+orgClause+`
		  AND `+watermarkClause+`
		GROUP BY org, event_date
		ORDER BY org, event_date
	`, args...)
	if err != nil {
		return nil, nextWatermark, fmt.Errorf("query dirty accepted raw partitions: %w", err)
	}
	defer rows.Close()

	var parts []backfillPartition
	for rows.Next() {
		var part backfillPartition
		if err := rows.Scan(&part.Org, &part.EventDate); err != nil {
			return nil, nextWatermark, fmt.Errorf("scan dirty accepted raw partition: %w", err)
		}
		part.EventDate = truncateUTCDate(part.EventDate)
		part.Start = part.EventDate
		part.End = part.EventDate.Add(24 * time.Hour)
		parts = append(parts, part)
	}
	return parts, nextWatermark, rows.Err()
}

func (r *repo) pendingDirtyPartitionCount(ctx context.Context, org string, excludedPrefixes []string) (uint64, error) {
	orgClause, orgArgs := orgPredicate("ifNull(org, '')", org, excludedPrefixes)
	var count uint64
	if err := r.conn.QueryRow(ctx, `
		SELECT count()
		FROM naap.resolver_dirty_partitions FINAL
		WHERE status = 'pending'
		  AND `+orgClause+`
	`, orgArgs...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending dirty partitions: %w", err)
	}
	return count, nil
}

func (r *repo) nextPendingDirtyPartition(ctx context.Context, org string, excludedPrefixes []string) (dirtyPartition, bool, error) {
	orgClause, orgArgs := orgPredicate("ifNull(org, '')", org, excludedPrefixes)
	rows, err := r.conn.Query(ctx, `
		SELECT
			ifNull(org, '') AS org,
			event_date,
			status,
			reason,
			first_dirty_at,
			last_dirty_at,
			ifNull(claim_owner, '') AS claim_owner,
			lease_expires_at,
			attempt_count,
			ifNull(last_error_summary, '') AS last_error_summary,
			updated_at
		FROM naap.resolver_dirty_partitions FINAL
		WHERE status = 'pending'
		  AND `+orgClause+`
		ORDER BY last_dirty_at, event_date, org
		LIMIT 1
	`, orgArgs...)
	if err != nil {
		return dirtyPartition{}, false, fmt.Errorf("query next pending dirty partition: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return dirtyPartition{}, false, nil
	}
	part, err := scanDirtyPartition(rows)
	if err != nil {
		return dirtyPartition{}, false, err
	}
	return part, true, rows.Err()
}

func (r *repo) successfulBackfillPartitions(ctx context.Context, org string, excludedPrefixes []string) (map[string]struct{}, error) {
	orgClause, orgArgs := orgPredicate("org", org, excludedPrefixes)
	query := `
		SELECT DISTINCT org, event_date
		FROM naap.resolver_backfill_runs
		WHERE status = 'success' AND ` + orgClause
	rows, err := r.conn.Query(ctx, query, orgArgs...)
	if err != nil {
		return nil, fmt.Errorf("query successful backfill partitions: %w", err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var partOrg string
		var eventDate time.Time
		if err := rows.Scan(&partOrg, &eventDate); err != nil {
			return nil, fmt.Errorf("scan successful backfill partition: %w", err)
		}
		out[partOrg+"|"+eventDate.UTC().Format("2006-01-02")] = struct{}{}
	}
	return out, rows.Err()
}

func orgPredicate(column, org string, excludedPrefixes []string) (string, []any) {
	clause := "(? = '' OR " + column + " = ?)"
	args := []any{org, org}
	for _, prefix := range excludedPrefixes {
		if prefix == "" {
			continue
		}
		clause += " AND NOT startsWith(" + column + ", ?)"
		args = append(args, prefix)
	}
	return clause, args
}

func canonicalStreamTracePredicate(alias string) string {
	return alias + `.trace_type IN (
		'gateway_receive_stream_request',
		'gateway_ingest_stream_closed',
		'gateway_send_first_ingest_segment',
		'gateway_receive_first_processed_segment',
		'gateway_receive_few_processed_segments',
		'gateway_receive_first_data_segment',
		'gateway_no_orchestrators_available',
		'orchestrator_swap'
	)`
}

func (r *repo) listBackfillPartitions(ctx context.Context, req RunRequest) ([]backfillPartition, error) {
	if req.Start == nil || req.End == nil {
		return nil, fmt.Errorf("backfill requires start and end")
	}
	orgClause, orgArgs := orgPredicate("org", req.Org, req.ExcludedOrgPrefixes)
	args := []any{
		req.Start.UTC(), req.End.UTC(),
		req.Start.UTC(), req.End.UTC(),
		req.Start.UTC(), req.End.UTC(),
		req.Start.UTC(), req.End.UTC(),
	}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT
			org,
			toDate(event_ts) AS event_date
		FROM (
			SELECT org, event_ts FROM naap.normalized_stream_trace
			WHERE event_ts >= ? AND event_ts < ?
			UNION ALL
			SELECT org, event_ts FROM naap.normalized_ai_stream_status
			WHERE event_ts >= ? AND event_ts < ?
			UNION ALL
			SELECT org, event_ts FROM naap.normalized_ai_stream_events
			WHERE event_ts >= ? AND event_ts < ?
			UNION ALL
			SELECT org, event_ts FROM naap.normalized_network_capabilities
			WHERE event_ts >= ? AND event_ts < ?
		)
		WHERE `+orgClause+`
		GROUP BY org, event_date
		ORDER BY org, event_date
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list backfill partitions: %w", err)
	}
	defer rows.Close()

	var out []backfillPartition
	for rows.Next() {
		var part backfillPartition
		if err := rows.Scan(&part.Org, &part.EventDate); err != nil {
			return nil, fmt.Errorf("scan backfill partition: %w", err)
		}
		part.EventDate = part.EventDate.UTC()
		part.Start = part.EventDate
		part.End = part.EventDate.Add(24 * time.Hour)
		if part.Start.Before(req.Start.UTC()) {
			part.Start = req.Start.UTC()
		}
		if part.End.After(req.End.UTC()) {
			part.End = req.End.UTC()
		}
		out = append(out, part)
	}
	return out, rows.Err()
}

func (r *repo) fetchTouchedSessionKeys(ctx context.Context, spec WindowSpec, includeCapabilityRepair bool) (map[string]string, error) {
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("fetch touched session keys requires bounded window")
	}
	out := make(map[string]string)
	queries := []string{
		`
			SELECT DISTINCT org, canonical_session_key
			FROM naap.normalized_stream_trace
			WHERE canonical_session_key != '' AND event_ts >= ? AND event_ts < ?
			  AND ` + canonicalStreamTracePredicate("normalized_stream_trace") + `
			  AND %s
		`,
		`
			SELECT DISTINCT org, canonical_session_key
			FROM naap.normalized_ai_stream_status
			WHERE canonical_session_key != '' AND event_ts >= ? AND event_ts < ?
			  AND %s
		`,
		`
			SELECT DISTINCT org, canonical_session_key
			FROM naap.normalized_ai_stream_events
			WHERE canonical_session_key != '' AND event_ts >= ? AND event_ts < ?
			  AND %s
		`,
	}
	for _, queryTemplate := range queries {
		orgClause, orgArgs := orgPredicate("org", spec.Org, spec.ExcludedOrgPrefixes)
		query := fmt.Sprintf(queryTemplate, orgClause)
		args := []any{spec.Start.UTC(), spec.End.UTC()}
		args = append(args, orgArgs...)
		if err := r.collectTouchedSessionKeys(ctx, out, query, args...); err != nil {
			return nil, fmt.Errorf("fetch touched sessions: %w", err)
		}
	}
	if !includeCapabilityRepair {
		return out, nil
	}
	identities, err := r.fetchCapabilityRepairIdentities(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("fetch touched sessions capability identities: %w", err)
	}
	if len(identities) == 0 {
		return out, nil
	}
	queryID, err := r.stageIdentities(ctx, identities)
	if err != nil {
		return nil, fmt.Errorf("stage touched-session repair identities: %w", err)
	}
	if queryID == "" {
		return out, nil
	}
	selectionOrgClause, selectionOrgArgs := orgPredicate("s.org", spec.Org, spec.ExcludedOrgPrefixes)
	repairQuery := `
		SELECT DISTINCT s.org, s.canonical_session_key
		FROM naap.canonical_selection_events s
		WHERE s.selection_ts >= ? AND s.selection_ts < ?
		  AND ` + selectionOrgClause + `
		  AND (
				lowerUTF8(ifNull(s.observed_orch_raw_address, '')) IN (
					SELECT identity FROM naap.resolver_query_identities WHERE query_id = ?
				)
				OR lowerUTF8(ifNull(s.observed_orch_url, '')) IN (
					SELECT identity FROM naap.resolver_query_identities WHERE query_id = ?
				)
		      )
	`
	args := []any{spec.Start.UTC().Add(-30 * time.Second), spec.End.UTC().Add(10 * time.Minute)}
	args = append(args, selectionOrgArgs...)
	args = append(args, queryID, queryID)
	if err := r.collectTouchedSessionKeys(ctx, out, repairQuery, args...); err != nil {
		return nil, fmt.Errorf("fetch touched sessions repair selections: %w", err)
	}
	return out, nil
}

func (r *repo) collectTouchedSessionKeys(ctx context.Context, out map[string]string, query string, args ...any) error {
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var org, key string
		if err := rows.Scan(&org, &key); err != nil {
			return fmt.Errorf("scan touched session: %w", err)
		}
		out[key] = org
	}
	return rows.Err()
}

func (r *repo) fetchCapabilityRepairIdentities(ctx context.Context, spec WindowSpec) ([]string, error) {
	orgClause, orgArgs := orgPredicate("org", spec.Org, spec.ExcludedOrgPrefixes)
	args := []any{spec.Start.UTC(), spec.End.UTC()}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT identity
		FROM (
			SELECT lowerUTF8(ifNull(orch_address, '')) AS identity
			FROM naap.normalized_network_capabilities
			WHERE event_ts >= ? AND event_ts < ?
			  AND `+orgClause+`
			  AND lowerUTF8(ifNull(orch_address, '')) != ''

			UNION DISTINCT

			SELECT lowerUTF8(ifNull(orch_uri_norm, '')) AS identity
			FROM naap.normalized_network_capabilities
			WHERE event_ts >= ? AND event_ts < ?
			  AND `+orgClause+`
			  AND lowerUTF8(ifNull(orch_uri_norm, '')) != ''
		)
	`, append(args, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var identities []string
	for rows.Next() {
		var identity string
		if err := rows.Scan(&identity); err != nil {
			return nil, err
		}
		identities = append(identities, identity)
	}
	return identities, rows.Err()
}

func (r *repo) fetchSelectionCandidates(ctx context.Context, spec WindowSpec) ([]selectionCandidate, error) {
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("selection candidate fetch requires bounded window")
	}
	orgClause, orgArgs := orgPredicate("org", spec.Org, spec.ExcludedOrgPrefixes)
	args := []any{
		spec.Start.UTC(), spec.End.UTC(),
		spec.Start.UTC(), spec.End.UTC(),
		spec.Start.UTC(), spec.End.UTC(),
	}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT
			c.org,
			c.canonical_session_key,
			c.event_id,
			c.event_type,
			c.event_ts,
			'' AS source_topic,
			toInt32(0) AS source_partition,
			toInt64(0) AS source_offset,
			c.source_priority,
			c.orch_address,
			c.orch_url,
			c.pipeline_hint,
			c.model_hint,
			c.explicit_swap
		FROM (
			SELECT
				org,
				canonical_session_key,
				event_id,
				event_type,
				event_ts,
					source_priority,
					orch_address,
					orch_url,
					pipeline_hint,
					model_hint,
					explicit_swap,
					candidate_identity,
				row_number() OVER candidate_order AS candidate_rank,
				lagInFrame(candidate_identity, 1, '') OVER candidate_order AS previous_candidate_identity
			FROM (
				SELECT
					org,
					canonical_session_key,
					event_id,
					event_type,
					event_ts,
					source_priority,
					orch_address,
					orch_url,
					pipeline_hint,
					model_hint,
					explicit_swap,
					if(orch_address != '', lowerUTF8(trimBoth(orch_address)), lowerUTF8(trimBoth(orch_url))) AS candidate_identity
				FROM (
					SELECT
						t.org AS org,
						t.canonical_session_key AS canonical_session_key,
						t.event_id AS event_id,
						'stream_trace' AS event_type,
						t.event_ts AS event_ts,
						toUInt8(1) AS source_priority,
						ifNull(t.orch_raw_address, '') AS orch_address,
						ifNull(t.orch_url, '') AS orch_url,
						ifNull(t.pipeline_id, '') AS pipeline_hint,
						cast('', 'String') AS model_hint,
						toUInt8(t.trace_type = 'orchestrator_swap') AS explicit_swap
					FROM naap.normalized_stream_trace t
					WHERE t.canonical_session_key != ''
					  AND t.event_ts >= ? AND t.event_ts < ?
					  AND `+canonicalStreamTracePredicate("t")+`
					UNION ALL
					SELECT
						s.org AS org,
						s.canonical_session_key AS canonical_session_key,
						s.event_id AS event_id,
						'ai_stream_status' AS event_type,
						s.event_ts AS event_ts,
						toUInt8(2) AS source_priority,
						ifNull(s.orch_raw_address, '') AS orch_address,
						ifNull(s.orch_url, '') AS orch_url,
						cast('', 'String') AS pipeline_hint,
						ifNull(s.raw_pipeline_hint, '') AS model_hint,
						toUInt8(0) AS explicit_swap
					FROM naap.normalized_ai_stream_status s
					WHERE s.canonical_session_key != ''
					  AND s.event_ts >= ? AND s.event_ts < ?
					UNION ALL
					SELECT
						a.org AS org,
						a.canonical_session_key AS canonical_session_key,
						a.event_id AS event_id,
						'ai_stream_events' AS event_type,
						a.event_ts AS event_ts,
						toUInt8(3) AS source_priority,
						ifNull(a.orch_raw_address, '') AS orch_address,
						ifNull(a.orch_url, '') AS orch_url,
						cast('', 'String') AS pipeline_hint,
						ifNull(a.raw_pipeline_hint, '') AS model_hint,
						toUInt8(positionCaseInsensitive(a.event_name, 'swap') > 0) AS explicit_swap
					FROM naap.normalized_ai_stream_events a
					WHERE a.canonical_session_key != ''
					  AND a.event_ts >= ? AND a.event_ts < ?
				)
				WHERE `+orgClause+`
			)
			WINDOW candidate_order AS (
				PARTITION BY org, canonical_session_key
				ORDER BY event_ts, source_priority, event_id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			)
		) c
		WHERE c.candidate_rank = 1
		   OR c.explicit_swap = 1
		   OR c.candidate_identity != c.previous_candidate_identity
		ORDER BY
			c.org,
			c.canonical_session_key,
			c.event_ts,
			c.source_priority,
			toInt32(0),
			toInt64(0),
			c.event_id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch selection candidates: %w", err)
	}
	defer rows.Close()

	var out []selectionCandidate
	for rows.Next() {
		var row selectionCandidate
		if err := rows.Scan(
			&row.Org,
			&row.SessionKey,
			&row.EventID,
			&row.EventType,
			&row.EventTS,
			&row.SourceTopic,
			&row.SourcePart,
			&row.SourceOffset,
			&row.SourcePriority,
			&row.OrchAddress,
			&row.OrchURL,
			&row.PipelineHint,
			&row.ModelHint,
			&row.ExplicitSwap,
		); err != nil {
			return nil, fmt.Errorf("scan selection candidate: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	lineage, err := r.fetchEventLineage(ctx, selectionCandidateEventIDs(out))
	if err != nil {
		return nil, fmt.Errorf("fetch event lineage for selection candidates: %w", err)
	}
	for idx := range out {
		if info, ok := lineage[out[idx].EventID]; ok {
			out[idx].SourceTopic = info.SourceTopic
			out[idx].SourcePart = info.SourcePart
			out[idx].SourceOffset = info.SourceOffset
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if left.Org != right.Org {
			return left.Org < right.Org
		}
		if left.SessionKey != right.SessionKey {
			return left.SessionKey < right.SessionKey
		}
		if !left.EventTS.Equal(right.EventTS) {
			return left.EventTS.Before(right.EventTS)
		}
		if left.SourcePriority != right.SourcePriority {
			return left.SourcePriority < right.SourcePriority
		}
		if left.SourcePart != right.SourcePart {
			return left.SourcePart < right.SourcePart
		}
		if left.SourceOffset != right.SourceOffset {
			return left.SourceOffset < right.SourceOffset
		}
		return left.EventID < right.EventID
	})
	return out, nil
}

func (r *repo) fetchCapabilitySnapshots(ctx context.Context, spec WindowSpec, identities []string) ([]capabilitySnapshot, error) {
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("fetch capability snapshots requires bounded window")
	}
	queryID, err := r.stageIdentities(ctx, identities)
	if err != nil {
		return nil, fmt.Errorf("stage identities for capability snapshots: %w", err)
	}
	start := spec.Start.UTC().Add(-10 * time.Minute)
	end := spec.End.UTC().Add(30 * time.Second)
	orgClause, orgArgs := orgPredicate("n.org", spec.Org, spec.ExcludedOrgPrefixes)
	args := []any{queryID, start, end}
	args = append(args, orgArgs...)
	args = append(args, queryID, start, end)
	args = append(args, orgArgs...)
	args = append(args, queryID, start, end)
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT DISTINCT
			org,
			orch_address,
			orch_uri,
			orch_uri_norm,
			if(
				match(lowerUTF8(ifNull(orch_name, '')), '^0x[0-9a-f]{40}$')
				AND lowerUTF8(ifNull(orch_name, '')) != lowerUTF8(ifNull(orch_address, '')),
				lowerUTF8(orch_name),
				''
			) AS local_address,
			event_id,
			event_ts,
			raw_capabilities
		FROM (
			SELECT
				n.org AS org,
				n.orch_address AS orch_address,
				n.orch_uri AS orch_uri,
				n.orch_uri_norm AS orch_uri_norm,
				ifNull(n.orch_name, '') AS orch_name,
				n.event_id AS event_id,
				n.event_ts AS event_ts,
				n.raw_capabilities AS raw_capabilities
			FROM naap.normalized_network_capabilities n
			INNER JOIN naap.resolver_query_identities i
				ON i.query_id = ?
			   AND i.identity = lowerUTF8(ifNull(n.orch_address, ''))
			WHERE n.event_ts >= ? AND n.event_ts < ?
			  AND `+orgClause+`

			UNION DISTINCT

			SELECT
				n.org AS org,
				n.orch_address AS orch_address,
				n.orch_uri AS orch_uri,
				n.orch_uri_norm AS orch_uri_norm,
				ifNull(n.orch_name, '') AS orch_name,
				n.event_id AS event_id,
				n.event_ts AS event_ts,
				n.raw_capabilities AS raw_capabilities
			FROM naap.normalized_network_capabilities n
			INNER JOIN naap.resolver_query_identities i
				ON i.query_id = ?
			   AND i.identity = lowerUTF8(ifNull(n.orch_uri_norm, ''))
			WHERE n.event_ts >= ? AND n.event_ts < ?
			  AND `+orgClause+`

			UNION DISTINCT

			SELECT
				n.org AS org,
				n.orch_address AS orch_address,
				n.orch_uri AS orch_uri,
				n.orch_uri_norm AS orch_uri_norm,
				ifNull(n.orch_name, '') AS orch_name,
				n.event_id AS event_id,
				n.event_ts AS event_ts,
				n.raw_capabilities AS raw_capabilities
			FROM naap.normalized_network_capabilities n
			INNER JOIN naap.resolver_query_identities i
				ON i.query_id = ?
			   AND i.identity = if(
					match(lowerUTF8(ifNull(n.orch_name, '')), '^0x[0-9a-f]{40}$')
					AND lowerUTF8(ifNull(n.orch_name, '')) != lowerUTF8(ifNull(n.orch_address, '')),
					lowerUTF8(n.orch_name),
					''
				)
			WHERE n.event_ts >= ? AND n.event_ts < ?
			  AND `+orgClause+`
		)
		ORDER BY org, orch_address, orch_uri_norm, event_ts, event_id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch capability snapshots: %w", err)
	}
	defer rows.Close()

	var out []capabilitySnapshot
	for rows.Next() {
		var row capabilitySnapshot
		if err := rows.Scan(
			&row.Org,
			&row.OrchAddress,
			&row.OrchURI,
			&row.OrchURINorm,
			&row.LocalAddress,
			&row.EventID,
			&row.EventTS,
			&row.RawPayload,
		); err != nil {
			return nil, fmt.Errorf("scan capability snapshot: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *repo) fetchSessionEvidence(ctx context.Context, refs []sessionKeyRef) (map[string]SessionEvidence, error) {
	if len(refs) == 0 {
		return map[string]SessionEvidence{}, nil
	}
	queryID, err := r.stageSessionKeys(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("stage session keys: %w", err)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			k.org,
			k.canonical_session_key,
			coalesce(nullIf(argMaxIfMerge(t.stream_id_state), ''), nullIf(argMaxIfMerge(s.stream_id_state), ''), '') AS stream_id,
			coalesce(nullIf(argMaxIfMerge(t.request_id_state), ''), nullIf(argMaxIfMerge(s.request_id_state), ''), '') AS request_id,
			coalesce(nullIf(any(sg.gateway), ''), nullIf(any(tg.gateway), ''), '') AS gateway,
			nullIf(minIfMerge(t.started_at_state), toDateTime64(0, 3, 'UTC')) AS started_at,
			nullIf(minIfMerge(t.first_processed_at_state), toDateTime64(0, 3, 'UTC')) AS first_processed_at,
			nullIf(minIfMerge(t.few_processed_at_state), toDateTime64(0, 3, 'UTC')) AS few_processed_at,
			nullIf(minIfMerge(t.first_ingest_at_state), toDateTime64(0, 3, 'UTC')) AS first_ingest_at,
			nullIf(minIfMerge(t.runner_first_processed_at_state), toDateTime64(0, 3, 'UTC')) AS runner_first_processed_at,
			nullIf(minIfMerge(s.start_time_state), toDateTime64(0, 3, 'UTC')) AS status_start_time,
			toUInt64(ifNull(sumMerge(t.started_count_state), 0)) AS started_count,
			toUInt64(ifNull(sumMerge(t.playable_seen_count_state), 0)) AS playable_seen_count,
			toUInt64(ifNull(sumMerge(t.no_orch_count_state), 0)) AS no_orch_count,
			toUInt64(ifNull(sumMerge(t.completed_count_state), 0)) AS completed_count,
			toUInt64(ifNull(sumMerge(t.swap_count_state), 0)) AS swap_count,
			max(tf.trace_last_seen) AS trace_last_seen,
			any(tf.trace_pipeline_hint) AS trace_pipeline_hint,
			toUInt64(ifNull(sumMerge(s.restart_seen_count_state), 0)) AS restart_seen_count,
			toUInt64(ifNull(sumMerge(s.error_seen_count_state), 0)) AS error_seen_count,
			toUInt64(ifNull(sumMerge(s.degraded_input_seen_count_state), 0)) AS degraded_input_seen_count,
			toUInt64(ifNull(sumMerge(s.degraded_inference_seen_count_state), 0)) AS degraded_inference_seen_count,
			toUInt64(ifNull(sumMerge(s.status_sample_count_state), 0)) AS status_sample_count,
			toUInt64(ifNull(sumMerge(s.status_error_sample_count_state), 0)) AS status_error_sample_count,
			toUInt64(ifNull(sumMerge(s.online_seen_count_state), 0)) AS online_seen_count,
			toUInt64(ifNull(sumMerge(s.positive_output_seen_count_state), 0)) AS positive_output_seen_count,
			toUInt64(ifNull(sumMerge(s.running_state_samples_count_state), 0)) AS running_state_samples_count,
			maxMerge(s.status_last_seen_state) AS status_last_seen,
			argMaxIfMerge(s.canonical_pipeline_state) AS status_pipeline_hint,
			argMaxIfMerge(e.event_pipeline_hint_state) AS event_pipeline_hint,
			maxMerge(e.event_last_seen_state) AS event_last_seen,
			toUInt64(ifNull(any(ee.startup_error_count), 0)) AS startup_error_count,
			toUInt64(ifNull(any(ee.excusable_error_count), 0)) AS excusable_error_count
		FROM naap.resolver_query_session_keys k
		LEFT JOIN naap.normalized_session_trace_rollup_latest t
			ON k.org = t.org AND k.canonical_session_key = t.canonical_session_key
		LEFT JOIN naap.normalized_session_status_rollup_latest s
			ON k.org = s.org AND k.canonical_session_key = s.canonical_session_key
		LEFT JOIN naap.normalized_session_event_rollup_latest e
			ON k.org = e.org AND k.canonical_session_key = e.canonical_session_key
		LEFT JOIN (
			SELECT
				k.org,
				k.canonical_session_key,
				max(t.event_ts) AS trace_last_seen,
				argMaxIf(t.raw_pipeline_hint, t.event_ts, toUInt8(t.raw_pipeline_hint != '')) AS trace_pipeline_hint
			FROM naap.resolver_query_session_keys k
			INNER JOIN naap.normalized_stream_trace t
				ON k.org = t.org AND k.canonical_session_key = t.canonical_session_key
			WHERE k.query_id = ?
			  AND `+canonicalStreamTracePredicate("t")+`
			GROUP BY k.org, k.canonical_session_key
		) tf
			ON k.org = tf.org AND k.canonical_session_key = tf.canonical_session_key
		LEFT JOIN (
			SELECT
				k.org,
				k.canonical_session_key,
				argMax(t.gateway, t.event_ts) AS gateway
			FROM naap.resolver_query_session_keys k
			INNER JOIN naap.normalized_stream_trace t
				ON k.org = t.org AND k.canonical_session_key = t.canonical_session_key
			WHERE k.query_id = ?
			  AND `+canonicalStreamTracePredicate("t")+`
			GROUP BY k.org, k.canonical_session_key
		) tg
			ON k.org = tg.org AND k.canonical_session_key = tg.canonical_session_key
		LEFT JOIN (
			SELECT
				k.org,
				k.canonical_session_key,
				toUInt64(countIf(lowerUTF8(ifNull(a.event_name, '')) = 'error')) AS startup_error_count,
				toUInt64(countIf(
					lowerUTF8(ifNull(a.event_name, '')) = 'error'
					AND (
						positionCaseInsensitive(ifNull(a.message, ''), 'no orchestrators available') > 0
						OR positionCaseInsensitive(ifNull(a.message, ''), 'mediamtx ingest disconnected') > 0
						OR positionCaseInsensitive(ifNull(a.message, ''), 'whip disconnected') > 0
						OR positionCaseInsensitive(ifNull(a.message, ''), 'missing video') > 0
						OR positionCaseInsensitive(ifNull(a.message, ''), 'ice connection state failed') > 0
						OR positionCaseInsensitive(ifNull(a.message, ''), 'user disconnected') > 0
					)
				)) AS excusable_error_count
			FROM naap.resolver_query_session_keys k
			INNER JOIN naap.normalized_ai_stream_events a
				ON k.org = a.org AND k.canonical_session_key = a.canonical_session_key
			WHERE k.query_id = ?
			GROUP BY k.org, k.canonical_session_key
		) ee
			ON k.org = ee.org AND k.canonical_session_key = ee.canonical_session_key
		LEFT JOIN (
			SELECT
				k.org,
				k.canonical_session_key,
				argMax(s.gateway, s.event_ts) AS gateway
			FROM naap.resolver_query_session_keys k
			INNER JOIN naap.normalized_ai_stream_status s
				ON k.org = s.org AND k.canonical_session_key = s.canonical_session_key
			WHERE k.query_id = ?
			GROUP BY k.org, k.canonical_session_key
		) sg
			ON k.org = sg.org AND k.canonical_session_key = sg.canonical_session_key
		WHERE k.query_id = ?
		GROUP BY k.org, k.canonical_session_key
	`, queryID, queryID, queryID, queryID, queryID)
	if err != nil {
		return nil, fmt.Errorf("fetch session evidence: %w", err)
	}
	defer rows.Close()

	out := make(map[string]SessionEvidence, len(refs))
	for rows.Next() {
		var row SessionEvidence
		var startedAt, firstProcessedAt, fewProcessedAt, firstIngestAt, runnerFirstProcessedAt, statusStartTime sql.NullTime
		var traceLastSeen, statusLastSeen, eventLastSeen sql.NullTime
		if err := rows.Scan(
			&row.Org,
			&row.SessionKey,
			&row.StreamID,
			&row.RequestID,
			&row.Gateway,
			&startedAt,
			&firstProcessedAt,
			&fewProcessedAt,
			&firstIngestAt,
			&runnerFirstProcessedAt,
			&statusStartTime,
			&row.StartedCount,
			&row.PlayableSeenCount,
			&row.NoOrchCount,
			&row.CompletedCount,
			&row.SwapCount,
			&traceLastSeen,
			&row.TracePipelineHint,
			&row.RestartSeenCount,
			&row.ErrorSeenCount,
			&row.DegradedInputSeenCount,
			&row.DegradedInferenceSeenCount,
			&row.StatusSampleCount,
			&row.StatusErrorSampleCount,
			&row.OnlineSeenCount,
			&row.PositiveOutputSeenCount,
			&row.RunningStateSamplesCount,
			&statusLastSeen,
			&row.StatusPipelineHint,
			&row.EventPipelineHint,
			&eventLastSeen,
			&row.StartupErrorCount,
			&row.ExcusableErrorCount,
		); err != nil {
			return nil, fmt.Errorf("scan session evidence: %w", err)
		}
		if startedAt.Valid {
			ts := startedAt.Time.UTC()
			row.StartedAt = &ts
		}
		if firstProcessedAt.Valid {
			ts := firstProcessedAt.Time.UTC()
			row.FirstProcessedAt = &ts
		}
		if fewProcessedAt.Valid {
			ts := fewProcessedAt.Time.UTC()
			row.FewProcessedAt = &ts
		}
		if firstIngestAt.Valid {
			ts := firstIngestAt.Time.UTC()
			row.FirstIngestAt = &ts
		}
		if runnerFirstProcessedAt.Valid {
			ts := runnerFirstProcessedAt.Time.UTC()
			row.RunnerFirstProcessedAt = &ts
		}
		if statusStartTime.Valid {
			ts := statusStartTime.Time.UTC()
			row.StatusStartTime = &ts
		}
		if traceLastSeen.Valid {
			ts := traceLastSeen.Time.UTC()
			row.TraceLastSeen = &ts
		}
		if statusLastSeen.Valid {
			ts := statusLastSeen.Time.UTC()
			row.StatusLastSeen = &ts
		}
		if eventLastSeen.Valid {
			ts := eventLastSeen.Time.UTC()
			row.EventLastSeen = &ts
		}
		out[row.SessionKey] = row
	}
	return out, rows.Err()
}

func (r *repo) fetchStatusHourEvidence(ctx context.Context, refs []sessionKeyRef, spec WindowSpec) ([]statusHourEvidence, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	queryID, err := r.stageSessionKeys(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("stage status-hour session keys: %w", err)
	}
	query := `
		SELECT
			k.org,
			k.canonical_session_key,
			h.hour,
			argMaxIfMerge(h.stream_id_state) AS stream_id,
			argMaxIfMerge(h.request_id_state) AS request_id,
			toUInt64(sumMerge(h.status_samples_state)) AS status_samples,
			toUInt64(sumMerge(h.fps_positive_samples_state)) AS fps_positive_samples,
			toUInt64(sumMerge(h.running_state_samples_state)) AS running_state_samples,
			toUInt64(sumMerge(h.degraded_input_samples_state)) AS degraded_input_samples,
			toUInt64(sumMerge(h.degraded_inference_samples_state)) AS degraded_inference_samples,
			toUInt64(sumMerge(h.error_samples_state)) AS error_samples,
			toFloat64(sumMerge(h.output_fps_sum_state)) AS output_fps_sum,
			toFloat64(sumMerge(h.input_fps_sum_state)) AS input_fps_sum
		FROM naap.resolver_query_session_keys k
		INNER JOIN naap.normalized_session_status_hour_rollup h
			ON k.org = h.org AND k.canonical_session_key = h.canonical_session_key
		WHERE k.query_id = ?`
	args := []any{queryID}
	if spec.Start != nil {
		query += " AND h.hour >= ?"
		args = append(args, spec.Start.UTC().Truncate(time.Hour).Add(-time.Hour))
	}
	if spec.End != nil {
		query += " AND h.hour < ?"
		args = append(args, spec.End.UTC().Truncate(time.Hour).Add(time.Hour))
	}
	query += `
		GROUP BY k.org, k.canonical_session_key, h.hour
		ORDER BY k.org, k.canonical_session_key, h.hour`
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch status hour evidence: %w", err)
	}
	defer rows.Close()

	var out []statusHourEvidence
	for rows.Next() {
		var row statusHourEvidence
		if err := rows.Scan(
			&row.Org,
			&row.SessionKey,
			&row.Hour,
			&row.StreamID,
			&row.RequestID,
			&row.StatusSamples,
			&row.FPSPositiveSamples,
			&row.RunningStateSamples,
			&row.DegradedInputSamples,
			&row.DegradedInferenceSamples,
			&row.ErrorSamples,
			&row.OutputFPSSum,
			&row.InputFPSSum,
		); err != nil {
			return nil, fmt.Errorf("scan status hour evidence: %w", err)
		}
		row.Hour = row.Hour.UTC()
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *repo) fetchLatestSessionDecisions(ctx context.Context, refs []sessionKeyRef) (map[string]SelectionDecision, error) {
	if len(refs) == 0 {
		return map[string]SelectionDecision{}, nil
	}
	queryID, err := r.stageSessionKeys(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("stage latest-decision session keys: %w", err)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			c.canonical_session_key,
			argMax(c.selection_event_id, tuple(c.selection_ts, c.decided_at)) AS selection_event_id,
			any(c.org) AS org,
			argMax(c.selection_ts, tuple(c.selection_ts, c.decided_at)) AS latest_selection_ts,
			argMax(c.attribution_status, tuple(c.selection_ts, c.decided_at)) AS attribution_status,
			argMax(c.attribution_reason, tuple(c.selection_ts, c.decided_at)) AS attribution_reason,
			argMax(c.attribution_method, tuple(c.selection_ts, c.decided_at)) AS attribution_method,
			argMax(c.selection_confidence, tuple(c.selection_ts, c.decided_at)) AS selection_confidence,
			argMax(ifNull(c.selected_capability_version_id, ''), tuple(c.selection_ts, c.decided_at)) AS capability_version_id,
			argMax(ifNull(c.selected_snapshot_event_id, ''), tuple(c.selection_ts, c.decided_at)) AS snapshot_event_id,
			argMax(c.selected_snapshot_ts, tuple(c.selection_ts, c.decided_at)) AS snapshot_ts,
			argMax(ifNull(c.attributed_orch_address, ''), tuple(c.selection_ts, c.decided_at)) AS attributed_orch_address,
			argMax(ifNull(c.attributed_orch_uri, ''), tuple(c.selection_ts, c.decided_at)) AS attributed_orch_uri,
			argMax(ifNull(c.canonical_pipeline, ''), tuple(c.selection_ts, c.decided_at)) AS canonical_pipeline,
			argMax(ifNull(c.canonical_model, ''), tuple(c.selection_ts, c.decided_at)) AS canonical_model,
			argMax(ifNull(c.gpu_id, ''), tuple(c.selection_ts, c.decided_at)) AS gpu_id,
			argMax(c.decision_input_hash, tuple(c.selection_ts, c.decided_at)) AS input_hash,
			max(c.decided_at) AS latest_decided_at
		FROM naap.resolver_query_session_keys k
		INNER JOIN naap.canonical_selection_attribution_current c
			ON k.org = c.org AND k.canonical_session_key = c.canonical_session_key
		WHERE k.query_id = ?
		GROUP BY c.canonical_session_key
	`, queryID)
	if err != nil {
		return nil, fmt.Errorf("fetch latest session decisions: %w", err)
	}
	defer rows.Close()

	out := make(map[string]SelectionDecision, len(refs))
	for rows.Next() {
		var row SelectionDecision
		var snapshotTS sql.NullTime
		var canonicalModel string
		if err := rows.Scan(
			&row.SessionKey,
			&row.SelectionEventID,
			&row.Org,
			&row.SelectionTS,
			&row.Status,
			&row.Reason,
			&row.Method,
			&row.Confidence,
			&row.CapabilityVersionID,
			&row.SnapshotEventID,
			&snapshotTS,
			&row.AttributedOrchAddress,
			&row.AttributedOrchURI,
			&row.CanonicalPipeline,
			&canonicalModel,
			&row.GPUID,
			&row.InputHash,
			new(time.Time),
		); err != nil {
			return nil, fmt.Errorf("scan latest session decision: %w", err)
		}
		if canonicalModel != "" {
			row.CanonicalModel = canonicalModel
		}
		if snapshotTS.Valid {
			ts := snapshotTS.Time.UTC()
			row.SnapshotTS = &ts
		}
		out[row.SessionKey] = row
	}
	return out, rows.Err()
}

func (r *repo) fetchCurrentDecisionHashes(ctx context.Context, selectionEventIDs []string) (map[string]string, error) {
	if len(selectionEventIDs) == 0 {
		return map[string]string{}, nil
	}
	queryID, err := r.stageSelectionEventIDs(ctx, selectionEventIDs)
	if err != nil {
		return nil, fmt.Errorf("stage decision ids: %w", err)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			c.selection_event_id,
			lower(hex(MD5(concat(
				ifNull(c.attribution_status, ''), '|',
				ifNull(c.attribution_reason, ''), '|',
				ifNull(c.attribution_method, ''), '|',
				ifNull(c.selection_confidence, ''), '|',
				ifNull(c.selected_capability_version_id, ''), '|',
				ifNull(c.selected_snapshot_event_id, ''), '|',
				ifNull(c.attributed_orch_address, ''), '|',
				ifNull(c.attributed_orch_uri, ''), '|',
				ifNull(c.canonical_pipeline, ''), '|',
				ifNull(c.canonical_model, ''), '|',
				ifNull(c.gpu_id, '')
			)))) AS row_hash
		FROM naap.resolver_query_selection_event_ids i
		INNER JOIN naap.canonical_selection_attribution_current c
			ON i.selection_event_id = c.selection_event_id
		WHERE i.query_id = ?
	`, queryID)
	if err != nil {
		return nil, fmt.Errorf("fetch current decision hashes: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(selectionEventIDs))
	for rows.Next() {
		var id, hash string
		if err := rows.Scan(&id, &hash); err != nil {
			return nil, fmt.Errorf("scan current decision hash: %w", err)
		}
		out[id] = hash
	}
	return out, rows.Err()
}

func (r *repo) recordRun(ctx context.Context, runID string, req RunRequest, status string, startedAt, finishedAt time.Time, rowsProcessed, mismatchCount uint64, errSummary string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.resolver_runs
		(
			run_id, mode, status, owner_id, org, window_start, window_end, cutoff_ts,
			lateness_window_seconds, rows_processed, mismatch_count, error_summary,
			resolver_version, started_at, finished_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		runID, string(req.Mode), status, r.ownerID(), nullableStringValue(req.Org), nullableTimeValue(req.Start), nullableTimeValue(req.End), nullableTimeValue(req.End),
		uint64(r.cfg.ResolverLatenessWindow.Seconds()), rowsProcessed, mismatchCount, nullableStringValue(errSummary),
		r.cfg.ResolverVersion, startedAt.UTC(), finishedAt.UTC(),
	)
}

func (r *repo) recordBackfillPartition(ctx context.Context, runID string, part backfillPartition, status string, rowsProcessed, mismatchCount uint64, errSummary string, startedAt, finishedAt time.Time) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.resolver_backfill_runs
		(
			run_id, owner_id, org, event_date, hash_range_start, hash_range_end, status,
			cutoff_ts, rows_processed, mismatch_count, error_summary, started_at, finished_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		runID, r.ownerID(), part.Org, part.EventDate, nil, nil, status,
		finishedAt.UTC(), rowsProcessed, mismatchCount, nullableStringValue(errSummary), startedAt.UTC(), finishedAt.UTC(),
	)
}

func (r *repo) insertWindowClaim(ctx context.Context, claim windowClaim) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.resolver_window_claims
		(
			claim_key, claim_type, mode, owner_id, org, window_start, window_end,
			lease_expires_at, released_at, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		claim.ClaimKey,
		claim.ClaimType,
		claim.Mode,
		claim.OwnerID,
		nullableStringValue(claim.Org),
		claim.WindowStart.UTC(),
		claim.WindowEnd.UTC(),
		claim.LeaseExpiresAt.UTC(),
		nullableTimeValue(claim.ReleasedAt),
		claim.CreatedAt.UTC(),
		time.Now().UTC(),
	)
}

func (r *repo) activeOverlappingWindowClaims(ctx context.Context, claim windowClaim) ([]windowClaim, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
			claim_key,
			claim_type,
			mode,
			ifNull(org, '') AS org,
			owner_id,
			window_start,
			window_end,
			lease_expires_at,
			created_at,
			released_at
		FROM naap.resolver_window_claims FINAL
		WHERE claim_type = ?
		  AND lease_expires_at > now64(3)
		  AND released_at IS NULL
		  AND window_start < ?
		  AND window_end > ?
		  AND (ifNull(org, '') = '' OR ? = '' OR org = ?)
	`, claim.ClaimType, claim.WindowEnd.UTC(), claim.WindowStart.UTC(), claim.Org, claim.Org)
	if err != nil {
		return nil, fmt.Errorf("query active overlapping window claims: %w", err)
	}
	defer rows.Close()

	var out []windowClaim
	for rows.Next() {
		var row windowClaim
		var releasedAt sql.NullTime
		if err := rows.Scan(
			&row.ClaimKey,
			&row.ClaimType,
			&row.Mode,
			&row.Org,
			&row.OwnerID,
			&row.WindowStart,
			&row.WindowEnd,
			&row.LeaseExpiresAt,
			&row.CreatedAt,
			&releasedAt,
		); err != nil {
			return nil, fmt.Errorf("scan window claim: %w", err)
		}
		row.WindowStart = row.WindowStart.UTC()
		row.WindowEnd = row.WindowEnd.UTC()
		row.LeaseExpiresAt = row.LeaseExpiresAt.UTC()
		row.CreatedAt = row.CreatedAt.UTC()
		if releasedAt.Valid {
			ts := releasedAt.Time.UTC()
			row.ReleasedAt = &ts
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func scanDirtyPartition(scanner interface{ Scan(dest ...any) error }) (dirtyPartition, error) {
	var part dirtyPartition
	var claimOwner sql.NullString
	var leaseExpiresAt sql.NullTime
	var lastError sql.NullString
	if err := scanner.Scan(
		&part.Org,
		&part.EventDate,
		&part.Status,
		&part.Reason,
		&part.FirstDirtyAt,
		&part.LastDirtyAt,
		&claimOwner,
		&leaseExpiresAt,
		&part.AttemptCount,
		&lastError,
		&part.UpdatedAt,
	); err != nil {
		return dirtyPartition{}, fmt.Errorf("scan dirty partition: %w", err)
	}
	part.EventDate = truncateUTCDate(part.EventDate)
	part.FirstDirtyAt = part.FirstDirtyAt.UTC()
	part.LastDirtyAt = part.LastDirtyAt.UTC()
	part.UpdatedAt = part.UpdatedAt.UTC()
	if claimOwner.Valid {
		part.ClaimOwner = claimOwner.String
	}
	if leaseExpiresAt.Valid {
		ts := leaseExpiresAt.Time.UTC()
		part.LeaseExpiresAt = &ts
	}
	if lastError.Valid {
		part.LastErrorSummary = lastError.String
	}
	return part, nil
}

func (r *repo) dirtyPartitionState(ctx context.Context, org string, eventDate time.Time) (*dirtyPartition, error) {
	rows, err := r.conn.Query(ctx, `
		SELECT
			ifNull(org, '') AS org,
			event_date,
			status,
			reason,
			first_dirty_at,
			last_dirty_at,
			claim_owner,
			lease_expires_at,
			attempt_count,
			last_error_summary,
			updated_at
		FROM naap.resolver_dirty_partitions FINAL
		WHERE org = ? AND event_date = ?
		LIMIT 1
	`, org, truncateUTCDate(eventDate))
	if err != nil {
		return nil, fmt.Errorf("query dirty partition state: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	part, err := scanDirtyPartition(rows)
	if err != nil {
		return nil, err
	}
	return &part, rows.Err()
}

func (r *repo) insertDirtyPartitionState(ctx context.Context, part dirtyPartition) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.resolver_dirty_partitions
		(
			org, event_date, status, reason, first_dirty_at, last_dirty_at,
			claim_owner, lease_expires_at, attempt_count, last_error_summary, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		part.Org,
		truncateUTCDate(part.EventDate),
		part.Status,
		part.Reason,
		part.FirstDirtyAt.UTC(),
		part.LastDirtyAt.UTC(),
		nullableStringValue(part.ClaimOwner),
		nullableTimeValue(part.LeaseExpiresAt),
		part.AttemptCount,
		nullableStringValue(part.LastErrorSummary),
		part.UpdatedAt.UTC(),
	)
}

func (r *repo) enqueueDirtyPartitions(ctx context.Context, parts []backfillPartition, dirtyAt time.Time) (int, error) {
	enqueued := 0
	for _, part := range parts {
		current, err := r.dirtyPartitionState(ctx, part.Org, part.EventDate)
		if err != nil {
			return enqueued, err
		}
		state := nextDirtyPartitionState(current, part, dirtyAt)
		if err := r.insertDirtyPartitionState(ctx, state); err != nil {
			return enqueued, fmt.Errorf("insert dirty partition pending state: %w", err)
		}
		enqueued++
	}
	return enqueued, nil
}

func nextDirtyPartitionState(current *dirtyPartition, part backfillPartition, dirtyAt time.Time) dirtyPartition {
	state := dirtyPartition{
		Org:          part.Org,
		EventDate:    truncateUTCDate(part.EventDate),
		Status:       "pending",
		Reason:       "late_accepted_raw",
		FirstDirtyAt: dirtyAt.UTC(),
		LastDirtyAt:  dirtyAt.UTC(),
		UpdatedAt:    dirtyAt.UTC(),
	}
	if current == nil {
		return state
	}
	state.FirstDirtyAt = current.FirstDirtyAt.UTC()
	state.AttemptCount = current.AttemptCount
	state.LastErrorSummary = ""
	switch current.Status {
	case "claimed":
		// Coalesce newly arrived late rows onto the in-flight claim instead of
		// immediately flipping the same day back to pending.
		state.Status = "claimed"
		state.ClaimOwner = current.ClaimOwner
		state.LeaseExpiresAt = current.LeaseExpiresAt
		state.LastErrorSummary = current.LastErrorSummary
	case "pending":
		state.Status = "pending"
	case "failed", "success":
		state.Status = "pending"
	}
	return state
}

func (r *repo) claimDirtyPartition(ctx context.Context, org string, eventDate time.Time, ownerID string, ttl time.Duration) (bool, error) {
	current, err := r.dirtyPartitionState(ctx, org, eventDate)
	if err != nil {
		return false, err
	}
	if current == nil || current.Status != "pending" {
		return false, nil
	}
	now := time.Now().UTC()
	current.Status = "claimed"
	current.ClaimOwner = ownerID
	lease := now.Add(ttl)
	current.LeaseExpiresAt = &lease
	current.AttemptCount++
	current.LastErrorSummary = ""
	current.UpdatedAt = now
	if err := r.insertDirtyPartitionState(ctx, *current); err != nil {
		return false, fmt.Errorf("claim dirty partition: %w", err)
	}
	return true, nil
}

func (r *repo) releaseDirtyPartition(ctx context.Context, org string, eventDate time.Time, ownerID string) error {
	current, err := r.dirtyPartitionState(ctx, org, eventDate)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	now := time.Now().UTC()
	current.Status = "pending"
	current.ClaimOwner = ""
	current.LeaseExpiresAt = nil
	current.UpdatedAt = now
	return r.insertDirtyPartitionState(ctx, *current)
}

func (r *repo) completeDirtyPartition(ctx context.Context, org string, eventDate time.Time, ownerID string) error {
	current, err := r.dirtyPartitionState(ctx, org, eventDate)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	now := time.Now().UTC()
	current.Status = "success"
	current.ClaimOwner = ownerID
	current.LeaseExpiresAt = nil
	current.LastErrorSummary = ""
	current.UpdatedAt = now
	return r.insertDirtyPartitionState(ctx, *current)
}

func (r *repo) failDirtyPartition(ctx context.Context, org string, eventDate time.Time, ownerID, errSummary string) error {
	current, err := r.dirtyPartitionState(ctx, org, eventDate)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	now := time.Now().UTC()
	current.Status = "failed"
	current.ClaimOwner = ownerID
	current.LeaseExpiresAt = nil
	current.LastErrorSummary = errSummary
	current.UpdatedAt = now
	return r.insertDirtyPartitionState(ctx, *current)
}

func (r *repo) insertSelectionEvents(ctx context.Context, runID string, rows []SelectionEvent) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_selection_events
		(
			selection_event_id, org, canonical_session_key, selection_seq, selection_ts,
			selection_trigger, observed_orch_raw_address, observed_orch_url,
			observed_model_hint, observed_pipeline_hint, anchor_event_id, anchor_event_type,
			anchor_event_ts, source_topic, source_partition, source_offset,
			selection_input_hash, resolver_version, resolver_run_id, rebuilt_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare selection event batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.ID, row.Org, row.SessionKey, row.Seq, row.SelectionTS.UTC(),
			row.Trigger, nullableStringValue(row.ObservedAddress), nullableStringValue(row.ObservedURL),
			nullableStringValue(row.ObservedModelHint), nullableStringValue(row.ObservedPipeline),
			row.AnchorEventID, row.AnchorEventType, row.AnchorEventTS.UTC(),
			row.SourceTopic, row.SourcePartition, row.SourceOffset,
			row.InputHash, r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append selection event: %w", err)
		}
	}
	return batch.Send()
}

func (r *repo) insertCapabilityVersions(ctx context.Context, runID string, rows []CapabilityVersion) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_orch_capability_versions
		(
			capability_version_id, org, orch_address, orch_uri, orch_uri_norm, local_address,
			snapshot_event_id, snapshot_ts, capability_payload_hash, raw_capabilities, is_noop,
			version_rank, resolver_version, resolver_run_id, built_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare capability version batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.ID, row.Org, row.OrchAddress, nullableStringValue(row.OrchURI), row.OrchURINorm, nullableStringValue(row.LocalAddress),
			row.SnapshotEventID, row.SnapshotTS.UTC(), row.PayloadHash, row.RawCapabilities, row.IsNoop,
			row.VersionRank, r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append capability version: %w", err)
		}
	}
	return batch.Send()
}

func (r *repo) insertCapabilityIntervals(ctx context.Context, runID string, rows []CapabilityInterval) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_orch_capability_intervals
		(
			capability_version_id, org, orch_address, orch_uri, orch_uri_norm, valid_from_ts,
			valid_to_ts, canonical_pipeline, canonical_model, gpu_id, gpu_model_name,
			gpu_memory_bytes_total, hardware_present, interval_hash, resolver_version,
			resolver_run_id, built_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare capability interval batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.VersionID, row.Org, row.OrchAddress, nullableStringValue(row.OrchURI), row.OrchURINorm, row.ValidFromTS.UTC(),
			nullableTimeValue(row.ValidToTS), nullableStringValue(row.Pipeline), nullableStringValue(row.Model),
			nullableStringValue(row.GPUID), nullableStringValue(row.GPUModelName), nullableUint64Value(row.GPUMemoryTotal),
			boolToUInt8(row.HardwarePresent), row.IntervalHash, r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append capability interval: %w", err)
		}
	}
	return batch.Send()
}

func (r *repo) insertDecisionRows(ctx context.Context, runID string, rows []SelectionDecision, previous map[string]string) error {
	if len(rows) == 0 {
		return nil
	}
	decisions, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_selection_attribution_decisions
		(
			decision_id, selection_event_id, org, canonical_session_key, selection_ts,
			attribution_status, attribution_reason, attribution_method, selection_confidence,
			selected_capability_version_id, selected_snapshot_event_id, selected_snapshot_ts,
			attributed_orch_address, attributed_orch_uri, canonical_pipeline, canonical_model,
			gpu_id, decision_input_hash, resolver_version, resolver_run_id, decided_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare decision batch: %w", err)
	}
	current, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_selection_attribution_current
		(
			selection_event_id, org, canonical_session_key, selection_ts, attribution_status,
			attribution_reason, attribution_method, selection_confidence, selected_capability_version_id,
			selected_snapshot_event_id, selected_snapshot_ts, attributed_orch_address, attributed_orch_uri,
			canonical_pipeline, canonical_model, gpu_id, decision_input_hash, resolver_version,
			resolver_run_id, decided_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare current decision batch: %w", err)
	}
	changes, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.selection_attribution_changes
		(
			run_id, selection_event_id, org, canonical_session_key, selection_ts,
			change_reason, previous_decision_hash, current_decision_hash, created_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare decision change batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		decisionID := stableHash(row.SelectionEventID, row.Status, row.Reason, row.Method, row.InputHash, now.Format(time.RFC3339Nano))
		if err := decisions.Append(
			decisionID, row.SelectionEventID, row.Org, row.SessionKey, row.SelectionTS.UTC(),
			row.Status, row.Reason, row.Method, row.Confidence,
			nullableStringValue(row.CapabilityVersionID), nullableStringValue(row.SnapshotEventID), nullableTimeValue(row.SnapshotTS),
			nullableStringValue(row.AttributedOrchAddress), nullableStringValue(row.AttributedOrchURI),
			nullableStringValue(row.CanonicalPipeline), nullableStringValue(row.CanonicalModel), nullableStringValue(row.GPUID),
			row.InputHash, r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append decision row: %w", err)
		}
		if err := current.Append(
			row.SelectionEventID, row.Org, row.SessionKey, row.SelectionTS.UTC(), row.Status,
			row.Reason, row.Method, row.Confidence, nullableStringValue(row.CapabilityVersionID),
			nullableStringValue(row.SnapshotEventID), nullableTimeValue(row.SnapshotTS),
			nullableStringValue(row.AttributedOrchAddress), nullableStringValue(row.AttributedOrchURI),
			nullableStringValue(row.CanonicalPipeline), nullableStringValue(row.CanonicalModel), nullableStringValue(row.GPUID),
			row.InputHash, r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append current decision row: %w", err)
		}
		currentHash := stableHash(
			row.Status, row.Reason, row.Method, row.Confidence, row.CapabilityVersionID, row.SnapshotEventID,
			row.AttributedOrchAddress, row.AttributedOrchURI, row.CanonicalPipeline, row.CanonicalModel, row.GPUID,
		)
		changeReason := "unchanged"
		prevHash := previous[row.SelectionEventID]
		if prevHash == "" {
			changeReason = "new_selection_decision"
		} else if prevHash != currentHash {
			changeReason = "decision_updated"
		}
		if err := changes.Append(
			runID, row.SelectionEventID, row.Org, row.SessionKey, row.SelectionTS.UTC(),
			changeReason, nullableStringValue(prevHash), currentHash, now,
		); err != nil {
			return fmt.Errorf("append decision change row: %w", err)
		}
	}
	if err := decisions.Send(); err != nil {
		return fmt.Errorf("send decision rows: %w", err)
	}
	if err := current.Send(); err != nil {
		return fmt.Errorf("send current decision rows: %w", err)
	}
	return changes.Send()
}

func (r *repo) insertSessionCurrentRows(ctx context.Context, runID string, rows []SessionCurrentRow) error {
	if len(rows) == 0 {
		return nil
	}
	store, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_session_current_store
		(
			canonical_session_key, org, stream_id, request_id, current_selection_event_id,
			current_selection_ts, canonical_pipeline, canonical_model, gpu_id, started_at,
			last_seen, startup_latency_ms, e2e_latency_ms, prompt_to_playable_latency_ms,
			requested_seen, playable_seen, selection_outcome, completed, swap_count, restart_seen,
			error_seen, degraded_input_seen, degraded_inference_seen, status_sample_count,
			status_error_sample_count, startup_error_count, excusable_error_count, loading_only_session, zero_output_fps_session,
			health_signal_count, health_expected_signal_count, health_signal_coverage_ratio,
			startup_outcome, excusal_reason, has_ambiguous_identity, has_snapshot_match, is_hardware_less,
			is_stale, attribution_reason, attribution_status, attributed_orch_address,
			attributed_orch_uri, attribution_snapshot_ts, resolver_version, resolver_run_id,
			materialized_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare session current batch: %w", err)
	}
	changes, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.session_current_changes
		(
			run_id, canonical_session_key, org, last_seen, change_reason, current_row_hash, created_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare session current change batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := store.Append(
			row.SessionKey, row.Org, row.StreamID, row.RequestID, nullableStringValue(row.CurrentSelectionEventID),
			nullableTimeValue(row.CurrentSelectionTS), row.CanonicalPipeline, nullableStringValue(row.CanonicalModel), nullableStringValue(row.GPUID),
			nullableTimeValue(row.StartedAt), row.LastSeen.UTC(), nullableFloat64Value(row.StartupLatencyMS),
			nullableFloat64Value(row.E2ELatencyMS), nullableFloat64Value(row.PromptToPlayableLatencyMS),
			row.RequestedSeen, row.PlayableSeen, row.SelectionOutcome, row.Completed,
			row.SwapCount, row.RestartSeen, row.ErrorSeen, row.DegradedInputSeen, row.DegradedInferenceSeen,
			row.StatusSampleCount, row.StatusErrorSampleCount, row.StartupErrorCount, row.ExcusableErrorCount, row.LoadingOnlySession, row.ZeroOutputFPSSession,
			row.HealthSignalCount, row.HealthExpectedSignalCount, row.HealthSignalCoverageRatio, row.StartupOutcome, row.ExcusalReason,
			row.HasAmbiguousIdentity, row.HasSnapshotMatch, row.IsHardwareLess, row.IsStale, row.AttributionReason,
			row.AttributionStatus, nullableStringValue(row.AttributedOrchAddress), nullableStringValue(row.AttributedOrchURI),
			nullableTimeValue(row.AttributionSnapshotTS), r.cfg.ResolverVersion, runID, now,
		); err != nil {
			return fmt.Errorf("append session current row: %w", err)
		}
		rowHash := sessionCurrentRowHash(row)
		changeReason := "session_updated"
		if err := changes.Append(runID, row.SessionKey, row.Org, row.LastSeen.UTC(), changeReason, rowHash, now); err != nil {
			return fmt.Errorf("append session current change: %w", err)
		}
	}
	if err := store.Send(); err != nil {
		return fmt.Errorf("send session current rows: %w", err)
	}
	return changes.Send()
}

func (r *repo) insertStatusHourRows(ctx context.Context, runID string, rows []StatusHourRow) error {
	if len(rows) == 0 {
		return nil
	}
	store, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_status_hours_store
		(
			canonical_session_key, org, hour, stream_id, request_id, canonical_pipeline,
			canonical_model, orch_address, attribution_status, attribution_reason,
			started_at, session_last_seen, startup_latency_ms, avg_e2e_latency_ms, prompt_to_playable_latency_ms,
			status_samples, fps_positive_samples,
			running_state_samples, degraded_input_samples, degraded_inference_samples,
			error_samples, avg_output_fps, avg_input_fps,
			is_terminal_tail_artifact, refresh_run_id, artifact_checksum, refreshed_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare status hour batch: %w", err)
	}
	changes, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.status_hour_changes
		(
			run_id, canonical_session_key, org, hour, change_reason, current_row_hash, created_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare status hour change batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := store.Append(
			row.SessionKey, row.Org, row.Hour.UTC(), row.StreamID, row.RequestID, row.CanonicalPipeline,
			nullableStringValue(row.CanonicalModel), nullableStringValue(row.OrchAddress), row.AttributionStatus, row.AttributionReason,
			nullableTimeValue(row.StartedAt), row.SessionLastSeen.UTC(), nullableFloat64Value(row.StartupLatencyMS),
			nullableFloat64Value(row.E2ELatencyMS), nullableFloat64Value(row.PromptToPlayableLatencyMS),
			row.StatusSamples, row.FPSPositiveSamples,
			row.RunningStateSamples, row.DegradedInputSamples, row.DegradedInferenceSamples, row.ErrorSamples,
			row.AvgOutputFPS, row.AvgInputFPS, row.IsTerminalTailArtifact,
			runID, r.cfg.ResolverVersion, now,
		); err != nil {
			return fmt.Errorf("append status hour row: %w", err)
		}
		rowHash := statusHourRowHash(row)
		if err := changes.Append(runID, row.SessionKey, row.Org, row.Hour.UTC(), "status_hour_updated", rowHash, now); err != nil {
			return fmt.Errorf("append status hour change: %w", err)
		}
	}
	if err := store.Send(); err != nil {
		return fmt.Errorf("send status hour rows: %w", err)
	}
	return changes.Send()
}

func (r *repo) insertSessionDemandInputRows(ctx context.Context, runID string, rows []SessionCurrentRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_session_demand_input_current
		(
			canonical_session_key, window_start, org, gateway, region, pipeline_id, model_id,
			requested_seen, selection_outcome, startup_outcome, excusal_reason,
			loading_only_session, zero_output_fps_session, health_signal_count, health_expected_signal_count,
			swap_count, error_seen, status_error_sample_count, health_signal_coverage_ratio, avg_output_fps, total_minutes, ticket_face_value_eth,
			refresh_run_id, artifact_checksum, refreshed_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare session demand input batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		windowStart := row.LastSeen.UTC().Truncate(time.Hour)
		if row.StartedAt != nil {
			windowStart = row.StartedAt.UTC().Truncate(time.Hour)
		}
		totalMinutes := 0.0
		if row.StartedAt != nil {
			totalMinutes = row.LastSeen.Sub(row.StartedAt.UTC()).Minutes()
			if totalMinutes < 0 {
				totalMinutes = 0
			}
		}
		if err := batch.Append(
			row.SessionKey, windowStart, row.Org, row.Gateway, nil, row.CanonicalPipeline, nullableStringValue(row.CanonicalModel),
			row.RequestedSeen, row.SelectionOutcome, row.StartupOutcome, row.ExcusalReason,
			row.LoadingOnlySession, row.ZeroOutputFPSSession, row.HealthSignalCount, row.HealthExpectedSignalCount,
			row.SwapCount, row.ErrorSeen, row.StatusErrorSampleCount, row.HealthSignalCoverageRatio, 0.0, totalMinutes, 0.0,
			runID, r.cfg.ResolverVersion, now,
		); err != nil {
			return fmt.Errorf("append session demand input row: %w", err)
		}
	}
	return batch.Send()
}

func (r *repo) publishServingRollups(ctx context.Context, runID string, slices []windowSliceRef) error {
	if len(slices) == 0 {
		return nil
	}
	queryID, err := r.stageWindowSlices(ctx, slices)
	if err != nil {
		return fmt.Errorf("stage window slices: %w", err)
	}
	if err := r.insertCanonicalStatusSamples(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertCanonicalActiveStreamState(ctx, runID, queryID); err != nil {
		return err
	}
	// insertPaymentLinkRows must run before insertPaymentHourlyRollups so the
	// store is populated with current-window rows before the hourly aggregation
	// reads from it.
	if err := r.insertPaymentLinkRows(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertPaymentHourlyRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertNetworkDemandRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertGPUNetworkDemandRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertSLAComplianceRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertFinalSLAComplianceByOrgRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertFinalSLAComplianceRollups(ctx, runID, queryID); err != nil {
		return err
	}
	if err := r.insertGPUMetricsRollups(ctx, runID, queryID); err != nil {
		return err
	}
	return nil
}

func (r *repo) insertCanonicalStatusSamples(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.canonical_status_samples_recent_store
		(
			canonical_session_key, event_id, sample_ts, org, stream_id, request_id, gateway,
			orch_address, pipeline, model_id, attribution_status, attribution_reason, state,
			output_fps, input_fps, e2e_latency_ms, is_attributed, refresh_run_id, artifact_checksum, refreshed_at
		)
		WITH status_events AS (
			SELECT
				s.event_id AS event_id,
				max(s.event_ts) AS event_ts,
				argMax(s.org, s.event_ts) AS org,
				argMax(s.gateway, s.event_ts) AS gateway,
				argMax(s.stream_id, s.event_ts) AS stream_id,
				argMax(s.request_id, s.event_ts) AS request_id,
				argMax(s.canonical_session_key, s.event_ts) AS canonical_session_key,
				argMax(s.state, s.event_ts) AS state,
				argMax(s.output_fps, s.event_ts) AS output_fps,
				argMax(s.input_fps, s.event_ts) AS input_fps
			FROM naap.normalized_ai_stream_status s
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = s.org AND toStartOfHour(s.event_ts) = w.window_start
			WHERE s.event_id != ''
			  AND s.canonical_session_key != ''
			GROUP BY s.event_id
		)
		SELECT
			s.canonical_session_key,
			s.event_id,
			s.event_ts AS sample_ts,
			s.org,
			coalesce(nullIf(fs.stream_id, ''), s.stream_id) AS stream_id,
			coalesce(nullIf(fs.request_id, ''), s.request_id) AS request_id,
			s.gateway,
			cast(nullIf(fs.attributed_orch_address, ''), 'Nullable(String)') AS orch_address,
			fs.canonical_pipeline AS pipeline,
			cast(nullIf(fs.canonical_model, ''), 'Nullable(String)') AS model_id,
			fs.attribution_status,
			fs.attribution_reason,
			s.state,
			s.output_fps,
			s.input_fps,
			fs.e2e_latency_ms,
			if(fs.attribution_status = 'resolved', toUInt8(1), toUInt8(0)) AS is_attributed,
			?,
			?,
			now64()
		FROM status_events s
		LEFT JOIN naap.canonical_session_current fs
			ON fs.canonical_session_key = s.canonical_session_key
	`, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) insertCanonicalActiveStreamState(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.canonical_active_stream_state_latest_store
		(
			canonical_session_key, event_id, sample_ts, org, stream_id, request_id, gateway,
			pipeline, model_id, orch_address, attribution_status, attribution_reason, state,
			output_fps, input_fps, e2e_latency_ms, started_at, last_seen, completed,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		WITH status_events AS (
			SELECT
				s.event_id AS event_id,
				max(s.event_ts) AS event_ts,
				argMax(s.org, s.event_ts) AS org,
				argMax(s.gateway, s.event_ts) AS gateway,
				argMax(s.stream_id, s.event_ts) AS stream_id,
				argMax(s.request_id, s.event_ts) AS request_id,
				argMax(s.canonical_session_key, s.event_ts) AS canonical_session_key,
				argMax(s.state, s.event_ts) AS state,
				argMax(s.output_fps, s.event_ts) AS output_fps,
				argMax(s.input_fps, s.event_ts) AS input_fps,
				argMax(s.e2e_latency_ms, s.event_ts) AS e2e_latency_ms
			FROM naap.normalized_ai_stream_status s
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = s.org AND toStartOfHour(s.event_ts) = w.window_start
			WHERE s.event_id != ''
			  AND s.canonical_session_key != ''
			GROUP BY s.event_id
		),
		status_enriched AS (
			SELECT
				s.canonical_session_key,
				s.event_id,
				s.event_ts AS sample_ts,
				s.org,
				coalesce(nullIf(fs.stream_id, ''), s.stream_id) AS stream_id,
				coalesce(nullIf(fs.request_id, ''), s.request_id) AS request_id,
				s.gateway,
				fs.canonical_pipeline AS pipeline,
				cast(nullIf(fs.canonical_model, ''), 'Nullable(String)') AS model_id,
				cast(nullIf(fs.attributed_orch_address, ''), 'Nullable(String)') AS orch_address,
				fs.attribution_status AS attribution_status,
				fs.attribution_reason AS attribution_reason,
				s.state AS state,
				s.output_fps AS output_fps,
				s.input_fps AS input_fps,
				fs.e2e_latency_ms AS e2e_latency_ms,
				fs.started_at AS started_at,
				fs.last_seen AS last_seen,
				fs.completed AS completed
			FROM status_events s
			LEFT JOIN naap.canonical_session_current fs
				ON fs.canonical_session_key = s.canonical_session_key
		)
		SELECT
			canonical_session_key,
			event_id,
			sample_ts,
			org,
			stream_id,
			request_id,
			gateway,
			pipeline,
			model_id,
			orch_address,
			attribution_status,
			attribution_reason,
			state,
			output_fps,
			input_fps,
			e2e_latency_ms,
			started_at,
			last_seen,
			completed,
			?,
			?,
			now64()
		FROM status_enriched
		ORDER BY canonical_session_key, sample_ts DESC, event_id DESC
		LIMIT 1 BY canonical_session_key
	`, queryID, runID, r.cfg.ResolverVersion)
}

// insertPaymentLinkRows writes one row per payment event into
// canonical_payment_links_store for every (org, window_start) slice in the
// current resolver run.  It reads directly from accepted_raw_events filtered to
// the active windows — avoiding the slow canonical_payment_links view — and
// LEFT JOINs canonical_session_current_store for the request_id → session link.
// ReplacingMergeTree(refreshed_at) on the store means re-runs overwrite stale
// rows, so re-linking as sessions resolve is free.
func (r *repo) insertPaymentLinkRows(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.canonical_payment_links_store
		(
			event_id, event_ts, org, gateway,
			session_id, request_id, manifest_id, pipeline_hint,
			sender_address, recipient_address, orchestrator_url,
			face_value_wei, price_wei_per_pixel, win_prob, num_tickets,
			canonical_session_key, link_method, link_status,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		SELECT
			p.event_id,
			p.event_ts,
			p.org,
			p.gateway,
			JSONExtractString(p.data, 'sessionID')                                                          AS session_id,
			JSONExtractString(p.data, 'requestID')                                                          AS request_id,
			JSONExtractString(p.data, 'manifestID')                                                         AS manifest_id,
			replaceRegexpOne(JSONExtractString(p.data, 'manifestID'), '^[0-9]+_', '')                       AS pipeline_hint,
			lower(JSONExtractString(p.data, 'sender'))                                                      AS sender_address,
			lower(JSONExtractString(p.data, 'recipient'))                                                   AS recipient_address,
			JSONExtractString(p.data, 'orchestrator')                                                       AS orchestrator_url,
			toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(p.data, 'faceValue'), ' WEI', '')))    AS face_value_wei,
			toFloat64OrDefault(replaceRegexpOne(JSONExtractString(p.data, 'price'), ' wei/pixel$', ''))     AS price_wei_per_pixel,
			toFloat64OrDefault(JSONExtractString(p.data, 'winProb'))                                        AS win_prob,
			toUInt64OrDefault(JSONExtractString(p.data, 'numTickets'))                                      AS num_tickets,
			cast(nullIf(fs.canonical_session_key, ''), 'Nullable(String)')                                  AS canonical_session_key,
			if(
				JSONExtractString(p.data, 'requestID') != '' AND isNotNull(canonical_session_key),
				'request_id',
				'unlinked'
			)                                                                                               AS link_method,
			if(isNotNull(canonical_session_key), 'resolved', 'unresolved')                                 AS link_status,
			?,
			?,
			now64()
		FROM naap.accepted_raw_events p
		INNER JOIN naap.resolver_query_window_slices w
			ON  w.query_id = ?
			AND p.org = w.org
			AND toStartOfHour(p.event_ts) = w.window_start
		LEFT JOIN naap.canonical_session_current_store fs FINAL
			ON  fs.org = p.org
			AND fs.request_id = JSONExtractString(p.data, 'requestID')
			AND JSONExtractString(p.data, 'requestID') != ''
		WHERE p.event_type = 'create_new_payment'
		  AND p.event_id   != ''
	`, runID, r.cfg.ResolverVersion, queryID)
}

// insertPaymentHourlyRollups aggregates canonical_payment_links_store rows for
// the active window slices into api_payment_hourly_store.  Reads from the store
// (bounded, indexed) rather than the canonical_payment_links view (unbounded
// full-scan with NOT-IN anti-joins) so it completes in milliseconds.
func (r *repo) insertPaymentHourlyRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_payment_hourly_store
		(
			hour, org, pipeline, orch_address, total_wei, event_count,
			avg_price_wei_per_pixel, refresh_run_id, artifact_checksum, refreshed_at
		)
		SELECT
			toStartOfHour(p.event_ts)                                              AS hour,
			p.org                                                                  AS org,
			coalesce(nullIf(fs.canonical_pipeline, ''), p.pipeline_hint)          AS pipeline,
			p.recipient_address                                                    AS orch_address,
			sum(p.face_value_wei)                                                  AS total_wei,
			count()                                                                AS event_count,
			avg(p.price_wei_per_pixel)                                             AS avg_price_wei_per_pixel,
			?,
			?,
			now64()
		FROM naap.canonical_payment_links_store p FINAL
		INNER JOIN naap.resolver_query_window_slices rs
			ON  rs.query_id = ?
			AND p.org = rs.org
			AND toStartOfHour(p.event_ts) = rs.window_start
		LEFT JOIN naap.canonical_session_current_store fs FINAL
			ON  fs.canonical_session_key = p.canonical_session_key
			AND isNotNull(p.canonical_session_key)
		GROUP BY
			hour,
			p.org,
			pipeline,
			p.recipient_address
	`, runID, r.cfg.ResolverVersion, queryID)
}

func (r *repo) insertNetworkDemandRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_network_demand_by_org_store
		(
			window_start, org, gateway, region, pipeline_id, model_id, sessions_count,
			avg_output_fps, output_fps_sum, status_samples, total_minutes, known_sessions_count, requested_sessions, startup_success_sessions,
			no_orch_sessions, startup_excused_sessions, startup_failed_sessions, loading_only_sessions,
			zero_output_fps_sessions, effective_failed_sessions, confirmed_swapped_sessions,
			inferred_swap_sessions, total_swapped_sessions, sessions_ending_in_error,
			error_status_samples, health_signal_count, health_expected_signal_count, health_signal_coverage_ratio,
			startup_success_rate, excused_failure_rate, effective_success_rate, ticket_face_value_eth,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		WITH demand_sessions AS (
			SELECT
				d.canonical_session_key,
				d.window_start,
				d.org,
				d.gateway,
				d.region,
				d.pipeline_id,
				d.model_id,
				d.requested_seen,
				d.selection_outcome,
				d.startup_outcome,
				d.excusal_reason,
				d.loading_only_session,
				d.zero_output_fps_session,
				d.health_signal_count,
				d.health_expected_signal_count,
				d.swap_count,
				d.error_seen,
				d.status_error_sample_count,
				d.total_minutes,
				d.ticket_face_value_eth
			FROM (
				SELECT *
				FROM naap.canonical_session_demand_input_current FINAL
			) d
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = d.org AND w.window_start = d.window_start
		),
		status_events AS (
			SELECT
				s.event_id AS event_id,
				max(s.event_ts) AS event_ts,
				argMax(s.org, s.event_ts) AS org,
				argMax(s.gateway, s.event_ts) AS gateway,
				argMax(s.canonical_session_key, s.event_ts) AS canonical_session_key,
				argMax(s.output_fps, s.event_ts) AS output_fps
			FROM naap.normalized_ai_stream_status s
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = s.org AND w.window_start = toStartOfHour(s.event_ts)
			WHERE s.event_id != ''
			  AND s.canonical_session_key != ''
			GROUP BY s.event_id
		),
		perf_sessions AS (
			SELECT
				s.canonical_session_key AS canonical_session_key,
				toStartOfHour(s.event_ts) AS window_start,
				s.org AS org,
				s.gateway AS gateway,
				cast(null AS Nullable(String)) AS region,
				fs.canonical_pipeline AS pipeline_id,
				cast(nullIf(fs.canonical_model, ''), 'Nullable(String)') AS model_id,
				toUInt64(count()) AS status_samples,
				sum(s.output_fps) AS output_fps_sum
			FROM status_events s
			LEFT JOIN naap.canonical_session_current fs
				ON s.canonical_session_key = fs.canonical_session_key
			GROUP BY
				s.canonical_session_key,
				window_start,
				s.org,
				s.gateway,
				fs.canonical_pipeline,
				model_id
		),
		combined_rows AS (
			SELECT
				canonical_session_key,
				window_start,
				org,
				gateway,
				region,
				pipeline_id,
				model_id,
				toUInt8(1) AS has_demand_row,
				requested_seen,
				selection_outcome,
				startup_outcome,
				excusal_reason,
				loading_only_session,
				zero_output_fps_session,
				health_signal_count AS session_health_signal_count,
				health_expected_signal_count AS session_health_expected_signal_count,
				swap_count,
				error_seen,
				status_error_sample_count,
				total_minutes,
				ticket_face_value_eth,
				toUInt64(0) AS status_samples,
				0.0 AS output_fps_sum
			FROM demand_sessions
			UNION ALL
			SELECT
				canonical_session_key,
				window_start,
				org,
				gateway,
				region,
				pipeline_id,
				model_id,
				toUInt8(0) AS has_demand_row,
				toUInt8(0) AS requested_seen,
				'unknown' AS selection_outcome,
				'unknown' AS startup_outcome,
				'none' AS excusal_reason,
				toUInt8(0) AS loading_only_session,
				toUInt8(0) AS zero_output_fps_session,
				toUInt64(0) AS session_health_signal_count,
				toUInt64(0) AS session_health_expected_signal_count,
				toUInt64(0) AS swap_count,
				toUInt8(0) AS error_seen,
				toUInt64(0) AS status_error_sample_count,
				0.0 AS total_minutes,
				0.0 AS ticket_face_value_eth,
				status_samples,
				output_fps_sum
			FROM perf_sessions
		),
		session_union AS (
			SELECT
				canonical_session_key,
				window_start,
				org,
				gateway,
				region,
				pipeline_id,
				model_id,
				max(requested_seen) AS requested_seen,
				argMax(selection_outcome, has_demand_row) AS selection_outcome,
				argMax(startup_outcome, has_demand_row) AS startup_outcome,
				argMax(excusal_reason, has_demand_row) AS excusal_reason,
				max(loading_only_session) AS loading_only_session,
				max(zero_output_fps_session) AS zero_output_fps_session,
				max(session_health_signal_count) AS session_health_signal_count,
				max(session_health_expected_signal_count) AS session_health_expected_signal_count,
				max(swap_count) AS swap_count,
				max(error_seen) AS error_seen,
				max(status_error_sample_count) AS status_error_sample_count,
				max(total_minutes) AS total_minutes,
				max(ticket_face_value_eth) AS ticket_face_value_eth,
				sum(status_samples) AS session_status_samples,
				sum(output_fps_sum) AS session_output_fps_sum
			FROM combined_rows
			GROUP BY
				canonical_session_key,
				window_start,
				org,
				gateway,
				region,
				pipeline_id,
				model_id
		)
		SELECT
			window_start,
			org,
			gateway,
			region,
			pipeline_id,
			model_id,
			toUInt64(count()) AS sessions_count,
			if(sum(session_status_samples) > 0, sum(session_output_fps_sum) / toFloat64(sum(session_status_samples)), 0.0) AS avg_output_fps,
			sum(session_output_fps_sum) AS output_fps_sum,
			toUInt64(sum(session_status_samples)) AS status_samples,
			sum(total_minutes) AS total_minutes,
			toUInt64(countIf(requested_seen = 1)) AS known_sessions_count,
			toUInt64(countIf(requested_seen = 1)) AS requested_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'success')) AS startup_success_sessions,
			toUInt64(countIf(requested_seen = 1 AND selection_outcome = 'no_orch')) AS no_orch_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason != 'none')) AS startup_excused_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason = 'none')) AS startup_failed_sessions,
			toUInt64(countIf(requested_seen = 1 AND loading_only_session = 1)) AS loading_only_sessions,
			toUInt64(countIf(requested_seen = 1 AND zero_output_fps_session = 1)) AS zero_output_fps_sessions,
			toUInt64(countIf(
				requested_seen = 1 AND (
					(startup_outcome = 'failed' AND excusal_reason = 'none') OR
					loading_only_session = 1 OR
					zero_output_fps_session = 1
				)
			)) AS effective_failed_sessions,
			toUInt64(0) AS confirmed_swapped_sessions,
			toUInt64(0) AS inferred_swap_sessions,
			toUInt64(countIf(requested_seen = 1 AND swap_count > 0)) AS total_swapped_sessions,
			toUInt64(countIf(requested_seen = 1 AND error_seen = 1)) AS sessions_ending_in_error,
			toUInt64(sum(status_error_sample_count)) AS error_status_samples,
			toUInt64(sum(session_health_signal_count)) AS health_signal_count,
			toUInt64(sum(session_health_expected_signal_count)) AS health_expected_signal_count,
			least(
				if(
					sum(session_health_expected_signal_count) > 0,
					sum(session_health_signal_count) / toFloat64(sum(session_health_expected_signal_count)),
					1.0
				),
				1.0
			) AS health_signal_coverage_ratio,
			if(countIf(requested_seen = 1) > 0, countIf(requested_seen = 1 AND startup_outcome = 'success') / toFloat64(countIf(requested_seen = 1)), 0.0) AS startup_success_rate,
			if(countIf(requested_seen = 1) > 0, countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason != 'none') / toFloat64(countIf(requested_seen = 1)), 0.0) AS excused_failure_rate,
			if(
				countIf(requested_seen = 1) > 0,
				1.0 - (
					countIf(
						requested_seen = 1 AND (
							(startup_outcome = 'failed' AND excusal_reason = 'none') OR
							loading_only_session = 1 OR
							zero_output_fps_session = 1
						)
					) / toFloat64(countIf(requested_seen = 1))
				),
				0.0
			) AS effective_success_rate,
			sum(ticket_face_value_eth) AS ticket_face_value_eth,
			?,
			?,
			now64()
		FROM session_union
		GROUP BY window_start, org, gateway, region, pipeline_id, model_id
	`, queryID, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) insertGPUNetworkDemandRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_gpu_network_demand_by_org_store
		(
			window_start, org, gateway, orchestrator_address, region, pipeline_id, model_id, gpu_id, gpu_identity_status,
			sessions_count, avg_output_fps, output_fps_sum, status_samples, total_minutes, known_sessions_count, requested_sessions, startup_success_sessions,
			no_orch_sessions, startup_excused_sessions, startup_failed_sessions, loading_only_sessions,
			zero_output_fps_sessions, effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error, error_status_samples, health_signal_count,
			health_expected_signal_count, health_signal_coverage_ratio, startup_success_rate,
			excused_failure_rate, effective_success_rate, ticket_face_value_eth, refresh_run_id,
			artifact_checksum, refreshed_at
		)
		WITH demand_sessions AS (
			SELECT
				d.canonical_session_key,
				d.window_start,
				d.org,
				d.gateway,
				d.region,
				ifNull(cs.attributed_orch_address, '') AS orchestrator_address,
				d.pipeline_id,
				coalesce(d.model_id, nullIf(cs.canonical_model, '')) AS model_id,
				cast(nullIf(cs.gpu_id, ''), 'Nullable(String)') AS gpu_id,
				if(ifNull(cs.attributed_orch_address, '') != '' AND ifNull(cs.gpu_id, '') = '', 'hardware_unresolved', 'resolved') AS gpu_identity_status,
				d.requested_seen,
				d.selection_outcome,
				d.startup_outcome,
				d.excusal_reason,
				d.loading_only_session,
				d.zero_output_fps_session,
				d.health_signal_count,
				d.health_expected_signal_count,
				d.swap_count,
				d.error_seen,
				d.status_error_sample_count,
				d.total_minutes,
				d.ticket_face_value_eth
			FROM (
				SELECT *
				FROM naap.canonical_session_demand_input_current FINAL
			) d
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = d.org AND w.window_start = d.window_start
			LEFT JOIN naap.canonical_session_current cs
				ON d.canonical_session_key = cs.canonical_session_key
			WHERE ifNull(cs.attributed_orch_address, '') != ''
		),
		status_events AS (
			SELECT
				s.event_id AS event_id,
				max(s.event_ts) AS event_ts,
				argMax(s.org, s.event_ts) AS org,
				argMax(s.gateway, s.event_ts) AS gateway,
				argMax(s.canonical_session_key, s.event_ts) AS canonical_session_key,
				argMax(s.output_fps, s.event_ts) AS output_fps
			FROM naap.normalized_ai_stream_status s
			INNER JOIN naap.resolver_query_window_slices w
				ON w.query_id = ? AND w.org = s.org AND w.window_start = toStartOfHour(s.event_ts)
			WHERE s.event_id != ''
			  AND s.canonical_session_key != ''
			GROUP BY s.event_id
		),
		perf_sessions AS (
			SELECT
				s.canonical_session_key AS canonical_session_key,
				toStartOfHour(s.event_ts) AS window_start,
				s.org AS org,
				s.gateway AS gateway,
				cast(null AS Nullable(String)) AS region,
				ifNull(cs.attributed_orch_address, '') AS orchestrator_address,
				fs.canonical_pipeline AS pipeline_id,
				cast(nullIf(fs.canonical_model, ''), 'Nullable(String)') AS model_id,
				cast(nullIf(cs.gpu_id, ''), 'Nullable(String)') AS gpu_id,
				if(ifNull(cs.attributed_orch_address, '') != '' AND ifNull(cs.gpu_id, '') = '', 'hardware_unresolved', 'resolved') AS gpu_identity_status,
				toUInt64(count()) AS status_samples,
				sum(s.output_fps) AS output_fps_sum
			FROM status_events s
			LEFT JOIN naap.canonical_session_current fs
				ON s.canonical_session_key = fs.canonical_session_key
			LEFT JOIN naap.canonical_session_current cs
				ON s.canonical_session_key = cs.canonical_session_key
			WHERE ifNull(cs.attributed_orch_address, '') != ''
			GROUP BY
				s.canonical_session_key,
				window_start,
				s.org,
				s.gateway,
				orchestrator_address,
				fs.canonical_pipeline,
				model_id,
				gpu_id,
				gpu_identity_status
		),
		combined_rows AS (
			SELECT
				coalesce(d.canonical_session_key, p.canonical_session_key) AS canonical_session_key,
				coalesce(d.window_start, p.window_start) AS window_start,
				coalesce(d.org, p.org) AS org,
				coalesce(d.gateway, p.gateway) AS gateway,
				coalesce(d.orchestrator_address, p.orchestrator_address) AS orchestrator_address,
				coalesce(d.region, p.region) AS region,
				coalesce(d.pipeline_id, p.pipeline_id) AS pipeline_id,
				coalesce(d.model_id, p.model_id) AS model_id,
				coalesce(d.gpu_id, p.gpu_id) AS gpu_id,
				if(d.gpu_identity_status = 'hardware_unresolved' OR p.gpu_identity_status = 'hardware_unresolved', 'hardware_unresolved', 'resolved') AS gpu_identity_status,
				ifNull(d.requested_seen, toUInt8(0)) AS requested_seen,
				ifNull(d.selection_outcome, 'unknown') AS selection_outcome,
				ifNull(d.startup_outcome, 'unknown') AS startup_outcome,
				ifNull(d.excusal_reason, 'none') AS excusal_reason,
				ifNull(d.loading_only_session, toUInt8(0)) AS loading_only_session,
				ifNull(d.zero_output_fps_session, toUInt8(0)) AS zero_output_fps_session,
				ifNull(d.health_signal_count, toUInt64(0)) AS session_health_signal_count,
				ifNull(d.health_expected_signal_count, toUInt64(0)) AS session_health_expected_signal_count,
				ifNull(d.swap_count, toUInt64(0)) AS swap_count,
				ifNull(d.error_seen, toUInt8(0)) AS error_seen,
				ifNull(d.status_error_sample_count, toUInt64(0)) AS status_error_sample_count,
				ifNull(d.total_minutes, 0.0) AS total_minutes,
				ifNull(d.ticket_face_value_eth, 0.0) AS ticket_face_value_eth,
				ifNull(p.status_samples, toUInt64(0)) AS status_samples,
				ifNull(p.output_fps_sum, 0.0) AS output_fps_sum
			FROM demand_sessions d
			FULL OUTER JOIN perf_sessions p
				ON d.canonical_session_key = p.canonical_session_key
			   AND d.window_start = p.window_start
			   AND d.org = p.org
			   AND d.gateway = p.gateway
			   AND d.orchestrator_address = p.orchestrator_address
			   AND d.pipeline_id = p.pipeline_id
			   AND ifNull(d.model_id, '') = ifNull(p.model_id, '')
			   AND ifNull(d.gpu_id, '') = ifNull(p.gpu_id, '')
		),
		session_union AS (
			SELECT
				canonical_session_key,
				window_start,
				org,
				gateway,
				orchestrator_address,
				region,
				pipeline_id,
				model_id,
				gpu_id,
				max(gpu_identity_status) AS gpu_identity_status,
				max(requested_seen) AS requested_seen,
				argMax(selection_outcome, status_samples) AS selection_outcome,
				argMax(startup_outcome, status_samples) AS startup_outcome,
				argMax(excusal_reason, status_samples) AS excusal_reason,
				max(loading_only_session) AS loading_only_session,
				max(zero_output_fps_session) AS zero_output_fps_session,
				max(session_health_signal_count) AS session_health_signal_count,
				max(session_health_expected_signal_count) AS session_health_expected_signal_count,
				max(swap_count) AS swap_count,
				max(error_seen) AS error_seen,
				max(status_error_sample_count) AS status_error_sample_count,
				max(total_minutes) AS total_minutes,
				max(ticket_face_value_eth) AS ticket_face_value_eth,
				sum(status_samples) AS session_status_samples,
				sum(output_fps_sum) AS session_output_fps_sum
			FROM combined_rows
			GROUP BY
				canonical_session_key,
				window_start,
				org,
				gateway,
				orchestrator_address,
				region,
				pipeline_id,
				model_id,
				gpu_id
		)
		SELECT
			window_start,
			org,
			gateway,
			orchestrator_address,
			region,
			pipeline_id,
			model_id,
			gpu_id,
			gpu_identity_status,
			toUInt64(count()) AS sessions_count,
			if(sum(session_status_samples) > 0, sum(session_output_fps_sum) / toFloat64(sum(session_status_samples)), 0.0) AS avg_output_fps,
			sum(session_output_fps_sum) AS output_fps_sum,
			toUInt64(sum(session_status_samples)) AS status_samples,
			sum(total_minutes) AS total_minutes,
			toUInt64(countIf(requested_seen = 1)) AS known_sessions_count,
			toUInt64(countIf(requested_seen = 1)) AS requested_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'success')) AS startup_success_sessions,
			toUInt64(countIf(requested_seen = 1 AND selection_outcome = 'no_orch')) AS no_orch_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason != 'none')) AS startup_excused_sessions,
			toUInt64(countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason = 'none')) AS startup_failed_sessions,
			toUInt64(countIf(requested_seen = 1 AND loading_only_session = 1)) AS loading_only_sessions,
			toUInt64(countIf(requested_seen = 1 AND zero_output_fps_session = 1)) AS zero_output_fps_sessions,
			toUInt64(countIf(
				requested_seen = 1 AND (
					(startup_outcome = 'failed' AND excusal_reason = 'none') OR
					loading_only_session = 1 OR
					zero_output_fps_session = 1
				)
			)) AS effective_failed_sessions,
			toUInt64(0) AS confirmed_swapped_sessions,
			toUInt64(0) AS inferred_swap_sessions,
			toUInt64(countIf(requested_seen = 1 AND swap_count > 0)) AS total_swapped_sessions,
			toUInt64(countIf(requested_seen = 1 AND error_seen = 1)) AS sessions_ending_in_error,
			toUInt64(sum(status_error_sample_count)) AS error_status_samples,
			toUInt64(sum(session_health_signal_count)) AS health_signal_count,
			toUInt64(sum(session_health_expected_signal_count)) AS health_expected_signal_count,
			least(
				if(
					sum(session_health_expected_signal_count) > 0,
					sum(session_health_signal_count) / toFloat64(sum(session_health_expected_signal_count)),
					1.0
				),
				1.0
			) AS health_signal_coverage_ratio,
			if(countIf(requested_seen = 1) > 0, countIf(requested_seen = 1 AND startup_outcome = 'success') / toFloat64(countIf(requested_seen = 1)), 0.0) AS startup_success_rate,
			if(countIf(requested_seen = 1) > 0, countIf(requested_seen = 1 AND startup_outcome = 'failed' AND excusal_reason != 'none') / toFloat64(countIf(requested_seen = 1)), 0.0) AS excused_failure_rate,
			if(
				countIf(requested_seen = 1) > 0,
				1.0 - (
					countIf(
						requested_seen = 1 AND (
							(startup_outcome = 'failed' AND excusal_reason = 'none') OR
							loading_only_session = 1 OR
							zero_output_fps_session = 1
						)
					) / toFloat64(countIf(requested_seen = 1))
				),
				0.0
			) AS effective_success_rate,
			sum(ticket_face_value_eth) AS ticket_face_value_eth,
			?,
			?,
			now64()
		FROM session_union
		GROUP BY
			window_start,
			org,
			gateway,
			orchestrator_address,
			region,
			pipeline_id,
			model_id,
			gpu_id,
			gpu_identity_status
	`, queryID, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) insertSLAComplianceRollups(ctx context.Context, runID, queryID string) error {
	// This input store remains the resolver-owned source of truth for additive
	// SLA facts. Final scored serving rows are published immediately afterward
	// from the api_base_* helper relations so the API never scores on the hot path.
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_sla_compliance_inputs_by_org_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions, output_failed_sessions,
			effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, error_status_samples, health_signal_count, health_expected_signal_count,
			health_signal_coverage_ratio, startup_success_rate, excused_failure_rate, effective_success_rate,
			no_swap_rate, output_viability_rate, output_fps_sum, status_samples, prompt_to_first_frame_sum_ms,
			prompt_to_first_frame_sample_count, e2e_latency_sum_ms, e2e_latency_sample_count,
			refresh_run_id, artifact_checksum, refreshed_at
		)
		WITH status_hour_support AS (
			SELECT
				n.org AS org,
				n.canonical_session_key AS canonical_session_key,
				n.hour AS window_start,
				toUInt64(sumMerge(n.status_samples_state)) AS status_samples,
				sumMerge(n.output_fps_sum_state) AS output_fps_sum,
				sumMerge(n.e2e_latency_sum_state) AS e2e_latency_sum_ms,
				toUInt64(sumMerge(n.e2e_latency_count_state)) AS e2e_latency_sample_count
			FROM naap.normalized_session_status_hour_rollup n
			INNER JOIN naap.resolver_query_window_slices rs
				ON rs.query_id = ? AND n.org = rs.org AND n.hour = rs.window_start
			GROUP BY n.org, n.canonical_session_key, n.hour
		),
		base AS (
			SELECT
				h.hour AS window_start,
				h.org AS org,
				ifNull(h.orch_address, '') AS orchestrator_address,
				h.canonical_pipeline AS pipeline_id,
				h.canonical_model AS model_id,
				fs.requested_seen AS requested_seen,
				fs.resolver_startup_outcome AS startup_outcome,
				fs.excusal_reason AS excusal_reason,
				fs.loading_only_session AS loading_only_session,
				fs.zero_output_fps_session AS zero_output_fps_session,
				fs.selection_outcome AS selection_outcome,
				fs.swap_count AS swap_count,
				fs.error_seen AS error_seen,
				h.error_samples AS error_status_samples,
				fs.health_signal_count AS health_signal_count,
				fs.health_expected_signal_count AS health_expected_signal_count,
				ifNull(sh.output_fps_sum, h.avg_output_fps * toFloat64(h.status_samples)) AS output_fps_sum,
				ifNull(sh.status_samples, h.status_samples) AS status_samples,
				if(h.prompt_to_playable_latency_ms > 0, h.prompt_to_playable_latency_ms, 0.0) AS prompt_to_first_frame_sum_ms,
				toUInt64(h.prompt_to_playable_latency_ms > 0) AS prompt_to_first_frame_sample_count,
				if(
					ifNull(sh.e2e_latency_sample_count, toUInt64(0)) > 0,
					ifNull(sh.e2e_latency_sum_ms, 0.0),
					if(h.avg_e2e_latency_ms > 0, h.avg_e2e_latency_ms, 0.0)
				) AS e2e_latency_sum_ms,
				if(
					ifNull(sh.e2e_latency_sample_count, toUInt64(0)) > 0,
					sh.e2e_latency_sample_count,
					toUInt64(h.avg_e2e_latency_ms > 0)
				) AS e2e_latency_sample_count
			FROM naap.canonical_status_hours h
			INNER JOIN naap.resolver_query_window_slices rs
				ON rs.query_id = ? AND h.org = rs.org AND h.hour = rs.window_start
			LEFT JOIN naap.canonical_session_current fs
				ON h.canonical_session_key = fs.canonical_session_key
			LEFT JOIN status_hour_support sh
				ON h.org = sh.org
			   AND h.canonical_session_key = sh.canonical_session_key
			   AND h.hour = sh.window_start
			WHERE h.is_terminal_tail_artifact = 0
		),
		inventory_keys AS (
			SELECT DISTINCT
				org,
				orchestrator_address,
				coalesce(nullIf(model_id, ''), nullIf(pipeline_id, '')) AS inventory_key
			FROM base
			WHERE orchestrator_address != ''
		),
		inventory AS (
			SELECT
				inv.org,
				inv.orch_address AS orchestrator_address,
				coalesce(nullIf(inv.model_id, ''), nullIf(inv.pipeline_id, '')) AS inventory_key,
				any(inv.model_id) AS inventory_model_id,
				cast(nullIf(argMaxIfMerge(inv.gpu_id_state), ''), 'Nullable(String)') AS inventory_gpu_id,
				nullIf(argMaxIfMerge(inv.gpu_model_name_state), '') AS inventory_gpu_model_name
			FROM naap.canonical_latest_orchestrator_pipeline_inventory_agg inv
			INNER JOIN inventory_keys k
				ON inv.org = k.org
			   AND inv.orch_address = k.orchestrator_address
			   AND coalesce(nullIf(inv.model_id, ''), nullIf(inv.pipeline_id, '')) = k.inventory_key
			GROUP BY inv.org, orchestrator_address, inventory_key
		)
		SELECT
			b.window_start,
			b.org,
			b.orchestrator_address,
			b.pipeline_id,
			coalesce(b.model_id, i.inventory_model_id) AS model_id,
			i.inventory_gpu_id AS gpu_id,
			any(i.inventory_gpu_model_name) AS gpu_model_name,
			cast(null AS Nullable(String)) AS region,
			toUInt64(countIf(b.requested_seen = 1)) AS known_sessions_count,
			toUInt64(countIf(b.requested_seen = 1)) AS requested_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'success')) AS startup_success_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.selection_outcome = 'no_orch')) AS no_orch_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason != 'none')) AS startup_excused_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason = 'none')) AS startup_failed_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.loading_only_session = 1)) AS loading_only_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.zero_output_fps_session = 1)) AS zero_output_fps_sessions,
			toUInt64(countIf(
				b.requested_seen = 1 AND (
					b.loading_only_session = 1 OR
					b.zero_output_fps_session = 1
				)
			)) AS output_failed_sessions,
			toUInt64(countIf(
				b.requested_seen = 1 AND (
					(b.startup_outcome = 'failed' AND b.excusal_reason = 'none') OR
					b.loading_only_session = 1 OR
					b.zero_output_fps_session = 1
				)
			)) AS effective_failed_sessions,
			toUInt64(0) AS confirmed_swapped_sessions,
			toUInt64(0) AS inferred_swap_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.swap_count > 0)) AS total_swapped_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.error_seen = 1)) AS sessions_ending_in_error,
			toUInt64(sum(b.error_status_samples)) AS error_status_samples,
			toUInt64(sum(b.health_signal_count)) AS health_signal_count,
			toUInt64(sum(b.health_expected_signal_count)) AS health_expected_signal_count,
			least(
				if(
					sum(b.health_expected_signal_count) > 0,
					sum(b.health_signal_count) / toFloat64(sum(b.health_expected_signal_count)),
					1.0
				),
				1.0
			) AS health_signal_coverage_ratio,
			if(
				countIf(b.requested_seen = 1) > 0,
				toFloat64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'success')) / toFloat64(countIf(b.requested_seen = 1)),
				cast(null AS Nullable(Float64))
			) AS startup_success_rate,
			if(
				countIf(b.requested_seen = 1) > 0,
				toFloat64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason != 'none')) / toFloat64(countIf(b.requested_seen = 1)),
				cast(null AS Nullable(Float64))
			) AS excused_failure_rate,
			if(
				countIf(b.requested_seen = 1) > 0,
				1.0 - (
					toFloat64(countIf(
						b.requested_seen = 1 AND (
							(b.startup_outcome = 'failed' AND b.excusal_reason = 'none') OR
							b.loading_only_session = 1 OR
							b.zero_output_fps_session = 1
						)
					)) / toFloat64(countIf(b.requested_seen = 1))
				),
				cast(null AS Nullable(Float64))
			) AS effective_success_rate,
			if(
				countIf(b.requested_seen = 1) > 0,
				1.0 - toFloat64(countIf(b.requested_seen = 1 AND b.swap_count > 0)) / toFloat64(countIf(b.requested_seen = 1)),
				cast(null AS Nullable(Float64))
			) AS no_swap_rate,
			if(
				countIf(b.requested_seen = 1) > 0,
				1.0 - (
					toFloat64(countIf(
						b.requested_seen = 1 AND (
							b.loading_only_session = 1 OR
							b.zero_output_fps_session = 1
						)
					))
				) / toFloat64(countIf(b.requested_seen = 1)),
				cast(null AS Nullable(Float64))
			) AS output_viability_rate,
			sum(b.output_fps_sum) AS output_fps_sum,
			toUInt64(sum(b.status_samples)) AS status_samples,
			sum(b.prompt_to_first_frame_sum_ms) AS prompt_to_first_frame_sum_ms,
			toUInt64(sum(b.prompt_to_first_frame_sample_count)) AS prompt_to_first_frame_sample_count,
			sum(b.e2e_latency_sum_ms) AS e2e_latency_sum_ms,
			toUInt64(sum(b.e2e_latency_sample_count)) AS e2e_latency_sample_count,
			?,
			?,
			now64()
		FROM base b
		LEFT JOIN inventory i
			ON b.org = i.org
		   AND b.orchestrator_address = i.orchestrator_address
		   AND coalesce(nullIf(b.model_id, ''), nullIf(b.pipeline_id, '')) = i.inventory_key
		GROUP BY
			b.window_start,
			b.org,
			b.orchestrator_address,
			b.pipeline_id,
			coalesce(b.model_id, i.inventory_model_id),
			i.inventory_gpu_id
	`, queryID, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) insertFinalSLAComplianceByOrgRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_sla_compliance_by_org_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			output_failed_sessions, effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error, error_status_samples, health_signal_count,
			health_expected_signal_count, health_signal_coverage_ratio, startup_success_rate, excused_failure_rate,
			effective_success_rate, no_swap_rate, output_viability_rate, output_fps_sum, status_samples,
			avg_output_fps, prompt_to_first_frame_sum_ms, prompt_to_first_frame_sample_count,
			avg_prompt_to_first_frame_ms, e2e_latency_sum_ms, e2e_latency_sample_count, avg_e2e_latency_ms,
			reliability_score, ptff_score, e2e_score, latency_score, fps_score, quality_score,
			sla_semantics_version, sla_score, refresh_run_id, artifact_checksum, refreshed_at
		)
		SELECT
			s.window_start, s.org, s.orchestrator_address, s.pipeline_id, s.model_id, s.gpu_id, s.gpu_model_name, s.region,
			s.known_sessions_count, s.requested_sessions, s.startup_success_sessions, s.no_orch_sessions,
			s.startup_excused_sessions, s.startup_failed_sessions, s.loading_only_sessions, s.zero_output_fps_sessions,
			s.output_failed_sessions, s.effective_failed_sessions, s.confirmed_swapped_sessions, s.inferred_swap_sessions,
			s.total_swapped_sessions, s.sessions_ending_in_error, s.error_status_samples, s.health_signal_count,
			s.health_expected_signal_count, s.health_signal_coverage_ratio, s.startup_success_rate, s.excused_failure_rate,
			s.effective_success_rate, s.no_swap_rate, s.output_viability_rate, s.output_fps_sum, s.status_samples,
			s.avg_output_fps, s.prompt_to_first_frame_sum_ms, s.prompt_to_first_frame_sample_count,
			s.avg_prompt_to_first_frame_ms, s.e2e_latency_sum_ms, s.e2e_latency_sample_count, s.avg_e2e_latency_ms,
			s.reliability_score, s.ptff_score, s.e2e_score, s.latency_score, s.fps_score, s.quality_score,
			s.sla_semantics_version, s.sla_score, ?, ?, now64()
		FROM naap.api_base_sla_compliance_scored_by_org s
		INNER JOIN naap.resolver_query_window_slices w
			ON w.query_id = ? AND s.org = w.org AND s.window_start = w.window_start
	`, runID, r.cfg.ResolverVersion, queryID)
}

func (r *repo) insertFinalSLAComplianceRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_sla_compliance_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, gpu_model_name, region,
			known_sessions_count, requested_sessions, startup_success_sessions, no_orch_sessions,
			startup_excused_sessions, startup_failed_sessions, loading_only_sessions, zero_output_fps_sessions,
			output_failed_sessions, effective_failed_sessions, confirmed_swapped_sessions, inferred_swap_sessions,
			total_swapped_sessions, sessions_ending_in_error, error_status_samples, health_signal_count,
			health_expected_signal_count, health_signal_coverage_ratio, startup_success_rate, excused_failure_rate,
			effective_success_rate, no_swap_rate, output_viability_rate, output_fps_sum, status_samples,
			avg_output_fps, prompt_to_first_frame_sum_ms, prompt_to_first_frame_sample_count,
			avg_prompt_to_first_frame_ms, e2e_latency_sum_ms, e2e_latency_sample_count, avg_e2e_latency_ms,
			reliability_score, ptff_score, e2e_score, latency_score, fps_score, quality_score,
			sla_semantics_version, sla_score, refresh_run_id, artifact_checksum, refreshed_at
		)
		WITH owned_windows AS (
			SELECT DISTINCT window_start
			FROM naap.resolver_query_window_slices
			WHERE query_id = ?
		)
		SELECT
			s.window_start, s.org, s.orchestrator_address, s.pipeline_id, s.model_id, s.gpu_id, s.gpu_model_name, s.region,
			s.known_sessions_count, s.requested_sessions, s.startup_success_sessions, s.no_orch_sessions,
			s.startup_excused_sessions, s.startup_failed_sessions, s.loading_only_sessions, s.zero_output_fps_sessions,
			s.output_failed_sessions, s.effective_failed_sessions, s.confirmed_swapped_sessions, s.inferred_swap_sessions,
			s.total_swapped_sessions, s.sessions_ending_in_error, s.error_status_samples, s.health_signal_count,
			s.health_expected_signal_count, s.health_signal_coverage_ratio, s.startup_success_rate, s.excused_failure_rate,
			s.effective_success_rate, s.no_swap_rate, s.output_viability_rate, s.output_fps_sum, s.status_samples,
			s.avg_output_fps, s.prompt_to_first_frame_sum_ms, s.prompt_to_first_frame_sample_count,
			s.avg_prompt_to_first_frame_ms, s.e2e_latency_sum_ms, s.e2e_latency_sample_count, s.avg_e2e_latency_ms,
			s.reliability_score, s.ptff_score, s.e2e_score, s.latency_score, s.fps_score, s.quality_score,
			s.sla_semantics_version, s.sla_score, ?, ?, now64()
		FROM naap.api_base_sla_compliance_scored s
		INNER JOIN owned_windows w
			ON s.window_start = w.window_start
	`, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) insertGPUMetricsRollups(ctx context.Context, runID, queryID string) error {
	return r.conn.Exec(ctx, `
		INSERT INTO naap.api_gpu_metrics_by_org_store
		(
			window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id, region,
			avg_output_fps, output_fps_sum, p95_output_fps, output_fps_p95_state, fps_jitter_coefficient, status_samples,
			error_status_samples, health_signal_count, health_expected_signal_count, health_signal_coverage_ratio, gpu_model_name,
			gpu_memory_bytes_total, runner_version, cuda_version, avg_prompt_to_first_frame_ms, prompt_to_first_frame_sum_ms,
			avg_startup_latency_ms, startup_latency_sum_ms, avg_e2e_latency_ms, e2e_latency_sum_ms, p95_prompt_to_first_frame_latency_ms,
			prompt_to_first_frame_p95_state, p95_startup_latency_ms, startup_latency_p95_state, p95_e2e_latency_ms, e2e_latency_p95_state, prompt_to_first_frame_sample_count,
			startup_latency_sample_count, e2e_latency_sample_count, known_sessions_count,
			startup_success_sessions, no_orch_sessions, startup_excused_sessions, startup_failed_sessions,
			confirmed_swapped_sessions, inferred_swap_sessions, total_swapped_sessions,
			sessions_ending_in_error, startup_failed_rate, swap_rate, refresh_run_id,
			artifact_checksum, refreshed_at
		)
		WITH status_hour_support AS (
			SELECT
				n.org AS org,
				n.canonical_session_key AS canonical_session_key,
				n.hour AS window_start,
				toUInt64(sumMerge(n.status_samples_state)) AS status_samples,
				sumMerge(n.output_fps_sum_state) AS output_fps_sum,
				sumMerge(n.e2e_latency_sum_state) AS e2e_latency_sum_ms,
				toUInt64(sumMerge(n.e2e_latency_count_state)) AS e2e_latency_sample_count
			FROM naap.normalized_session_status_hour_rollup n
			INNER JOIN naap.resolver_query_window_slices rs
				ON rs.query_id = ? AND n.org = rs.org AND n.hour = rs.window_start
			GROUP BY n.org, n.canonical_session_key, n.hour
		),
		base AS (
			SELECT
				h.hour AS window_start,
				h.org AS org,
				ifNull(h.orch_address, '') AS orchestrator_address,
				h.canonical_pipeline AS pipeline_id,
				h.canonical_model AS model_id,
				cast(nullIf(fs.gpu_id, ''), 'Nullable(String)') AS session_gpu_id,
				fs.requested_seen AS requested_seen,
				ifNull(sh.status_samples, h.status_samples) AS status_samples,
				h.error_samples AS error_status_samples,
				ifNull(sh.output_fps_sum, h.avg_output_fps * toFloat64(h.status_samples)) AS output_fps_sum,
				h.avg_output_fps AS session_hour_avg_output_fps,
				fs.health_signal_count AS health_signal_count,
				fs.health_expected_signal_count AS health_expected_signal_count,
				h.prompt_to_playable_latency_ms AS prompt_to_playable_latency_ms,
				if(h.prompt_to_playable_latency_ms > 0, h.prompt_to_playable_latency_ms, 0.0) AS prompt_to_first_frame_sum_ms,
				toUInt64(h.prompt_to_playable_latency_ms > 0) AS prompt_to_first_frame_sample_count,
				h.startup_latency_ms AS startup_latency_ms,
				if(h.startup_latency_ms > 0, h.startup_latency_ms, 0.0) AS startup_latency_sum_ms,
				toUInt64(h.startup_latency_ms > 0) AS startup_latency_sample_count,
				if(
					ifNull(sh.e2e_latency_sample_count, toUInt64(0)) > 0,
					ifNull(sh.e2e_latency_sum_ms, 0.0),
					if(h.avg_e2e_latency_ms > 0, h.avg_e2e_latency_ms, 0.0)
				) AS e2e_latency_sum_ms,
				if(
					ifNull(sh.e2e_latency_sample_count, toUInt64(0)) > 0,
					sh.e2e_latency_sample_count,
					toUInt64(h.avg_e2e_latency_ms > 0)
				) AS e2e_latency_sample_count,
				h.avg_e2e_latency_ms AS session_hour_avg_e2e_latency_ms,
				fs.resolver_startup_outcome AS startup_outcome,
				fs.excusal_reason AS excusal_reason,
				fs.selection_outcome AS selection_outcome,
				fs.swap_count AS swap_count,
				fs.error_seen AS error_seen
			FROM naap.canonical_status_hours h
			INNER JOIN naap.resolver_query_window_slices rs
				ON rs.query_id = ? AND h.org = rs.org AND h.hour = rs.window_start
			LEFT JOIN naap.canonical_session_current fs
				ON h.canonical_session_key = fs.canonical_session_key
			LEFT JOIN status_hour_support sh
				ON h.org = sh.org AND h.canonical_session_key = sh.canonical_session_key AND h.hour = sh.window_start
			WHERE h.is_terminal_tail_artifact = 0
		),
		inventory_keys AS (
			SELECT DISTINCT
				org,
				orchestrator_address,
				coalesce(nullIf(model_id, ''), nullIf(pipeline_id, '')) AS inventory_key
			FROM base
			WHERE orchestrator_address != ''
		),
		inventory AS (
			SELECT
				inv.org,
				inv.orch_address AS orchestrator_address,
				coalesce(nullIf(inv.model_id, ''), nullIf(inv.pipeline_id, '')) AS inventory_key,
				any(inv.model_id) AS inventory_model_id,
				cast(nullIf(argMaxIfMerge(inv.gpu_id_state), ''), 'Nullable(String)') AS inventory_gpu_id,
				nullIf(argMaxIfMerge(inv.gpu_model_name_state), '') AS inventory_gpu_model_name,
				nullIf(argMaxIfMerge(inv.gpu_memory_bytes_total_state), toUInt64(0)) AS inventory_gpu_memory_bytes_total,
				nullIf(argMaxIfMerge(inv.runner_version_state), '') AS inventory_runner_version,
				nullIf(argMaxIfMerge(inv.cuda_version_state), '') AS inventory_cuda_version
			FROM naap.canonical_latest_orchestrator_pipeline_inventory_agg inv
			INNER JOIN inventory_keys k
				ON inv.org = k.org
			   AND inv.orch_address = k.orchestrator_address
			   AND coalesce(nullIf(inv.model_id, ''), nullIf(inv.pipeline_id, '')) = k.inventory_key
			GROUP BY inv.org, orchestrator_address, inventory_key
		)
			SELECT
				b.window_start,
				b.org,
				b.orchestrator_address,
				b.pipeline_id,
				coalesce(b.model_id, i.inventory_model_id) AS model_id,
				coalesce(b.session_gpu_id, i.inventory_gpu_id) AS gpu_id,
				cast(null AS Nullable(String)) AS region,
			if(sum(b.status_samples) > 0, sum(b.output_fps_sum) / toFloat64(sum(b.status_samples)), 0.0) AS avg_output_fps,
			sum(b.output_fps_sum) AS output_fps_sum,
			quantileTDigest(0.95)(b.session_hour_avg_output_fps) AS p95_output_fps,
			quantileTDigestState(0.95)(b.session_hour_avg_output_fps) AS output_fps_p95_state,
			cast(null AS Nullable(Float64)) AS fps_jitter_coefficient,
			toUInt64(sum(b.status_samples)) AS status_samples,
			toUInt64(sum(b.error_status_samples)) AS error_status_samples,
			toUInt64(sum(b.health_signal_count)) AS health_signal_count,
			toUInt64(sum(b.health_expected_signal_count)) AS health_expected_signal_count,
			least(
				if(
					sum(b.health_expected_signal_count) > 0,
					sum(b.health_signal_count) / toFloat64(sum(b.health_expected_signal_count)),
					1.0
				),
				1.0
			) AS health_signal_coverage_ratio,
			i.inventory_gpu_model_name AS gpu_model_name,
			i.inventory_gpu_memory_bytes_total AS gpu_memory_bytes_total,
			i.inventory_runner_version AS runner_version,
			i.inventory_cuda_version AS cuda_version,
			if(sum(b.prompt_to_first_frame_sample_count) > 0, sum(b.prompt_to_first_frame_sum_ms) / toFloat64(sum(b.prompt_to_first_frame_sample_count)), cast(null AS Nullable(Float64))) AS avg_prompt_to_first_frame_ms,
			sum(b.prompt_to_first_frame_sum_ms) AS prompt_to_first_frame_sum_ms,
			if(sum(b.startup_latency_sample_count) > 0, sum(b.startup_latency_sum_ms) / toFloat64(sum(b.startup_latency_sample_count)), cast(null AS Nullable(Float64))) AS avg_startup_latency_ms,
			sum(b.startup_latency_sum_ms) AS startup_latency_sum_ms,
			if(sum(b.e2e_latency_sample_count) > 0, sum(b.e2e_latency_sum_ms) / toFloat64(sum(b.e2e_latency_sample_count)), cast(null AS Nullable(Float64))) AS avg_e2e_latency_ms,
			sum(b.e2e_latency_sum_ms) AS e2e_latency_sum_ms,
			if(
				countIf(ifNull(b.prompt_to_playable_latency_ms, 0.0) > 0) > 0,
				quantileTDigestIf(0.95)(ifNull(b.prompt_to_playable_latency_ms, 0.0), toUInt8(ifNull(b.prompt_to_playable_latency_ms, 0.0) > 0)),
				cast(null AS Nullable(Float64))
			) AS p95_prompt_to_first_frame_latency_ms,
			quantileTDigestIfState(0.95)(ifNull(b.prompt_to_playable_latency_ms, 0.0), toUInt8(ifNull(b.prompt_to_playable_latency_ms, 0.0) > 0)) AS prompt_to_first_frame_p95_state,
			if(
				countIf(ifNull(b.startup_latency_ms, 0.0) > 0) > 0,
				quantileTDigestIf(0.95)(ifNull(b.startup_latency_ms, 0.0), toUInt8(ifNull(b.startup_latency_ms, 0.0) > 0)),
				cast(null AS Nullable(Float64))
			) AS p95_startup_latency_ms,
			quantileTDigestIfState(0.95)(ifNull(b.startup_latency_ms, 0.0), toUInt8(ifNull(b.startup_latency_ms, 0.0) > 0)) AS startup_latency_p95_state,
			if(
				countIf(ifNull(b.session_hour_avg_e2e_latency_ms, 0.0) > 0) > 0,
				quantileTDigestIf(0.95)(ifNull(b.session_hour_avg_e2e_latency_ms, 0.0), toUInt8(ifNull(b.session_hour_avg_e2e_latency_ms, 0.0) > 0)),
				cast(null AS Nullable(Float64))
			) AS p95_e2e_latency_ms,
			quantileTDigestIfState(0.95)(ifNull(b.session_hour_avg_e2e_latency_ms, 0.0), toUInt8(ifNull(b.session_hour_avg_e2e_latency_ms, 0.0) > 0)) AS e2e_latency_p95_state,
			toUInt64(sum(b.prompt_to_first_frame_sample_count)) AS prompt_to_first_frame_sample_count,
			toUInt64(sum(b.startup_latency_sample_count)) AS startup_latency_sample_count,
			toUInt64(sum(b.e2e_latency_sample_count)) AS e2e_latency_sample_count,
			toUInt64(countIf(b.requested_seen = 1)) AS known_sessions_count,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'success')) AS startup_success_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.selection_outcome = 'no_orch')) AS no_orch_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason != 'none')) AS startup_excused_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason = 'none')) AS startup_failed_sessions,
			toUInt64(0) AS confirmed_swapped_sessions,
			toUInt64(0) AS inferred_swap_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.swap_count > 0)) AS total_swapped_sessions,
			toUInt64(countIf(b.requested_seen = 1 AND b.error_seen = 1)) AS sessions_ending_in_error,
			ifNull(toFloat64(countIf(b.requested_seen = 1 AND b.startup_outcome = 'failed' AND b.excusal_reason = 'none')) / nullIf(toFloat64(countIf(b.requested_seen = 1)), 0.0), 0.0) AS startup_failed_rate,
			ifNull(toFloat64(countIf(b.requested_seen = 1 AND b.swap_count > 0)) / nullIf(toFloat64(countIf(b.requested_seen = 1)), 0.0), 0.0) AS swap_rate,
			?,
			?,
			now64()
		FROM base b
		LEFT JOIN inventory i
			ON b.org = i.org
		   AND b.orchestrator_address = i.orchestrator_address
		   AND coalesce(nullIf(b.model_id, ''), nullIf(b.pipeline_id, '')) = i.inventory_key
		WHERE ifNull(coalesce(b.session_gpu_id, i.inventory_gpu_id), '') != ''
		GROUP BY
			b.window_start,
			b.org,
			b.orchestrator_address,
			b.pipeline_id,
			coalesce(b.model_id, i.inventory_model_id),
			coalesce(b.session_gpu_id, i.inventory_gpu_id),
			i.inventory_gpu_model_name,
			i.inventory_gpu_memory_bytes_total,
			i.inventory_runner_version,
			i.inventory_cuda_version
	`, queryID, queryID, runID, r.cfg.ResolverVersion)
}

func (r *repo) ownerID() string {
	return r.owner
}

func nullableStringValue(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func nullableTimeValue(v *time.Time) any {
	if v == nil || v.IsZero() {
		return nil
	}
	return v.UTC()
}

func nullableUint64Value(v *uint64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableFloat64Value(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func (r *repo) stageSessionKeys(ctx context.Context, refs []sessionKeyRef) (string, error) {
	queryID := stableHash("session-keys", fmt.Sprintf("%d", time.Now().UTC().UnixNano()), fmt.Sprintf("%d", len(refs)))
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.resolver_query_session_keys
		(
			query_id, org, canonical_session_key, created_at
		)
	`)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, ref := range refs {
		if err := batch.Append(queryID, ref.Org, ref.SessionKey, now); err != nil {
			return "", err
		}
	}
	if err := batch.Send(); err != nil {
		return "", err
	}
	return queryID, nil
}

func (r *repo) stageSelectionEventIDs(ctx context.Context, ids []string) (string, error) {
	queryID := stableHash("selection-ids", fmt.Sprintf("%d", time.Now().UTC().UnixNano()), fmt.Sprintf("%d", len(ids)))
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.resolver_query_selection_event_ids
		(
			query_id, selection_event_id, created_at
		)
	`)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, id := range ids {
		if err := batch.Append(queryID, id, now); err != nil {
			return "", err
		}
	}
	if err := batch.Send(); err != nil {
		return "", err
	}
	return queryID, nil
}

func (r *repo) stageWindowSlices(ctx context.Context, refs []windowSliceRef) (string, error) {
	queryID := stableHash("window-slices", fmt.Sprintf("%d", time.Now().UTC().UnixNano()), fmt.Sprintf("%d", len(refs)))
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.resolver_query_window_slices
		(
			query_id, org, window_start, created_at
		)
	`)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, ref := range refs {
		if err := batch.Append(queryID, ref.Org, ref.WindowStart.UTC(), now); err != nil {
			return "", err
		}
	}
	if err := batch.Send(); err != nil {
		return "", err
	}
	return queryID, nil
}

func (r *repo) stageIdentities(ctx context.Context, identities []string) (string, error) {
	if len(identities) == 0 {
		return "", nil
	}
	queryID := stableHash("identities", fmt.Sprintf("%d", time.Now().UTC().UnixNano()), fmt.Sprintf("%d", len(identities)))
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.resolver_query_identities
		(
			query_id, identity, created_at
		)
	`)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, identity := range identities {
		if identity == "" {
			continue
		}
		if err := batch.Append(queryID, identity, now); err != nil {
			return "", err
		}
	}
	if err := batch.Send(); err != nil {
		return "", err
	}
	return queryID, nil
}

func (r *repo) stageEventIDs(ctx context.Context, eventIDs []string) (string, error) {
	if len(eventIDs) == 0 {
		return "", nil
	}
	queryID := stableHash("event-ids", fmt.Sprintf("%d", time.Now().UTC().UnixNano()), fmt.Sprintf("%d", len(eventIDs)))
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.resolver_query_event_ids
		(
			query_id, event_id, created_at
		)
	`)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, eventID := range eventIDs {
		if eventID == "" {
			continue
		}
		if err := batch.Append(queryID, eventID, now); err != nil {
			return "", err
		}
	}
	if err := batch.Send(); err != nil {
		return "", err
	}
	return queryID, nil
}

func (r *repo) fetchEventLineage(ctx context.Context, eventIDs []string) (map[string]eventLineage, error) {
	queryID, err := r.stageEventIDs(ctx, eventIDs)
	if err != nil {
		return nil, err
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			e.event_id,
			ifNull(e.source_topic, '') AS source_topic,
			toInt32(ifNull(e.source_partition, 0)) AS source_partition,
			toInt64(ifNull(e.source_offset, 0)) AS source_offset
		FROM naap.accepted_raw_events e
		INNER JOIN naap.resolver_query_event_ids i
			ON i.query_id = ?
		   AND i.event_id = e.event_id
	`, queryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]eventLineage, len(eventIDs))
	for rows.Next() {
		var eventID string
		var row eventLineage
		if err := rows.Scan(&eventID, &row.SourceTopic, &row.SourcePart, &row.SourceOffset); err != nil {
			return nil, err
		}
		out[eventID] = row
	}
	return out, rows.Err()
}

func selectionCandidateEventIDs(rows []selectionCandidate) []string {
	seen := make(map[string]struct{}, len(rows))
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.EventID == "" {
			continue
		}
		if _, ok := seen[row.EventID]; ok {
			continue
		}
		seen[row.EventID] = struct{}{}
		out = append(out, row.EventID)
	}
	return out
}

func (r *repo) fetchCurrentSessionRowHashes(ctx context.Context, refs []sessionKeyRef) (map[string]string, error) {
	if len(refs) == 0 {
		return map[string]string{}, nil
	}
	queryID, err := r.stageSessionKeys(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("stage session keys for current session hashes: %w", err)
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			c.canonical_session_key,
			argMax(c.canonical_pipeline, c.materialized_at) AS canonical_pipeline,
			argMax(ifNull(c.canonical_model, ''), c.materialized_at) AS canonical_model,
			argMax(c.selection_outcome, c.materialized_at) AS selection_outcome,
			argMax(c.startup_outcome, c.materialized_at) AS startup_outcome,
			argMax(c.excusal_reason, c.materialized_at) AS excusal_reason,
			argMax(c.attribution_status, c.materialized_at) AS attribution_status,
			argMax(c.attribution_reason, c.materialized_at) AS attribution_reason,
			argMax(ifNull(c.attributed_orch_address, ''), c.materialized_at) AS attributed_orch_address,
			argMax(ifNull(c.attributed_orch_uri, ''), c.materialized_at) AS attributed_orch_uri,
			argMax(ifNull(c.startup_latency_ms, 0), c.materialized_at) AS startup_latency_ms,
			argMax(ifNull(c.e2e_latency_ms, 0), c.materialized_at) AS e2e_latency_ms,
			argMax(ifNull(c.prompt_to_playable_latency_ms, 0), c.materialized_at) AS prompt_to_playable_latency_ms,
			argMax(c.status_sample_count, c.materialized_at) AS status_sample_count,
			argMax(c.health_signal_count, c.materialized_at) AS health_signal_count,
			argMax(c.startup_error_count, c.materialized_at) AS startup_error_count,
			argMax(c.excusable_error_count, c.materialized_at) AS excusable_error_count
		FROM naap.canonical_session_current_store c
		INNER JOIN naap.resolver_query_session_keys i
			ON i.query_id = ? AND i.org = c.org AND i.canonical_session_key = c.canonical_session_key
		GROUP BY c.canonical_session_key
	`, queryID)
	if err != nil {
		return nil, fmt.Errorf("fetch current session hashes: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(refs))
	for rows.Next() {
		var (
			sessionKey, canonicalPipeline, canonicalModel                                         string
			selectionOutcome, startupOutcome, excusalReason, attributionStatus, attributionReason string
			attributedOrchAddress, attributedOrchURI                                              string
			startupLatencyMS, e2eLatencyMS, promptToPlayableLatencyMS                             float64
			statusSampleCount, healthSignalCount, startupErrorCount, excusableErrorCount          uint64
		)
		if err := rows.Scan(
			&sessionKey,
			&canonicalPipeline,
			&canonicalModel,
			&selectionOutcome,
			&startupOutcome,
			&excusalReason,
			&attributionStatus,
			&attributionReason,
			&attributedOrchAddress,
			&attributedOrchURI,
			&startupLatencyMS,
			&e2eLatencyMS,
			&promptToPlayableLatencyMS,
			&statusSampleCount,
			&healthSignalCount,
			&startupErrorCount,
			&excusableErrorCount,
		); err != nil {
			return nil, fmt.Errorf("scan current session hash: %w", err)
		}
		out[sessionKey] = stableHash(
			sessionKey,
			canonicalPipeline,
			canonicalModel,
			selectionOutcome,
			startupOutcome,
			excusalReason,
			attributionStatus,
			attributionReason,
			attributedOrchAddress,
			attributedOrchURI,
			fmt.Sprintf("%.3f", startupLatencyMS),
			fmt.Sprintf("%.3f", e2eLatencyMS),
			fmt.Sprintf("%.3f", promptToPlayableLatencyMS),
			fmt.Sprintf("%d", statusSampleCount),
			fmt.Sprintf("%d", healthSignalCount),
			fmt.Sprintf("%d", startupErrorCount),
			fmt.Sprintf("%d", excusableErrorCount),
		)
	}
	return out, rows.Err()
}

func (r *repo) fetchCurrentStatusHourRowHashes(ctx context.Context, refs []sessionKeyRef, rowsToMatch []StatusHourRow) (map[string]string, error) {
	if len(refs) == 0 || len(rowsToMatch) == 0 {
		return map[string]string{}, nil
	}
	queryID, err := r.stageSessionKeys(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("stage session keys for current status hour hashes: %w", err)
	}
	minHour := rowsToMatch[0].Hour.UTC()
	maxHour := rowsToMatch[0].Hour.UTC()
	for _, row := range rowsToMatch[1:] {
		hour := row.Hour.UTC()
		if hour.Before(minHour) {
			minHour = hour
		}
		if hour.After(maxHour) {
			maxHour = hour
		}
	}
	rows, err := r.conn.Query(ctx, `
		SELECT
			s.canonical_session_key,
			s.hour,
			argMax(s.canonical_pipeline, s.refreshed_at) AS canonical_pipeline,
			argMax(ifNull(s.canonical_model, ''), s.refreshed_at) AS canonical_model,
			argMax(s.attribution_status, s.refreshed_at) AS attribution_status,
			argMax(ifNull(s.startup_latency_ms, 0), s.refreshed_at) AS startup_latency_ms,
			argMax(ifNull(s.avg_e2e_latency_ms, 0), s.refreshed_at) AS avg_e2e_latency_ms,
			argMax(ifNull(s.prompt_to_playable_latency_ms, 0), s.refreshed_at) AS prompt_to_playable_latency_ms,
			argMax(s.status_samples, s.refreshed_at) AS status_samples
		FROM naap.canonical_status_hours_store s
		INNER JOIN naap.resolver_query_session_keys i
			ON i.query_id = ? AND i.org = s.org AND i.canonical_session_key = s.canonical_session_key
		WHERE s.hour >= ? AND s.hour <= ?
		GROUP BY s.canonical_session_key, s.hour
	`, queryID, minHour, maxHour)
	if err != nil {
		return nil, fmt.Errorf("fetch current status hour hashes: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(rowsToMatch))
	for rows.Next() {
		var (
			sessionKey, canonicalPipeline, canonicalModel, attributionStatus string
			hour                                                             time.Time
			startupLatencyMS, avgE2ELatencyMS, promptToPlayableLatencyMS     float64
			statusSamples                                                    uint64
		)
		if err := rows.Scan(
			&sessionKey,
			&hour,
			&canonicalPipeline,
			&canonicalModel,
			&attributionStatus,
			&startupLatencyMS,
			&avgE2ELatencyMS,
			&promptToPlayableLatencyMS,
			&statusSamples,
		); err != nil {
			return nil, fmt.Errorf("scan current status hour hash: %w", err)
		}
		key := stableHash(sessionKey, hour.UTC().Format(time.RFC3339))
		out[key] = stableHash(
			sessionKey,
			hour.UTC().Format(time.RFC3339),
			canonicalPipeline,
			canonicalModel,
			attributionStatus,
			fmt.Sprintf("%.3f", startupLatencyMS),
			fmt.Sprintf("%.3f", avgE2ELatencyMS),
			fmt.Sprintf("%.3f", promptToPlayableLatencyMS),
			fmt.Sprintf("%d", statusSamples),
		)
	}
	return out, rows.Err()
}

func sessionCurrentRowHash(row SessionCurrentRow) string {
	return stableHash(
		row.SessionKey,
		row.StreamID,
		row.RequestID,
		row.Gateway,
		row.CanonicalPipeline,
		row.CanonicalModel,
		row.SelectionOutcome,
		row.StartupOutcome,
		row.ExcusalReason,
		row.AttributionStatus,
		row.AttributionReason,
		row.AttributedOrchAddress,
		row.AttributedOrchURI,
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.StartupLatencyMS)),
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.E2ELatencyMS)),
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.PromptToPlayableLatencyMS)),
		fmt.Sprintf("%d", row.StatusSampleCount),
		fmt.Sprintf("%d", row.HealthSignalCount),
		fmt.Sprintf("%d", row.StartupErrorCount),
		fmt.Sprintf("%d", row.ExcusableErrorCount),
	)
}

func statusHourRowHash(row StatusHourRow) string {
	return stableHash(
		row.SessionKey,
		row.Hour.UTC().Format(time.RFC3339),
		row.CanonicalPipeline,
		row.CanonicalModel,
		row.AttributionStatus,
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.StartupLatencyMS)),
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.E2ELatencyMS)),
		fmt.Sprintf("%.3f", nullableFloat64HashValue(row.PromptToPlayableLatencyMS)),
		fmt.Sprintf("%d", row.StatusSamples),
	)
}

func nullableFloat64HashValue(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

// workerLifecycleLookback is how far before a window start we look for
// worker_lifecycle snapshots. BYOC workers may have registered well before
// the attribution window; 30 days covers typical deployments.
const workerLifecycleLookback = 30 * 24 * time.Hour

// orchIdentitiesToStrings converts a slice of orchIdentity to a flat list of
// normalized identity strings suitable for stageIdentities.
func orchIdentitiesToStrings(identities []orchIdentity) []string {
	seen := make(map[string]struct{}, len(identities)*2)
	out := make([]string, 0, len(identities)*2)
	for _, id := range identities {
		if id.Address != "" {
			if _, ok := seen[id.Address]; !ok {
				seen[id.Address] = struct{}{}
				out = append(out, id.Address)
			}
		}
		if id.URINorm != "" {
			if _, ok := seen[id.URINorm]; !ok {
				seen[id.URINorm] = struct{}{}
				out = append(out, id.URINorm)
			}
		}
	}
	return out
}

// fetchAIBatchJobCandidates returns AI batch jobs that completed within the
// window. It LEFT JOINs the received event onto the completed event so that
// ReceivedAt is populated when available (used as SelectionTS anchor).
// Jobs with an empty request_id are excluded — those belong to the known gap
// period before request_id tracking was fixed.
func (r *repo) fetchAIBatchJobCandidates(ctx context.Context, spec WindowSpec) ([]AIBatchJobRecord, error) {
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("fetch ai batch job candidates requires bounded window")
	}
	orgClause, orgArgs := orgPredicate("c.org", spec.Org, spec.ExcludedOrgPrefixes)
	args := []any{spec.Start.UTC(), spec.End.UTC()}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT
			c.request_id,
			c.org,
			ifNull(c.gateway, '') AS gateway,
			ifNull(c.pipeline, '') AS pipeline,
			ifNull(c.model_id, '') AS model_id,
			ifNull(c.orch_url, '') AS orch_url,
			ifNull(lowerUTF8(c.orch_url), '') AS orch_url_norm,
			received.received_at,
			c.event_ts AS completed_at,
			c.success,
			ifNull(toUInt16OrZero(toString(c.tries)), 0) AS tries,
			ifNull(c.duration_ms, 0) AS duration_ms,
			ifNull(c.latency_score, 0.0) AS latency_score,
			ifNull(c.price_per_unit, 0.0) AS price_per_unit,
			ifNull(c.error_type, '') AS error_type,
			ifNull(c.error, '') AS error
		FROM naap.normalized_ai_batch_job AS c FINAL
		LEFT JOIN (
			SELECT request_id, org, event_ts AS received_at
			FROM naap.normalized_ai_batch_job FINAL
			WHERE subtype = 'ai_batch_request_received'
		) received ON received.org = c.org AND received.request_id = c.request_id
		WHERE c.subtype = 'ai_batch_request_completed'
		  AND c.event_ts >= ? AND c.event_ts < ?
		  AND c.request_id != ''
		  AND `+orgClause+`
		ORDER BY c.org, c.request_id, c.event_ts
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch ai batch job candidates: %w", err)
	}
	defer rows.Close()

	var out []AIBatchJobRecord
	for rows.Next() {
		var j AIBatchJobRecord
		var receivedAt sql.NullTime
		if err := rows.Scan(
			&j.RequestID, &j.Org, &j.Gateway, &j.Pipeline, &j.ModelID,
			&j.OrchURL, &j.OrchURLNorm,
			&receivedAt,
			&j.CompletedAt, &j.Success,
			&j.Tries, &j.DurationMS, &j.LatencyScore, &j.PricePerUnit,
			&j.ErrorType, &j.Error,
		); err != nil {
			return nil, fmt.Errorf("scan ai batch job candidate: %w", err)
		}
		if receivedAt.Valid {
			t := receivedAt.Time.UTC()
			j.ReceivedAt = &t
		}
		j.CompletedAt = j.CompletedAt.UTC()
		out = append(out, j)
	}
	return out, rows.Err()
}

// fetchBYOCJobCandidates returns BYOC job gateway completion events within
// the window. The event_id field is used as the stable job key.
func (r *repo) fetchBYOCJobCandidates(ctx context.Context, spec WindowSpec) ([]BYOCJobRecord, error) {
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("fetch byoc job candidates requires bounded window")
	}
	orgClause, orgArgs := orgPredicate("j.org", spec.Org, spec.ExcludedOrgPrefixes)
	args := []any{spec.Start.UTC(), spec.End.UTC()}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT
			j.event_id,
			j.org,
			ifNull(j.gateway, '') AS gateway,
			ifNull(j.capability, '') AS capability,
			ifNull(lowerUTF8(j.orch_address), '') AS orch_address,
			ifNull(j.orch_url, '') AS orch_url,
			ifNull(lowerUTF8(j.orch_url), '') AS orch_url_norm,
			ifNull(j.worker_url, '') AS worker_url,
			ifNull(j.charged_compute, 0) AS charged_compute,
			j.event_ts AS completed_at,
			j.success,
			ifNull(j.duration_ms, 0) AS duration_ms,
			ifNull(j.http_status, 0) AS http_status,
			ifNull(j.error, '') AS error
		FROM naap.normalized_byoc_job AS j FINAL
		WHERE j.subtype = 'job_gateway_completed'
		  AND j.event_ts >= ? AND j.event_ts < ?
		  AND `+orgClause+`
		ORDER BY j.org, j.event_id, j.event_ts
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch byoc job candidates: %w", err)
	}
	defer rows.Close()

	var out []BYOCJobRecord
	for rows.Next() {
		var j BYOCJobRecord
		if err := rows.Scan(
			&j.EventID, &j.Org, &j.Gateway, &j.Capability,
			&j.OrchAddress, &j.OrchURL, &j.OrchURLNorm,
			&j.WorkerURL, &j.ChargedCompute,
			&j.CompletedAt, &j.Success,
			&j.DurationMS, &j.HTTPStatus, &j.Error,
		); err != nil {
			return nil, fmt.Errorf("scan byoc job candidate: %w", err)
		}
		j.CompletedAt = j.CompletedAt.UTC()
		out = append(out, j)
	}
	return out, rows.Err()
}

// fetchWorkerLifecycleSnapshots returns worker_lifecycle events for the given
// orchestrator addresses, looking back workerLifecycleLookback before the
// window start. This covers BYOC workers that registered well before the
// current attribution window.
func (r *repo) fetchWorkerLifecycleSnapshots(ctx context.Context, spec WindowSpec, orchAddresses []string) ([]workerLifecycleSnapshot, error) {
	if len(orchAddresses) == 0 {
		return nil, nil
	}
	if spec.Start == nil || spec.End == nil {
		return nil, fmt.Errorf("fetch worker lifecycle snapshots requires bounded window")
	}
	queryID, err := r.stageIdentities(ctx, orchAddresses)
	if err != nil {
		return nil, fmt.Errorf("stage identities for worker lifecycle: %w", err)
	}
	orgClause, orgArgs := orgPredicate("w.org", spec.Org, spec.ExcludedOrgPrefixes)
	lookbackStart := spec.Start.UTC().Add(-workerLifecycleLookback)
	args := []any{queryID, lookbackStart, spec.End.UTC()}
	args = append(args, orgArgs...)
	rows, err := r.conn.Query(ctx, `
		SELECT
			w.org,
			ifNull(w.capability, '') AS capability,
			ifNull(lowerUTF8(w.orch_address), '') AS orch_address,
			w.event_ts,
			ifNull(w.model, '') AS model,
			ifNull(w.price_per_unit, 0.0) AS price_per_unit,
			ifNull(w.worker_url, '') AS worker_url
		FROM naap.normalized_worker_lifecycle AS w FINAL
		INNER JOIN naap.resolver_query_identities i
			ON i.query_id = ?
		   AND i.identity = lowerUTF8(ifNull(w.orch_address, ''))
		WHERE w.event_ts >= ? AND w.event_ts < ?
		  AND `+orgClause+`
		ORDER BY w.org, w.capability, w.orch_address, w.event_ts
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch worker lifecycle snapshots: %w", err)
	}
	defer rows.Close()

	var out []workerLifecycleSnapshot
	for rows.Next() {
		var s workerLifecycleSnapshot
		if err := rows.Scan(
			&s.Org, &s.Capability, &s.OrchAddress,
			&s.EventTS, &s.Model, &s.PricePerUnit, &s.WorkerURL,
		); err != nil {
			return nil, fmt.Errorf("scan worker lifecycle snapshot: %w", err)
		}
		s.EventTS = s.EventTS.UTC()
		out = append(out, s)
	}
	return out, rows.Err()
}

// insertAIBatchJobRows upserts AI batch job attribution rows into
// canonical_ai_batch_job_store. The ReplacingMergeTree on materialized_at
// means a later write for the same (org, request_id, completed_at) supersedes
// the previous one.
func (r *repo) insertAIBatchJobRows(ctx context.Context, runID string, rows []AIBatchJobRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_ai_batch_job_store
		(
			request_id, org, gateway, pipeline, model_id,
			received_at, completed_at,
			success, tries, duration_ms,
			orch_url, orch_url_norm,
			latency_score, price_per_unit, error_type, error,
			attribution_status, attribution_reason, attribution_method, attribution_confidence,
			attributed_orch_uri, capability_version_id, attribution_snapshot_ts,
			gpu_id, gpu_model_name, gpu_memory_bytes_total, attributed_model,
			resolver_run_id, materialized_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare ai batch job row batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.RequestID, row.Org, row.Gateway, row.Pipeline, row.ModelID,
			nullableTimeValue(row.ReceivedAt), row.CompletedAt.UTC(),
			row.Success, row.Tries, row.DurationMS,
			row.OrchURL, row.OrchURLNorm,
			row.LatencyScore, row.PricePerUnit, row.ErrorType, row.Error,
			row.AttributionStatus, row.AttributionReason, row.AttributionMethod, row.AttributionConfidence,
			nullableStringValue(row.AttributedOrchURI), nullableStringValue(row.CapabilityVersionID),
			nullableTimeValue(row.AttributionSnapshotTS),
			nullableStringValue(row.GPUID), nullableStringValue(row.GPUModelName),
			row.GPUMemoryBytesTotal,
			nullableStringValue(row.AttributedModel),
			runID, now,
		); err != nil {
			return fmt.Errorf("append ai batch job row: %w", err)
		}
	}
	return batch.Send()
}

// insertBYOCJobRows upserts BYOC job attribution rows into
// canonical_byoc_job_store. The ReplacingMergeTree on materialized_at
// means a later write for the same (org, event_id, completed_at) supersedes
// the previous one.
func (r *repo) insertBYOCJobRows(ctx context.Context, runID string, rows []BYOCJobRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO naap.canonical_byoc_job_store
		(
			event_id, org, gateway, capability, completed_at,
			success, duration_ms, http_status,
			orch_address, orch_url, orch_url_norm,
			worker_url, charged_compute, error,
			model, price_per_unit,
			attribution_status, attribution_reason, attribution_method, attribution_confidence,
			attributed_orch_uri, capability_version_id, attribution_snapshot_ts,
			gpu_id, gpu_model_name, gpu_memory_bytes_total,
			resolver_run_id, materialized_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare byoc job row batch: %w", err)
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.EventID, row.Org, row.Gateway, row.Capability, row.CompletedAt.UTC(),
			row.Success, row.DurationMS, row.HTTPStatus,
			row.OrchAddress, row.OrchURL, row.OrchURLNorm,
			row.WorkerURL, row.ChargedCompute, row.Error,
			nullableStringValue(row.Model), row.PricePerUnit,
			row.AttributionStatus, row.AttributionReason, row.AttributionMethod, row.AttributionConfidence,
			nullableStringValue(row.AttributedOrchURI), nullableStringValue(row.CapabilityVersionID),
			nullableTimeValue(row.AttributionSnapshotTS),
			nullableStringValue(row.GPUID), nullableStringValue(row.GPUModelName),
			row.GPUMemoryBytesTotal,
			runID, now,
		); err != nil {
			return fmt.Errorf("append byoc job row: %w", err)
		}
	}
	return batch.Send()
}
