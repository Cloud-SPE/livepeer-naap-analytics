package replay

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// LoadConfig parameterises a fixture load.
//
// The loader uses the ClickHouse HTTP interface (JSONEachRow) rather than the
// native-protocol batch API because each INSERT into accepted_raw_events
// triggers a cascade of 16 materialized views. Native batches have the client
// stream blocks in lockstep with the server, which deadlocks under MV
// backpressure and trips the 300s socket timeout. One streaming HTTP POST
// keeps the server in charge of the pipeline and flushes MVs naturally.
type LoadConfig struct {
	Database     string
	HTTPAddr     string // host:port for the ClickHouse HTTP interface (e.g. localhost:8123)
	HTTPScheme   string // http or https; defaults to http
	User         string
	Password     string
	ReadTimeout  time.Duration // client-side response deadline
	WriteTimeout time.Duration // client-side request-body deadline
}

// LoadFixture truncates the raw and normalized layers, then streams the
// fixture archive into accepted_raw_events via HTTP JSONEachRow.
//
// Truncating normalized_* alongside accepted_raw_events matters because the
// normalized MVs are trigger-on-INSERT — rows from a prior run would
// otherwise mix with the fixture's downstream propagation.
//
// Returns the row count reported by ClickHouse's X-ClickHouse-Summary header.
func LoadFixture(
	ctx context.Context,
	conn ch.Conn,
	lc LoadConfig,
	archivePath string,
	manifest *Manifest,
) (int64, error) {
	if err := truncateLayers(ctx, conn, lc.Database, LayerRaw, LayerNormalized); err != nil {
		return 0, err
	}
	if lc.HTTPScheme == "" {
		lc.HTTPScheme = "http"
	}
	if lc.ReadTimeout == 0 {
		lc.ReadTimeout = 30 * time.Minute
	}
	if lc.WriteTimeout == 0 {
		lc.WriteTimeout = 30 * time.Minute
	}
	if lc.Database == "" {
		lc.Database = "naap"
	}

	cmd := exec.CommandContext(ctx, "zstd", "-d", "-c", archivePath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, fmt.Errorf("zstd pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("zstd start: %w", err)
	}
	// Ensure we reap the zstd process in every path.
	defer func() {
		_ = cmd.Wait()
	}()

	_, err = postInsert(ctx, lc, stdout)
	if err != nil {
		return 0, err
	}

	// Validate against the authoritative count: accepted_raw_events after
	// the load. The server's X-ClickHouse-Summary written_rows sums across
	// the raw INSERT *and* every cascading MV write (16 MVs plus their
	// AggregatingMergeTree state columns), so it is not directly comparable
	// to the manifest's raw-only row count.
	//
	// clickhouse-go v2 in strict mode rejects converting UInt64 to *int64,
	// so we scan into uint64 and narrow afterwards.
	var acceptedU uint64
	if err := conn.QueryRow(ctx,
		fmt.Sprintf("SELECT count() FROM %s.accepted_raw_events", lc.Database),
	).Scan(&acceptedU); err != nil {
		return 0, fmt.Errorf("validate accepted_raw_events count: %w", err)
	}
	accepted := int64(acceptedU)
	if manifest != nil && manifest.RowCount > 0 {
		delta := accepted - manifest.RowCount
		if delta < 0 {
			delta = -delta
		}
		// Ambient writes from live Kafka consumers may land between the
		// initial TRUNCATE and the end of our INSERT on environments that
		// have the Kafka Engine tables active. We allow a 1% slack and
		// surface the actual mismatch as part of the report later.
		if float64(delta)/float64(manifest.RowCount) > 0.01 {
			return accepted, fmt.Errorf(
				"accepted_raw_events count diverges >1%% from manifest: got=%d manifest=%d delta=%d",
				accepted, manifest.RowCount, delta,
			)
		}
	}
	return accepted, nil
}

// postInsert streams body to the ClickHouse HTTP endpoint as an INSERT.
// Returns the row count reported by the X-ClickHouse-Summary header on
// successful completion.
func postInsert(ctx context.Context, lc LoadConfig, body io.Reader) (int64, error) {
	// Query-side settings:
	//   - JSONEachRow parses the NDJSON stream into typed rows.
	//   - date_time_input_format=best_effort handles the "YYYY-MM-DD HH:MM:SS.sss" shape.
	//   - input_format_null_as_default keeps NULL schema_version parseable.
	//   - max_insert_block_size moderates MV backpressure (default is 1M which is too aggressive here).
	//   - async_insert is OFF so the HTTP response blocks until the full pipeline drains.
	q := url.Values{}
	q.Set("database", lc.Database)
	q.Set("query", "INSERT INTO "+lc.Database+".accepted_raw_events FORMAT JSONEachRow")
	q.Set("date_time_input_format", "best_effort")
	q.Set("input_format_null_as_default", "1")
	q.Set("max_insert_block_size", "65536")
	q.Set("async_insert", "0")
	// The harness runs at most ~5 rows wide; this setting keeps the server
	// from issuing its own 300s receive timeout before we finish streaming
	// the full archive (NDJSON can be several GB uncompressed).
	q.Set("receive_timeout", "3600")
	q.Set("send_timeout", "3600")

	u := fmt.Sprintf("%s://%s/?%s", lc.HTTPScheme, lc.HTTPAddr, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(lc.User, lc.Password)
	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{
		Timeout: lc.ReadTimeout + lc.WriteTimeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf(
			"clickhouse insert returned status=%d body=%s",
			resp.StatusCode, truncateString(string(respBody), 512),
		)
	}
	return parseSummaryRows(resp.Header.Get("X-ClickHouse-Summary")), nil
}

// truncateLayers empties every table listed under the given layers. Missing
// tables are tolerated (IF EXISTS) so the harness stays forward-compatible
// with schema evolution during the Phase 1–8 rollout.
func truncateLayers(ctx context.Context, conn ch.Conn, database string, layers ...Layer) error {
	for _, layer := range layers {
		for _, t := range TablesForLayer(layer) {
			stmt := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s", database, t)
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("truncate %s.%s: %w", database, t, err)
			}
		}
	}
	return nil
}

// parseSummaryRows extracts written_rows from the X-ClickHouse-Summary header.
// The header is a JSON object like:
//
//	{"read_rows":"0","read_bytes":"0","written_rows":"5761225","written_bytes":"..."}
//
// We avoid a full JSON parse to keep this dependency-free.
func parseSummaryRows(h string) int64 {
	const key = `"written_rows":"`
	i := indexOf(h, key)
	if i < 0 {
		return 0
	}
	j := indexOf(h[i+len(key):], `"`)
	if j < 0 {
		return 0
	}
	var out int64
	for _, c := range h[i+len(key) : i+len(key)+j] {
		if c < '0' || c > '9' {
			return 0
		}
		out = out*10 + int64(c-'0')
	}
	return out
}

func indexOf(s, sub string) int {
	if len(sub) == 0 {
		return 0
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
