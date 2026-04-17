package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Manifest describes a committed fixture. The archive itself is git-ignored;
// this manifest is how CI validates that a locally-fetched fixture matches
// the pinned snapshot.
type Manifest struct {
	Name          string   `json:"name"`
	WindowStart   string   `json:"window_start"`
	WindowEnd     string   `json:"window_end"`
	Source        string   `json:"source"`
	Database      string   `json:"database"`
	RowCount      int64    `json:"row_count"`
	NDJSONSHA256  string   `json:"ndjson_sha256"`
	ArchiveSHA256 string   `json:"archive_sha256"`
	ArchiveBytes  int64    `json:"archive_bytes"`
	ArchiveCodec  string   `json:"archive_codec"`
	SchemaColumns []string `json:"schema_columns"`
}

// LoadManifest reads and parses a fixture manifest JSON.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if m.Name == "" || m.WindowStart == "" || m.WindowEnd == "" {
		return nil, fmt.Errorf("manifest %s is missing required fields", path)
	}
	return &m, nil
}

// Report is what the harness writes to ./target/replay/ after a run.
// A second run over the same fixture must produce byte-identical rollups.
type Report struct {
	FixtureName string            `json:"fixture_name"`
	StartedAt   time.Time         `json:"started_at"`
	FinishedAt  time.Time         `json:"finished_at"`
	Layers      []LayerReport     `json:"layers"`
	Environment map[string]string `json:"environment,omitempty"`
}

// LayerReport records the rollup for every snapshotted table in one layer.
type LayerReport struct {
	Layer      Layer                  `json:"layer"`
	DurationMS int64                  `json:"duration_ms"`
	Tables     map[string]TableRollup `json:"tables"`
}

// TableRollup is the deterministic fingerprint of a single table.
type TableRollup struct {
	Rows             int64  `json:"rows"`
	ArtifactChecksum string `json:"artifact_checksum"` // 64-char hex, SipHash128 over sorted rows
}
