package clickhouse_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoNormalizedTablesInRepo ensures that the AI batch and BYOC repo files do
// not query normalized_* tables directly. The serving contract (ADR-003) requires
// all API queries to go through api_* views only.
//
// Scope: ai_batch.go, byoc_repo.go, dashboard_jobs.go.
// Pre-existing violations in capacity.go and network_ext.go are tracked as
// technical debt (see docs/design-docs/adr-003-serving-contract.md).
func TestNoNormalizedTablesInRepo(t *testing.T) {
	// Files introduced for R17/R18 that must comply with ADR-003.
	covered := []string{
		"ai_batch.go",
		"byoc_repo.go",
		"dashboard_jobs.go",
	}

	for _, name := range covered {
		path := filepath.Join(".", name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		content := string(data)
		if strings.Contains(content, "normalized_") {
			t.Errorf("%s references normalized_* table — repo layer must only query api_* views (ADR-003)", name)
		}
	}
}
