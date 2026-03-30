package validation

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func collectSQLFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", root, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(root, entry.Name()))
	}
	return files
}

func TestTierContract_APIModelsDoNotBypassCanonicalOrServingLayers(t *testing.T) {
	root := repoRoot(t)
	for _, file := range collectSQLFiles(t, filepath.Join(root, "warehouse", "models", "api")) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		sql := strings.ToLower(string(body))
		if strings.Contains(sql, "canonical_session_latest") {
			t.Fatalf("%s still depends on removed compatibility surface canonical_session_latest", file)
		}
		for _, forbidden := range []string{"ref('raw_", `ref("raw_`, "from naap.raw_", "ref('normalized_", `ref("normalized_`, "from naap.normalized_", "ref('operational_", `ref("operational_`, "from naap.operational_", "ref('api_", `ref("api_`} {
			if strings.Contains(sql, forbidden) {
				t.Fatalf("%s bypasses the serving contract with forbidden dependency %q", file, forbidden)
			}
		}
		for _, line := range strings.Split(sql, "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.Contains(trimmed, "from naap.api_") {
				continue
			}
			if strings.Contains(trimmed, "_store") {
				continue
			}
			t.Fatalf("%s bypasses the serving contract with forbidden dependency %q", file, strings.TrimSpace(line))
		}
	}
}

func TestTierContract_ResolverRuntimeDoesNotDependOnWarehouseTarget(t *testing.T) {
	root := repoRoot(t)
	resolverRoot := filepath.Join(root, "api", "internal", "resolver")
	entries, err := os.ReadDir(resolverRoot)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", resolverRoot, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file := filepath.Join(resolverRoot, entry.Name())
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		text := strings.ToLower(string(body))
		for _, forbidden := range []string{"warehouse/target", "canonical_sql_artifact_dir"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s illegally depends on refresh-era warehouse target runtime artifacts via %q", file, forbidden)
			}
		}
	}
}

func TestTierContract_ClickHouseRepoDoesNotUseCanonicalSessionLatest(t *testing.T) {
	root := repoRoot(t)
	repoDir := filepath.Join(root, "api", "internal", "repo", "clickhouse")
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", repoDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file := filepath.Join(repoDir, entry.Name())
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		if strings.Contains(strings.ToLower(string(body)), "canonical_session_latest") {
			t.Fatalf("%s still depends on removed compatibility surface canonical_session_latest", file)
		}
	}
}

func TestTierContract_CanonicalModelsDoNotUseCanonicalSessionLatest(t *testing.T) {
	root := repoRoot(t)
	canonicalDir := filepath.Join(root, "warehouse", "models", "canonical")
	for _, file := range collectSQLFiles(t, canonicalDir) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		if strings.Contains(strings.ToLower(string(body)), "canonical_session_latest") {
			t.Fatalf("%s still depends on removed compatibility surface canonical_session_latest", file)
		}
	}
}

func TestTierContract_NonAPIModelsDoNotDeriveTruthFromAPIModels(t *testing.T) {
	root := repoRoot(t)
	modelRoots := []string{
		filepath.Join(root, "warehouse", "models", "canonical"),
		filepath.Join(root, "warehouse", "models", "facts"),
		filepath.Join(root, "warehouse", "models", "refresh"),
		filepath.Join(root, "warehouse", "models", "serving"),
		filepath.Join(root, "warehouse", "models", "staging"),
	}
	for _, modelRoot := range modelRoots {
		for _, file := range collectSQLFiles(t, modelRoot) {
			if strings.HasPrefix(filepath.Base(file), "v_api_") {
				continue
			}
			body, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("ReadFile %s: %v", file, err)
			}
			sql := strings.ToLower(string(body))
			for _, forbidden := range []string{"ref('api_", `ref("api_`, "from naap.api_"} {
				if strings.Contains(sql, forbidden) {
					if strings.Contains(sql, "from naap.api_") &&
						strings.Contains(file, filepath.Join("warehouse", "models", "serving")) &&
						strings.Contains(sql, "_store") {
						continue
					}
					t.Fatalf("%s illegally derives truth from api_* via %q", file, forbidden)
				}
			}
		}
	}
}
