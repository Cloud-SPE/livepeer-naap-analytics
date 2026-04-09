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

func isInternalAPIBaseDependency(sql, needle string) bool {
	if !strings.Contains(sql, needle) {
		return false
	}
	switch needle {
	case "ref('api_", `ref("api_`:
		return strings.Contains(sql, "ref('api_base_") || strings.Contains(sql, `ref("api_base_`)
	case "from naap.api_":
		return strings.Contains(sql, "from naap.api_base_")
	default:
		return false
	}
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
			if isInternalAPIBaseDependency(sql, forbidden) {
				continue
			}
			if strings.Contains(sql, forbidden) {
				t.Fatalf("%s bypasses the serving contract with forbidden dependency %q", file, forbidden)
			}
		}
		for _, line := range strings.Split(sql, "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.Contains(trimmed, "from naap.api_") {
				continue
			}
			if strings.Contains(trimmed, "from naap.api_base_") {
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
		filepath.Join(root, "warehouse", "models", "api_base"),
		filepath.Join(root, "warehouse", "models", "canonical"),
		filepath.Join(root, "warehouse", "models", "facts"),
		filepath.Join(root, "warehouse", "models", "refresh"),
		filepath.Join(root, "warehouse", "models", "serving"),
		filepath.Join(root, "warehouse", "models", "staging"),
	}
	for _, modelRoot := range modelRoots {
		if _, err := os.Stat(modelRoot); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("Stat %s: %v", modelRoot, err)
		}
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
				if isInternalAPIBaseDependency(sql, forbidden) {
					continue
				}
				if strings.Contains(sql, forbidden) {
					if strings.Contains(sql, "from naap.api_") && strings.Contains(sql, "_store") {
						continue
					}
					t.Fatalf("%s illegally derives truth from api_* via %q", file, forbidden)
				}
			}
		}
	}
}

func TestTierContract_RollupServingLayersDoNotAverageDerivedAggregates(t *testing.T) {
	root := repoRoot(t)
	checks := map[string][]string{
		filepath.Join(root, "warehouse", "models", "api"): {
			"avg(s.avg_",
			"avg(b.avg_",
			"quantile(0.95)(s.avg_",
			"quantile(0.95)(b.avg_",
			"avg(s.health_signal_coverage_ratio)",
			"avg(b.health_signal_coverage_ratio)",
			"avg(s.avg_output_fps)",
			"avg(b.avg_output_fps)",
			"avg(s.avg_prompt_to_first_frame_ms)",
			"avg(b.avg_prompt_to_first_frame_ms)",
			"avg(s.avg_e2e_latency_ms)",
			"avg(b.avg_e2e_latency_ms)",
			"avg(s.latency_score)",
			"avg(b.latency_score)",
			"avg(s.fps_score)",
			"avg(b.fps_score)",
			"avg(s.quality_score)",
			"avg(b.quality_score)",
			"avg(s.sla_score)",
			"avg(b.sla_score)",
		},
		filepath.Join(root, "api", "internal", "resolver"): {
			"avg(b.avg_",
			"quantile(0.95)(b.avg_",
			"avg(b.health_signal_coverage_ratio)",
			"avg(b.latency_score)",
			"avg(b.fps_score)",
			"avg(b.quality_score)",
			"avg(b.sla_score)",
		},
	}

	for dir, forbiddenPatterns := range checks {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !(strings.HasSuffix(name, ".sql") || strings.HasSuffix(name, ".go")) {
				continue
			}
			file := filepath.Join(dir, name)
			body, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("ReadFile %s: %v", file, err)
			}
			text := strings.ToLower(string(body))
			for _, forbidden := range forbiddenPatterns {
				if strings.Contains(text, forbidden) {
					t.Fatalf("%s uses forbidden aggregate-of-aggregate pattern %q", file, forbidden)
				}
			}
		}
	}
}

func TestTierContract_RollupSafetyRuleIsDocumented(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "docs")); err != nil {
		t.Skip("docs tree not mounted in this test environment")
	}
	docChecks := map[string][]string{
		filepath.Join(root, "docs", "design.md"): {
			"rollup safety",
			"do not compute aggregates from already-aggregated values unless the aggregate is mathematically merge-safe",
		},
		filepath.Join(root, "docs", "design-docs", "data-validation-rules.md"): {
			"percentiles require merge-safe aggregate state",
			"overlapping classifications must expose an explicit additive union counter",
		},
	}

	for file, requiredSnippets := range docChecks {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		text := strings.ToLower(string(body))
		for _, snippet := range requiredSnippets {
			if !strings.Contains(text, snippet) {
				t.Fatalf("%s is missing required rollup-safety guidance %q", file, snippet)
			}
		}
	}
}

func TestTierContract_SLABenchmarkStateUsesAdditiveInputsOnly(t *testing.T) {
	root := repoRoot(t)
	checks := map[string][]string{
		filepath.Join(root, "warehouse", "models", "api_base", "api_base_sla_quality_cohort_daily_state.sql"): {
			"ref('api_base_sla_quality_inputs_by_org')",
		},
		filepath.Join(root, "warehouse", "models", "api_base", "api_base_sla_quality_benchmarks.sql"): {
			"ref('api_base_sla_quality_inputs')",
			"ref('api_base_sla_quality_cohort_daily_state')",
		},
		filepath.Join(root, "warehouse", "models", "api_base", "api_base_sla_quality_benchmarks_by_org.sql"): {
			"ref('api_base_sla_quality_inputs_by_org')",
			"ref('api_base_sla_quality_cohort_daily_state')",
		},
		filepath.Join(root, "warehouse", "models", "api_base", "api_base_sla_compliance_scored.sql"): {
			"ref('api_base_sla_quality_inputs')",
			"ref('api_base_sla_quality_benchmarks')",
		},
		filepath.Join(root, "warehouse", "models", "api_base", "api_base_sla_compliance_scored_by_org.sql"): {
			"ref('api_base_sla_quality_inputs_by_org')",
			"ref('api_base_sla_quality_benchmarks_by_org')",
		},
	}

	for file, requiredSnippets := range checks {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		text := strings.ToLower(string(body))
		for _, snippet := range requiredSnippets {
			if !strings.Contains(text, strings.ToLower(snippet)) {
				t.Fatalf("%s is missing required benchmark-state dependency %q", file, snippet)
			}
		}
		for _, forbidden := range []string{"quality_cohort_level", "quality_history_window_count", "ref('api_sla_compliance", `ref("api_sla_compliance`} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s illegally depends on previously scored SLA outputs via %q", file, forbidden)
			}
		}
	}
}
