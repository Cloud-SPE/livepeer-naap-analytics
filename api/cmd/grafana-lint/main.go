// Command grafana-lint enforces the API-table serving contract on
// Grafana dashboards: every panel SQL must reference only api_*
// relations, and every panel should declare its backing_table.
//
// See docs/design-docs/api-table-contract.md §Enforcement rule 2.
//
// A known-violation allowlist lives at scripts/grafana-lint-allowlist.txt
// so the lint can gate new drift without demanding every legacy panel be
// fixed in the same PR.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Tables that cannot appear in a dashboard panel SQL. Matches the
// "deliberate non-uses" list in docs/design-docs/api-table-contract.md.
// Ordering matters for reporting only — the first pattern that matches
// a line is reported.
var forbiddenTablePatterns = []*regexp.Regexp{
	regexp.MustCompile(`\bnaap\.raw_events\b`),
	regexp.MustCompile(`\bnaap\.accepted_raw_events\b`),
	regexp.MustCompile(`\bnaap\.stg_[A-Za-z0-9_]+\b`),
	regexp.MustCompile(`\bnaap\.normalized_[A-Za-z0-9_]+\b`),
	regexp.MustCompile(`\bnaap\.agg_[A-Za-z0-9_]+\b`),
	regexp.MustCompile(`\bnaap\.canonical_[A-Za-z0-9_]+_store\b`),
	// canonical_* views (non-_store) are also forbidden at the panel
	// boundary — they are not a serving contract. A narrower match
	// would also work; this one is intentionally broad.
	regexp.MustCompile(`\bnaap\.canonical_[A-Za-z0-9_]+\b`),
}

type violation struct {
	Dashboard string
	Panel     string
	Line      int
	Pattern   string
	SQL       string
}

type allowlistKey struct {
	Dashboard string
	Panel     string
}

func main() {
	var (
		dashboardsDir = flag.String("dashboards", "infra/grafana/dashboards",
			"directory containing naap-*.json dashboards (non-recursive; `infra/` subdir is ignored)")
		allowlistPath = flag.String("allowlist", "scripts/grafana-lint-allowlist.txt",
			"path to the known-violation allowlist")
		quiet = flag.Bool("quiet", false, "suppress the allowlisted echo")
	)
	flag.Parse()

	allow, err := loadAllowlist(*allowlistPath)
	if err != nil {
		fatal("load allowlist %s: %v", *allowlistPath, err)
	}

	dashboards, err := listDashboards(*dashboardsDir)
	if err != nil {
		fatal("list dashboards: %v", err)
	}

	var (
		violations         []violation
		allowlistedCount   int
		missingBackingMeta []string
	)
	for _, path := range dashboards {
		dashViolations, panelsWithoutBacking, err := lintDashboard(path)
		if err != nil {
			fatal("%s: %v", path, err)
		}
		missingBackingMeta = append(missingBackingMeta, panelsWithoutBacking...)
		for _, v := range dashViolations {
			key := allowlistKey{Dashboard: v.Dashboard, Panel: v.Panel}
			if phase, ok := allow[key]; ok {
				if !*quiet {
					fmt.Printf("ALLOWLISTED (%s) %s / panel %q line %d [%s]: %s\n",
						phase, v.Dashboard, v.Panel, v.Line, v.Pattern, shortSQL(v.SQL))
				}
				allowlistedCount++
				continue
			}
			fmt.Printf("FORBIDDEN %s / panel %q line %d [%s]: %s\n",
				v.Dashboard, v.Panel, v.Line, v.Pattern, shortSQL(v.SQL))
			violations = append(violations, v)
		}
	}

	fmt.Printf("\ngrafana-lint: %d allowlisted, %d unexpected, %d panels missing backing_table meta\n",
		allowlistedCount, len(violations), len(missingBackingMeta))

	if len(missingBackingMeta) > 0 && !*quiet {
		fmt.Println("panels missing meta.backing_table (warn):")
		for _, p := range missingBackingMeta[:min(len(missingBackingMeta), 10)] {
			fmt.Printf("  - %s\n", p)
		}
		if len(missingBackingMeta) > 10 {
			fmt.Printf("  ... %d more\n", len(missingBackingMeta)-10)
		}
	}

	if len(violations) > 0 {
		fmt.Println("\nDashboards must query only naap.api_* relations. See")
		fmt.Println("docs/design-docs/api-table-contract.md §Enforcement rule 2.")
		fmt.Println("If the violation is expected (tracked in the plan), add it to")
		fmt.Println("scripts/grafana-lint-allowlist.txt with a phase reference.")
		os.Exit(1)
	}

	fmt.Println("grafana-lint: clean (unexpected=0)")
}

func listDashboards(root string) ([]string, error) {
	var out []string
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "naap-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		out = append(out, filepath.Join(root, name))
	}
	sort.Strings(out)
	return out, nil
}

func lintDashboard(path string) ([]violation, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, nil, fmt.Errorf("parse: %w", err)
	}
	var (
		violations []violation
		missing    []string
	)
	walkPanels(doc, func(panelTitle string, panel map[string]any) {
		// Any panel with targets is a renderable panel — rows do not
		// have targets, so they self-filter out.
		targets := asArray(panel["targets"])
		if len(targets) == 0 {
			return
		}
		hasBacking := hasBackingTableMeta(panel)
		if !hasBacking {
			missing = append(missing, fmt.Sprintf("%s / panel %q",
				filepath.Base(path), panelTitle))
		}
		for _, t := range targets {
			tgt, ok := t.(map[string]any)
			if !ok {
				continue
			}
			sql := firstNonEmptyString(tgt, "rawSql", "query", "expr")
			if sql == "" {
				continue
			}
			for lineNum, line := range strings.Split(sql, "\n") {
				for _, p := range forbiddenTablePatterns {
					if loc := p.FindStringIndex(line); loc != nil {
						violations = append(violations, violation{
							Dashboard: filepath.Base(path),
							Panel:     panelTitle,
							Line:      lineNum + 1,
							Pattern:   p.String(),
							SQL:       strings.TrimSpace(line),
						})
						// Only report the first forbidden pattern per
						// line to keep the output readable.
						break
					}
				}
			}
		}
	})
	return violations, missing, nil
}

// walkPanels traverses panels and nested-row panels. fn is invoked on
// every panel (including rows, which the caller filters via targets).
func walkPanels(root map[string]any, fn func(title string, panel map[string]any)) {
	panels := asArray(root["panels"])
	for _, p := range panels {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		title, _ := pm["title"].(string)
		fn(title, pm)
		// Grafana rows hold nested panels.
		if nested, ok := pm["panels"].([]any); ok {
			for _, np := range nested {
				npm, ok := np.(map[string]any)
				if !ok {
					continue
				}
				ntitle, _ := npm["title"].(string)
				fn(ntitle, npm)
			}
		}
	}
}

// hasBackingTableMeta returns true when the panel has a non-empty
// meta.backing_table annotation. The contract says every panel should
// declare this; missing annotations are a warn, not a fail.
func hasBackingTableMeta(panel map[string]any) bool {
	meta, ok := panel["meta"].(map[string]any)
	if !ok {
		return false
	}
	bt, ok := meta["backing_table"].(string)
	return ok && strings.TrimSpace(bt) != ""
}

func asArray(v any) []any {
	if v == nil {
		return nil
	}
	a, _ := v.([]any)
	return a
}

func firstNonEmptyString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func shortSQL(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 100 {
		s = s[:100] + "..."
	}
	return s
}

func loadAllowlist(path string) (map[allowlistKey]string, error) {
	out := map[allowlistKey]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors := (*fs.PathError)(nil); errIsNotExist(err, errors) {
			return out, nil
		}
		return nil, err
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Format: <dashboard.json> / <panel title>\t<phase>
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		loc := strings.TrimSpace(parts[0])
		phase := strings.TrimSpace(parts[1])
		slash := strings.Index(loc, " / ")
		if slash < 0 {
			continue
		}
		key := allowlistKey{
			Dashboard: strings.TrimSpace(loc[:slash]),
			Panel:     strings.TrimSpace(loc[slash+len(" / "):]),
		}
		out[key] = phase
	}
	return out, nil
}

func errIsNotExist(err error, _ *fs.PathError) bool {
	return err != nil && (os.IsNotExist(err) || strings.Contains(err.Error(), "no such file"))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "grafana-lint: "+format+"\n", args...)
	os.Exit(2)
}
