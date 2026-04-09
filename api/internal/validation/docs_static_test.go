package validation

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var markdownLinkPattern = regexp.MustCompile(`\[[^\]]+\]\(([^)]+)\)`)
var lowercaseKebabMarkdownPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*\.md$`)
var specRoutePattern = regexp.MustCompile("GET\\s+(/[^`\\s|)]+)")
var openAPIPathPattern = regexp.MustCompile(`(?m)^  (/[^:]+):$`)

func TestDocsUseResolvableRelativeLinks(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "docs")); err != nil {
		t.Skip("docs tree not mounted in this test environment")
	}
	paths := []string{
		filepath.Join(root, "README.md"),
		filepath.Join(root, "AGENTS.md"),
		filepath.Join(root, "warehouse", "README.md"),
		filepath.Join(root, "infra", "clickhouse", "README.md"),
	}

	docRoots := []string{
		filepath.Join(root, "docs"),
	}
	for _, docRoot := range docRoots {
		_ = filepath.WalkDir(docRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				t.Fatalf("walk %s: %v", path, err)
			}
			if d.IsDir() {
				if filepath.Base(path) == "review-pending" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".md" {
				return nil
			}
			paths = append(paths, path)
			return nil
		})
	}

	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		matches := markdownLinkPattern.FindAllStringSubmatch(string(body), -1)
		for _, match := range matches {
			target := strings.TrimSpace(match[1])
			target = strings.Trim(target, "<>")
			if target == "" || strings.HasPrefix(target, "#") {
				continue
			}
			if strings.HasPrefix(target, "/") {
				t.Fatalf("%s uses an absolute markdown link target %q; use a relative path", path, target)
			}
			if strings.HasPrefix(target, "file://") || strings.HasPrefix(target, "vscode://") {
				t.Fatalf("%s uses editor/local-machine markdown link target %q; use a repo-relative path", path, target)
			}
			if strings.Contains(target, "://") {
				if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
					continue
				}
				t.Fatalf("%s uses unsupported markdown link target %q", path, target)
			}
			if strings.HasPrefix(target, "mailto:") {
				continue
			}
			target = strings.SplitN(target, "#", 2)[0]
			if strings.Contains(filepath.ToSlash(target), "review-pending/") {
				t.Fatalf("%s links to review-pending doc %q; review-pending files must stay unlinked from the active docs set", path, match[1])
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(path), filepath.FromSlash(target)))
			if _, err := os.Stat(resolved); err != nil {
				t.Fatalf("%s has broken markdown link %q -> %s: %v", path, match[1], resolved, err)
			}
		}
	}
}

func TestActiveDocsUseLowercaseKebabCaseFilenames(t *testing.T) {
	root := repoRoot(t)
	activeDocsRoot := filepath.Join(root, "docs")
	if _, err := os.Stat(activeDocsRoot); err != nil {
		t.Skip("docs tree not mounted in this test environment")
	}

	_ = filepath.WalkDir(activeDocsRoot, func(current string, d os.DirEntry, err error) error {
		if err != nil {
			t.Fatalf("walk %s: %v", current, err)
		}
		if d.IsDir() {
			if filepath.Base(current) == "review-pending" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(current) != ".md" {
			return nil
		}
		if !lowercaseKebabMarkdownPattern.MatchString(filepath.Base(current)) {
			t.Fatalf("%s is not lowercase kebab-case", current)
		}
		return nil
	})
}

func TestProductSpecRoutesExistInOpenAPI(t *testing.T) {
	root := repoRoot(t)
	openAPIPath := filepath.Join(root, "api", "internal", "runtime", "static", "openapi.yaml")
	body, err := os.ReadFile(openAPIPath)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", openAPIPath, err)
	}

	openAPIPaths := map[string]struct{}{}
	for _, match := range openAPIPathPattern.FindAllStringSubmatch(string(body), -1) {
		openAPIPaths[match[1]] = struct{}{}
	}

	specFiles := []string{
		filepath.Join(root, "docs", "product-specs", "index.md"),
		filepath.Join(root, "docs", "product-specs", "api-overview.md"),
		filepath.Join(root, "docs", "product-specs", "r1-network-state.md"),
		filepath.Join(root, "docs", "product-specs", "r3-performance-quality.md"),
	}

	for _, file := range specFiles {
		specBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", file, err)
		}
		for _, match := range specRoutePattern.FindAllStringSubmatch(string(specBody), -1) {
			route := strings.TrimSpace(match[1])
			if _, ok := openAPIPaths[route]; !ok {
				t.Fatalf("%s references route %q that is absent from %s", file, route, openAPIPath)
			}
		}
	}
}

func TestNoTrackedBaselineReportsRemainInDocsTree(t *testing.T) {
	root := repoRoot(t)
	baselinePattern := filepath.Join(root, "docs", "baselines", "*")
	matches, err := filepath.Glob(baselinePattern)
	if err != nil {
		t.Fatalf("Glob %s: %v", baselinePattern, err)
	}
	if len(matches) != 0 {
		var rel []string
		for _, match := range matches {
			rel = append(rel, path.Clean(strings.TrimPrefix(strings.ReplaceAll(match, root, ""), string(filepath.Separator))))
		}
		t.Fatalf("docs/baselines should be empty; found tracked baseline reports: %v", rel)
	}
}
