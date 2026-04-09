package validation

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var markdownLinkPattern = regexp.MustCompile(`\[[^\]]+\]\(([^)]+)\)`)

func TestDocsUseResolvableRelativeLinks(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "docs")); err != nil {
		t.Skip("docs tree not mounted in this test environment")
	}
	ignoredScratchDocs := map[string]struct{}{
		"ISSUES_BACKLOG.md":            {},
		"KAFKA_QUEUE_EVENT_CATALOG.md": {},
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
			if d.IsDir() || filepath.Ext(path) != ".md" {
				return nil
			}
			if _, ignored := ignoredScratchDocs[filepath.Base(path)]; ignored {
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
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			target = strings.SplitN(target, "#", 2)[0]
			resolved := filepath.Clean(filepath.Join(filepath.Dir(path), filepath.FromSlash(target)))
			if _, err := os.Stat(resolved); err != nil {
				t.Fatalf("%s has broken markdown link %q -> %s: %v", path, match[1], resolved, err)
			}
		}
	}
}
