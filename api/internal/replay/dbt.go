package replay

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"

	"go.uber.org/zap"
)

// DBTConfig parameterises the dbt invocation the api phase issues.
type DBTConfig struct {
	// RepoRoot is the repository root whose docker-compose.yml defines
	// the `warehouse` tooling service. Defaults to the current working
	// directory if unset.
	RepoRoot string

	// Selector is the dbt --select expression. Defaults to `api api_base`
	// so the harness rebuilds every model that publishes a serving-layer
	// relation. Pass `--exclude` via Extra if needed.
	Selector string

	// Extra are additional dbt arguments appended after --select, e.g.
	// `--vars "{...}"` or `--exclude foo`. Rarely needed.
	Extra []string
}

// RunDBT invokes dbt via the docker-compose warehouse service to rebuild
// the api + api_base view definitions against the current ClickHouse
// state. The models are views, so this step does not write row-level
// data — it only refreshes the CREATE OR REPLACE VIEW statements and
// catches schema-drift errors the replay would otherwise surface as a
// confusing checksum mismatch.
//
// Streams stdout / stderr through the harness logger so the output is
// visible but does not land in the run report JSON.
func RunDBT(ctx context.Context, log *zap.Logger, dc DBTConfig) error {
	root := dc.RepoRoot
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("getwd: %w", err)
		}
		root = cwd
	}
	selector := dc.Selector
	if selector == "" {
		// `+api` rebuilds every downstream-facing api view AND every
		// canonical view they depend on. Without `+` the canonical views
		// keep whatever CREATE VIEW definition bootstrap left them with,
		// which breaks phases that denormalize new columns onto canonical
		// views (Phase 3, Phase 4). Canonical views are cheap to rebuild
		// (~1 s on the daily fixture). Phase 5 retired api_base_* so there
		// is no longer a second top-level selector to pass.
		selector = "+api"
	}

	args := []string{"compose", "--profile", "tooling", "run", "--rm", "warehouse",
		"run", "--select", selector}
	args = append(args, dc.Extra...)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if log == nil {
		log = zap.NewNop()
	}
	log.Info("replay: invoking dbt",
		zap.String("selector", selector),
		zap.String("repo_root", root),
	)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"dbt run (exit=%v): stderr=%s stdout-tail=%s",
			cmd.ProcessState.ExitCode(),
			truncateString(stderr.String(), 2048),
			truncateString(tail(stdout.String(), 512), 512),
		)
	}
	log.Info("replay: dbt complete",
		zap.String("stdout_tail", tail(stdout.String(), 256)),
	)
	return nil
}

// tail returns the last n characters of s, ASCII-safe (runes may get
// truncated mid-multibyte — fine for log output).
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
