package validation

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

type grafanaAlertingFile struct {
	ContactPoints []any               `yaml:"contactPoints"`
	Policies      []any               `yaml:"policies"`
	Templates     []any               `yaml:"templates"`
	Groups        []grafanaAlertGroup `yaml:"groups"`
}

type grafanaAlertGroup struct {
	Name  string             `yaml:"name"`
	Rules []grafanaAlertRule `yaml:"rules"`
}

type grafanaAlertRule struct {
	UID          string            `yaml:"uid"`
	Title        string            `yaml:"title"`
	DashboardUID string            `yaml:"dashboardUid"`
	PanelID      int               `yaml:"panelId"`
	Annotations  map[string]string `yaml:"annotations"`
	Labels       map[string]string `yaml:"labels"`
	Data         []grafanaRuleData `yaml:"data"`
}

type grafanaRuleData struct {
	RefID         string           `yaml:"refId"`
	DatasourceUID string           `yaml:"datasourceUid"`
	Model         grafanaRuleModel `yaml:"model"`
}

type grafanaRuleModel struct {
	RawSQL     string `yaml:"rawSql"`
	Expr       string `yaml:"expr"`
	Expression string `yaml:"expression"`
	Type       string `yaml:"type"`
}

type grafanaDashboardFile struct {
	UID    string                  `json:"uid"`
	Panels []grafanaDashboardPanel `json:"panels"`
}

type grafanaDashboardTarget struct {
	RawSQL string `json:"rawSql"`
}

type grafanaDashboardPanel struct {
	ID      int                      `json:"id"`
	Title   string                   `json:"title"`
	Type    string                   `json:"type"`
	Targets []grafanaDashboardTarget `json:"targets"`
	Panels  []grafanaDashboardPanel  `json:"panels"`
}

func alertingFilePaths(t *testing.T) []string {
	t.Helper()
	root := repoRoot(t)
	paths, err := filepath.Glob(filepath.Join(root, "infra", "grafana", "provisioning", "alerting", "*.yml"))
	if err != nil {
		t.Fatalf("Glob alerting files: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("expected Grafana alerting provisioning files")
	}
	return paths
}

func loadAlertingFiles(t *testing.T) []grafanaAlertingFile {
	t.Helper()
	var files []grafanaAlertingFile
	for _, path := range alertingFilePaths(t) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		var parsed grafanaAlertingFile
		if err := yaml.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("yaml.Unmarshal %s: %v", path, err)
		}
		files = append(files, parsed)
	}
	return files
}

func loadAlertRules(t *testing.T) []grafanaAlertRule {
	t.Helper()
	var rules []grafanaAlertRule
	for _, file := range loadAlertingFiles(t) {
		for _, group := range file.Groups {
			rules = append(rules, group.Rules...)
		}
	}
	if len(rules) == 0 {
		t.Fatal("expected Grafana alert rules")
	}
	return rules
}

func loadAlertRuleByUID(t *testing.T, uid string) grafanaAlertRule {
	t.Helper()
	for _, rule := range loadAlertRules(t) {
		if rule.UID == uid {
			return rule
		}
	}
	t.Fatalf("alert rule %q not found", uid)
	return grafanaAlertRule{}
}

func dashboardPanelIDsByUID(t *testing.T) map[string]map[int]struct{} {
	t.Helper()
	root := repoRoot(t)
	var paths []string
	err := filepath.WalkDir(filepath.Join(root, "infra", "grafana", "dashboards"), func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir dashboards: %v", err)
	}
	panelIDs := make(map[string]map[int]struct{})
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		var dashboard grafanaDashboardFile
		if err := json.Unmarshal(body, &dashboard); err != nil {
			t.Fatalf("json.Unmarshal %s: %v", path, err)
		}
		if dashboard.UID == "" {
			continue
		}
		set := make(map[int]struct{})
		collectDashboardPanelIDs(dashboard.Panels, set)
		panelIDs[dashboard.UID] = set
	}
	return panelIDs
}

func collectDashboardPanelIDs(panels []grafanaDashboardPanel, out map[int]struct{}) {
	for _, panel := range panels {
		if panel.ID != 0 {
			out[panel.ID] = struct{}{}
		}
		if len(panel.Panels) > 0 {
			collectDashboardPanelIDs(panel.Panels, out)
		}
	}
}

func walkDashboardPanels(panels []grafanaDashboardPanel, visit func(panel grafanaDashboardPanel)) {
	for _, panel := range panels {
		visit(panel)
		if len(panel.Panels) > 0 {
			walkDashboardPanels(panel.Panels, visit)
		}
	}
}

func firstRuleSQL(t *testing.T, uid string) string {
	t.Helper()
	rule := loadAlertRuleByUID(t, uid)
	for _, query := range rule.Data {
		if query.Model.RawSQL != "" {
			return query.Model.RawSQL
		}
	}
	t.Fatalf("alert rule %q has no rawSql query", uid)
	return ""
}

func TestGrafanaAlertingProvisioningParses(t *testing.T) {
	for _, path := range alertingFilePaths(t) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		var parsed any
		if err := yaml.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("yaml.Unmarshal %s: %v", path, err)
		}
	}
}

func TestGrafanaAlertRulesUseKnownDatasourceUIDs(t *testing.T) {
	validUIDs := map[string]struct{}{
		"prometheus":      {},
		"clickhouse_prod": {},
		"__expr__":        {},
	}
	for _, rule := range loadAlertRules(t) {
		for _, query := range rule.Data {
			if _, ok := validUIDs[query.DatasourceUID]; !ok {
				t.Fatalf("rule %s uses unknown datasource uid %q", rule.UID, query.DatasourceUID)
			}
		}
	}
}

func TestGrafanaAlertRulesCarryRequiredLabelsAndAnnotations(t *testing.T) {
	requiredLabels := []string{"severity", "datasource", "component", "surface", "pipeline_type"}
	requiredAnnotations := []string{"summary", "description", "runbook_url"}

	for _, rule := range loadAlertRules(t) {
		for _, label := range requiredLabels {
			if strings.TrimSpace(rule.Labels[label]) == "" {
				t.Fatalf("rule %s missing required label %q", rule.UID, label)
			}
		}
		for _, annotation := range requiredAnnotations {
			if strings.TrimSpace(rule.Annotations[annotation]) == "" {
				t.Fatalf("rule %s missing required annotation %q", rule.UID, annotation)
			}
		}
	}
}

func TestGrafanaAlertRuleUIDLengthsFitGrafanaLimit(t *testing.T) {
	for _, rule := range loadAlertRules(t) {
		if len(rule.UID) > 40 {
			t.Fatalf("rule %s exceeds Grafana 40 character uid limit", rule.UID)
		}
	}
}

func TestGrafanaAlertRulesReferenceValidDashboardsAndPanels(t *testing.T) {
	dashboards := dashboardPanelIDsByUID(t)
	for _, rule := range loadAlertRules(t) {
		if rule.DashboardUID == "" {
			continue
		}
		panels, ok := dashboards[rule.DashboardUID]
		if !ok {
			t.Fatalf("rule %s references unknown dashboard uid %q", rule.UID, rule.DashboardUID)
		}
		if _, ok := panels[rule.PanelID]; !ok {
			t.Fatalf("rule %s references unknown panel %d on dashboard %q", rule.UID, rule.PanelID, rule.DashboardUID)
		}
	}
}

func TestGrafanaAlertingProvisioningDoesNotReferenceEmailOrSMTP(t *testing.T) {
	for _, path := range alertingFilePaths(t) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		lower := strings.ToLower(string(body))
		for _, forbidden := range []string{"type: email", "smtp", "addresses:"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s contains forbidden email-related token %q", path, forbidden)
			}
		}
	}
}

func TestGrafanaAlertTemplateIncludesActionableTargetsAndGrafanaLink(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "infra", "grafana", "provisioning", "alerting", "02-templates.yml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	text := string(body)
	for _, required := range []string{
		"Target: {{ template \"naap.target.label\" (index .Alerts 0) }}",
		"Observed:",
		"Grafana: {{ . }}",
		"(index .Alerts 0).GeneratorURL",
		".ValueString",
		".Labels.instance",
		".Labels.pipeline_id",
		".Labels.consumergroup",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("expected alert template to contain %q", required)
		}
	}
}

func TestGrafanaMultiDimensionalAlertsUseMathExpressions(t *testing.T) {
	expectedMathRules := map[string]string{
		"naap_scrape_target_down":   "$A < 1",
		"naap_host_disk_high":       "$A > 85",
		"naap_kafka_lag_high":       "$A > 50000",
		"naap_rr_dup_rows":          "$A > 0",
		"naap_rr_canon_cov_low":     "$A < 98",
		"naap_rr_density_drop":      "$A < 20",
		"naap_stream_sig_cov_low":   "$A < 60",
		"naap_stream_unserved_high": "$A > 40",
		"naap_stream_density_drop":  "$A < 20",
	}

	for uid, wantExpression := range expectedMathRules {
		rule := loadAlertRuleByUID(t, uid)
		found := false
		for _, query := range rule.Data {
			if query.RefID != "B" {
				continue
			}
			found = true
			if query.Model.Type != "math" {
				t.Fatalf("rule %s refId B should use math expression, got %q", uid, query.Model.Type)
			}
			if strings.TrimSpace(query.Model.Expression) != wantExpression {
				t.Fatalf("rule %s refId B should use expression %q, got %q", uid, wantExpression, query.Model.Expression)
			}
		}
		if !found {
			t.Fatalf("rule %s missing refId B expression", uid)
		}
	}
}

func TestAffectedDashboardPanelsDeduplicateOrchestratorStateLookups(t *testing.T) {
	root := repoRoot(t)
	paths := []string{
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-overview.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-live-operations.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-performance-drilldown.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-supply-inventory.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-economics.json"),
	}

	affectedTitles := map[string]struct{}{
		"Gateway -> Orchestrator -> Pipeline Paths (15m)":        {},
		"Top 10 Orchestrators by SLA Score (Bar Chart)":          {},
		"Top Orchestrators (24h: Streams, FPS, Latency)":         {},
		"Current Active Streams":                                 {},
		"Current Known Live Streams":                             {},
		"Current Streams by Orchestrator (includes zero-active)": {},
		"Current Gateway -> Orchestrator -> Pipeline Paths":      {},
		"FPS Average by Orchestrator (1h intervals)":             {},
		"Jitter Coefficient by Orchestrator":                     {},
		"GPU Inventory (Latest Snapshots)":                       {},
		"Latest Quoted Prices by Orchestrator":                   {},
	}

	unsafeSnippets := []string{
		"WITH ens AS (SELECT orch_address AS addr, name AS ens_name FROM naap.api_current_orchestrator)",
		"WITH ens AS (SELECT orch_address AS addr, name AS ens_name FROM naap.api_current_orchestrator),",
		"LEFT JOIN naap.api_current_orchestrator o ON o.orch_address = so.orch_address",
	}

	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}

		var dashboard grafanaDashboardFile
		if err := json.Unmarshal(body, &dashboard); err != nil {
			t.Fatalf("json.Unmarshal %s: %v", path, err)
		}

		walkDashboardPanels(dashboard.Panels, func(panel grafanaDashboardPanel) {
			if _, ok := affectedTitles[panel.Title]; !ok {
				return
			}
			if panel.Title == "Current Active Streams" && panel.Type != "table" {
				return
			}
			for _, target := range panel.Targets {
				sql := strings.TrimSpace(target.RawSQL)
				if sql == "" {
					continue
				}
				for _, snippet := range unsafeSnippets {
					if strings.Contains(sql, snippet) {
						t.Fatalf("panel %q in %s still contains unsafe orchestrator-state lookup %q", panel.Title, path, snippet)
					}
				}
			}
		})
	}
}

func TestAffectedDashboardPanelsUseCollisionAwareLabelsAndStreamingSLA(t *testing.T) {
	root := repoRoot(t)
	paths := []string{
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-overview.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-live-operations.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-performance-drilldown.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-supply-inventory.json"),
		filepath.Join(root, "infra", "grafana", "dashboards", "naap-economics.json"),
	}

	labelTitles := map[string]struct{}{
		"Gateway -> Orchestrator -> Pipeline Paths (15m)":        {},
		"Top 10 Orchestrators by SLA Score (Bar Chart)":          {},
		"Top Orchestrators (24h: Streams, FPS, Latency)":         {},
		"Current Active Streams":                                 {},
		"Current Known Live Streams":                             {},
		"Current Streams by Orchestrator (includes zero-active)": {},
		"Current Gateway -> Orchestrator -> Pipeline Paths":      {},
		"FPS Average by Orchestrator (1h intervals)":             {},
		"Jitter Coefficient by Orchestrator":                     {},
		"GPU Inventory (Latest Snapshots)":                       {},
		"Latest Quoted Prices by Orchestrator":                   {},
	}

	var slaChartCount int

	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}

		var dashboard grafanaDashboardFile
		if err := json.Unmarshal(body, &dashboard); err != nil {
			t.Fatalf("json.Unmarshal %s: %v", path, err)
		}

		walkDashboardPanels(dashboard.Panels, func(panel grafanaDashboardPanel) {
			if panel.Title == "Top 10 Orchestrators by Avg FPS (Bar Chart)" {
				t.Fatalf("panel %q in %s should have been renamed to SLA Score", panel.Title, path)
			}
			if _, ok := labelTitles[panel.Title]; !ok {
				return
			}
			if panel.Title == "Current Active Streams" && panel.Type != "table" {
				return
			}
			for _, target := range panel.Targets {
				sql := strings.TrimSpace(target.RawSQL)
				if sql == "" {
					continue
				}
				for _, required := range []string{"label_meta AS (", "name_collision_count", "labels AS ("} {
					if !strings.Contains(sql, required) {
						t.Fatalf("panel %q in %s missing collision-aware label snippet %q", panel.Title, path, required)
					}
				}
				switch panel.Title {
				case "Top 10 Orchestrators by SLA Score (Bar Chart)":
					slaChartCount++
					if !strings.Contains(sql, "FROM naap.api_hourly_streaming_sla") {
						t.Fatalf("panel %q in %s should read from streaming SLA source", panel.Title, path)
					}
					if strings.Contains(sql, "api_requests_sla") {
						t.Fatalf("panel %q in %s should not reference request/response SLA source", panel.Title, path)
					}
					if !strings.Contains(sql, "argMax(sla_score") {
						t.Fatalf("panel %q in %s should rank by latest SLA score", panel.Title, path)
					}
					if !strings.Contains(sql, "eligible_orch AS (") || !strings.Contains(sql, "INNER JOIN eligible_orch") {
						t.Fatalf("panel %q in %s should filter SLA rows through eligible orchestrators", panel.Title, path)
					}
				case "Top Orchestrators (24h: Streams, FPS, Latency)":
					if !strings.Contains(sql, "FROM naap.api_hourly_streaming_sla") {
						t.Fatalf("panel %q in %s should join streaming SLA data", panel.Title, path)
					}
					if strings.Contains(sql, "api_requests_sla") {
						t.Fatalf("panel %q in %s should not reference request/response SLA source", panel.Title, path)
					}
					if !strings.Contains(sql, "\"SLA Score\"") {
						t.Fatalf("panel %q in %s should expose an SLA Score column", panel.Title, path)
					}
					if !strings.Contains(sql, "eligible_pairs AS (") || !strings.Contains(sql, "INNER JOIN eligible_pairs") {
						t.Fatalf("panel %q in %s should filter SLA rows through eligible address/pipeline pairs", panel.Title, path)
					}
				}
			}
		})
	}

	if slaChartCount != 2 {
		t.Fatalf("expected 2 SLA chart panels, found %d", slaChartCount)
	}
}

func TestNaapJobsDashboardOrgVariableUsesHourlyRequestDemand(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "infra", "grafana", "dashboards", "naap-jobs.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}

	if !strings.Contains(string(body), "SELECT DISTINCT org FROM naap.api_hourly_request_demand") {
		t.Fatalf("%s should source the org variable from naap.api_hourly_request_demand", path)
	}
	if strings.Contains(string(body), "SELECT DISTINCT org FROM (SELECT org FROM naap.api_fact_ai_batch_job") {
		t.Fatalf("%s still sources the org variable from request job facts", path)
	}
}
