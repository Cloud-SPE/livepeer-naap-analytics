package validation

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func runGrafanaRenderer(t *testing.T, env map[string]string) (string, string, error) {
	t.Helper()
	root := repoRoot(t)
	src := filepath.Join(root, "infra", "grafana", "provisioning")
	dst := t.TempDir()
	script := filepath.Join(root, "infra", "grafana", "scripts", "render-provisioning.sh")

	cmd := exec.Command("/bin/sh", script)
	cmd.Env = append(os.Environ(),
		"GRAFANA_PROVISIONING_SRC_DIR="+src,
		"GRAFANA_RENDERED_PROVISIONING_DIR="+dst,
		"GRAFANA_SKIP_SERVER_START=true",
	)
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	return dst, string(output), err
}

func TestGrafanaRendererFailsWhenEnabledWithoutChannels(t *testing.T) {
	_, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":       "true",
		"GRAFANA_ALERT_DISCORD_ENABLED":  "false",
		"GRAFANA_ALERT_TELEGRAM_ENABLED": "false",
	})
	if err == nil {
		t.Fatal("expected renderer to fail when alerting is enabled without channels")
	}
	if !strings.Contains(output, "at least one enabled delivery channel") {
		t.Fatalf("unexpected renderer failure: %s", output)
	}
}

func TestGrafanaRendererFailsWhenDiscordIsEnabledWithoutWebhook(t *testing.T) {
	_, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":       "true",
		"GRAFANA_ALERT_DISCORD_ENABLED":  "true",
		"GRAFANA_ALERT_TELEGRAM_ENABLED": "false",
	})
	if err == nil {
		t.Fatal("expected renderer to fail when discord is enabled without a webhook")
	}
	if !strings.Contains(output, "GRAFANA_ALERT_DISCORD_WEBHOOK_URL") {
		t.Fatalf("unexpected renderer failure: %s", output)
	}
}

func TestGrafanaRendererOmitsAlertingWhenDisabled(t *testing.T) {
	dst, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":       "false",
		"GRAFANA_ALERT_DISCORD_ENABLED":  "false",
		"GRAFANA_ALERT_TELEGRAM_ENABLED": "false",
	})
	if err != nil {
		t.Fatalf("renderer failed: %v\n%s", err, output)
	}
	if _, statErr := os.Stat(filepath.Join(dst, "alerting")); !os.IsNotExist(statErr) {
		t.Fatalf("expected alerting directory to be omitted, got err=%v", statErr)
	}
}

func TestGrafanaRendererRendersDiscordOnlyRouting(t *testing.T) {
	dst, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":          "true",
		"GRAFANA_ALERT_DISCORD_ENABLED":     "true",
		"GRAFANA_ALERT_DISCORD_WEBHOOK_URL": "https://discord.example.test/webhook",
		"GRAFANA_ALERT_TELEGRAM_ENABLED":    "false",
	})
	if err != nil {
		t.Fatalf("renderer failed: %v\n%s", err, output)
	}
	body, err := os.ReadFile(filepath.Join(dst, "alerting", "00-contact-points.yml"))
	if err != nil {
		t.Fatalf("ReadFile contact points: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "type: discord") {
		t.Fatalf("expected discord receiver in rendered contact points: %s", text)
	}
	if strings.Contains(text, "type: telegram") {
		t.Fatalf("did not expect telegram receiver in rendered contact points: %s", text)
	}
	if _, err := os.Stat(filepath.Join(dst, "alerting", "10-prometheus-rules.yml")); err != nil {
		t.Fatalf("expected alert rules to be rendered: %v", err)
	}
}

func TestGrafanaRendererRendersTelegramFanoutWhenEnabled(t *testing.T) {
	dst, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":          "true",
		"GRAFANA_ALERT_DISCORD_ENABLED":     "true",
		"GRAFANA_ALERT_DISCORD_WEBHOOK_URL": "https://discord.example.test/webhook",
		"GRAFANA_ALERT_TELEGRAM_ENABLED":    "true",
		"GRAFANA_ALERT_TELEGRAM_BOT_TOKEN":  "telegram-token",
		"GRAFANA_ALERT_TELEGRAM_CHAT_ID":    "@telegram-test",
	})
	if err != nil {
		t.Fatalf("renderer failed: %v\n%s", err, output)
	}
	body, err := os.ReadFile(filepath.Join(dst, "alerting", "00-contact-points.yml"))
	if err != nil {
		t.Fatalf("ReadFile contact points: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "type: discord") || !strings.Contains(text, "type: telegram") {
		t.Fatalf("expected both discord and telegram receivers in rendered contact points: %s", text)
	}
}

func TestGrafanaRendererGroupsNotificationsByConcreteTargetLabels(t *testing.T) {
	dst, output, err := runGrafanaRenderer(t, map[string]string{
		"GRAFANA_ALERTING_ENABLED":          "true",
		"GRAFANA_ALERT_DISCORD_ENABLED":     "true",
		"GRAFANA_ALERT_DISCORD_WEBHOOK_URL": "https://discord.example.test/webhook",
		"GRAFANA_ALERT_TELEGRAM_ENABLED":    "true",
		"GRAFANA_ALERT_TELEGRAM_BOT_TOKEN":  "telegram-token",
		"GRAFANA_ALERT_TELEGRAM_CHAT_ID":    "@telegram-test",
	})
	if err != nil {
		t.Fatalf("renderer failed: %v\n%s", err, output)
	}
	body, err := os.ReadFile(filepath.Join(dst, "alerting", "01-notification-policies.yml"))
	if err != nil {
		t.Fatalf("ReadFile notification policies: %v", err)
	}
	text := string(body)
	for _, required := range []string{"- job", "- instance", "- topic", "- consumergroup"} {
		if !strings.Contains(text, required) {
			t.Fatalf("expected rendered notification policy to group by %q: %s", required, text)
		}
	}
}
