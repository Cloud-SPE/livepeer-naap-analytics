#!/bin/sh
set -eu

fail() {
  echo "grafana provisioning: $*" >&2
  exit 1
}

normalize_bool() {
  value=$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')
  case "$value" in
    1|true|yes|on)
      printf 'true\n'
      ;;
    0|false|no|off|'')
      printf 'false\n'
      ;;
    *)
      printf 'invalid\n'
      ;;
  esac
}

read_bool_env() {
  var_name=$1
  eval "raw_value=\${$var_name:-}"
  normalized=$(normalize_bool "$raw_value")
  if [ "$normalized" = "invalid" ]; then
    fail "$var_name must be one of true/false/1/0/yes/no/on/off"
  fi
  printf '%s\n' "$normalized"
}

require_secret() {
  var_name=$1
  eval "value=\${$var_name:-}"
  case "$value" in
    ''|not-configured|replace-me|@not_configured|https://example.invalid/discord-webhook)
      fail "$var_name must be set when its channel is enabled"
      ;;
  esac
}

copy_dir_if_present() {
  src_dir=$1
  dst_dir=$2
  if [ -d "$src_dir" ]; then
    mkdir -p "$(dirname "$dst_dir")"
    cp -R "$src_dir" "$dst_dir"
  fi
}

render_contact_points() {
  default_receiver_type=$1
  critical_receivers=$2
  out_file=$3

  {
    printf 'apiVersion: 1\n\n'
    printf 'contactPoints:\n'
    printf '  - orgId: 1\n'
    printf '    name: naap-default-warning\n'
    printf '    receivers:\n'
    if [ "$default_receiver_type" = "discord" ]; then
      printf '      - uid: naap_discord_warning\n'
      printf '        type: discord\n'
      printf '        disableResolveMessage: true\n'
      printf '        settings:\n'
      printf '          url: "%s"\n' "$GRAFANA_ALERT_DISCORD_WEBHOOK_URL"
      printf '          use_discord_username: false\n'
      printf '          message: |\n'
      printf '            {{ template "naap.default.message" . }}\n'
    else
      printf '      - uid: naap_telegram_warning\n'
      printf '        type: telegram\n'
      printf '        disableResolveMessage: true\n'
      printf '        settings:\n'
      printf '          bottoken: "%s"\n' "$GRAFANA_ALERT_TELEGRAM_BOT_TOKEN"
      printf '          chatid: "%s"\n' "$GRAFANA_ALERT_TELEGRAM_CHAT_ID"
      printf '          message: |\n'
      printf '            {{ template "naap.default.message" . }}\n'
    fi

    printf '  - orgId: 1\n'
    printf '    name: naap-critical-fanout\n'
    printf '    receivers:\n'
    if [ "$critical_receivers" = "discord" ] || [ "$critical_receivers" = "discord+telegram" ]; then
      printf '      - uid: naap_discord_critical\n'
      printf '        type: discord\n'
      printf '        disableResolveMessage: true\n'
      printf '        settings:\n'
      printf '          url: "%s"\n' "$GRAFANA_ALERT_DISCORD_WEBHOOK_URL"
      printf '          use_discord_username: false\n'
      printf '          message: |\n'
      printf '            {{ template "naap.default.message" . }}\n'
    fi
    if [ "$critical_receivers" = "telegram" ] || [ "$critical_receivers" = "discord+telegram" ]; then
      printf '      - uid: naap_telegram_critical\n'
      printf '        type: telegram\n'
      printf '        disableResolveMessage: true\n'
      printf '        settings:\n'
      printf '          bottoken: "%s"\n' "$GRAFANA_ALERT_TELEGRAM_BOT_TOKEN"
      printf '          chatid: "%s"\n' "$GRAFANA_ALERT_TELEGRAM_CHAT_ID"
      printf '          message: |\n'
      printf '            {{ template "naap.default.message" . }}\n'
    fi
  } >"$out_file"
}

render_notification_policies() {
  out_file=$1
  {
    printf 'apiVersion: 1\n\n'
    printf 'policies:\n'
    printf '  - orgId: 1\n'
    printf '    receiver: naap-default-warning\n'
    printf '    group_by:\n'
    printf '      - alertname\n'
    printf '      - severity\n'
    printf '      - component\n'
    printf '      - surface\n'
    printf '      - pipeline_type\n'
    printf '      - job_type\n'
    printf '      - pipeline_id\n'
    printf '      - gateway\n'
    printf '      - orchestrator_uri\n'
    printf '      - job\n'
    printf '      - instance\n'
    printf '      - topic\n'
    printf '      - consumergroup\n'
    printf '      - datasource\n'
    printf '    group_wait: 30s\n'
    printf '    group_interval: 5m\n'
    printf '    repeat_interval: 4h\n'
    printf '    routes:\n'
    printf "      - receiver: naap-critical-fanout\n"
    printf "        object_matchers:\n"
    printf "          - ['severity', '=~', 'critical|page-now']\n"
    printf '        group_wait: 15s\n'
    printf '        group_interval: 2m\n'
    printf '        repeat_interval: 30m\n'
  } >"$out_file"
}

SRC_DIR=${GRAFANA_PROVISIONING_SRC_DIR:-/etc/grafana/provisioning-src}
DST_DIR=${GRAFANA_RENDERED_PROVISIONING_DIR:-/tmp/grafana-provisioning}

ALERTING_ENABLED=$(read_bool_env GRAFANA_ALERTING_ENABLED)
DISCORD_ENABLED=$(read_bool_env GRAFANA_ALERT_DISCORD_ENABLED)
TELEGRAM_ENABLED=$(read_bool_env GRAFANA_ALERT_TELEGRAM_ENABLED)

rm -rf "$DST_DIR"
mkdir -p "$DST_DIR"

copy_dir_if_present "$SRC_DIR/datasources" "$DST_DIR/datasources"
copy_dir_if_present "$SRC_DIR/dashboards" "$DST_DIR/dashboards"
copy_dir_if_present "$SRC_DIR/plugins" "$DST_DIR/plugins"

if [ "$ALERTING_ENABLED" = "true" ]; then
  if [ "$DISCORD_ENABLED" = "false" ] && [ "$TELEGRAM_ENABLED" = "false" ]; then
    fail "GRAFANA_ALERTING_ENABLED=true requires at least one enabled delivery channel"
  fi

  if [ "$DISCORD_ENABLED" = "true" ]; then
    require_secret GRAFANA_ALERT_DISCORD_WEBHOOK_URL
  fi
  if [ "$TELEGRAM_ENABLED" = "true" ]; then
    require_secret GRAFANA_ALERT_TELEGRAM_BOT_TOKEN
    require_secret GRAFANA_ALERT_TELEGRAM_CHAT_ID
  fi

  mkdir -p "$DST_DIR/alerting"
  cp "$SRC_DIR/alerting/02-templates.yml" "$DST_DIR/alerting/02-templates.yml"
  cp "$SRC_DIR/alerting/10-prometheus-rules.yml" "$DST_DIR/alerting/10-prometheus-rules.yml"
  cp "$SRC_DIR/alerting/11-clickhouse-rules.yml" "$DST_DIR/alerting/11-clickhouse-rules.yml"

  if [ "$DISCORD_ENABLED" = "true" ]; then
    default_receiver_type=discord
  else
    default_receiver_type=telegram
  fi

  if [ "$DISCORD_ENABLED" = "true" ] && [ "$TELEGRAM_ENABLED" = "true" ]; then
    critical_receivers=discord+telegram
  elif [ "$DISCORD_ENABLED" = "true" ]; then
    critical_receivers=discord
  else
    critical_receivers=telegram
  fi

  render_contact_points "$default_receiver_type" "$critical_receivers" "$DST_DIR/alerting/00-contact-points.yml"
  render_notification_policies "$DST_DIR/alerting/01-notification-policies.yml"
fi

if [ "${GRAFANA_SKIP_SERVER_START:-false}" = "true" ]; then
  exit 0
fi

exec /run.sh
