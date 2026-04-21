# Infrastructure Hardening Runbook

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-02 |
| **Ticket** | TASK-17 / [#296](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/296) |
| **Last reviewed** | 2026-04-09 |

Related: [`deploy/set_ufw_rules.sh`](../../deploy/set_ufw_rules.sh) · [`deploy/ubuntu-setup.sh`](../../deploy/ubuntu-setup.sh) · [`deploy/ubuntu-file-modifications.txt`](../../deploy/ubuntu-file-modifications.txt)

---

## Overview

This runbook documents the current security posture of the NaaP analytics infrastructure, identifies the remaining open action items, and provides the exact commands to close them. It covers both infra2 (Kafka, Traefik, Kafka UI, MirrorMaker2) and infra1 (ClickHouse, API, Grafana, Prometheus).

---

## Current Security Posture

The following controls are already in place and require no further action.

### Host-level hardening (`deploy/ubuntu-setup.sh`, `deploy/ubuntu-file-modifications.txt`)

| Control | Status | Notes |
|---|---|---|
| UFW default policy | ✅ in place | `default deny incoming`, `default allow outgoing` |
| SSH port | ✅ in place | Non-standard port 9222; `sshd_config` hardened |
| Root account | ✅ in place | Locked via `passwd -l root` |
| Fail2Ban | ✅ in place | Installed and active |
| Kernel hardening | ✅ in place | `sysctl.conf` modifications applied |
| Sudo configuration | ✅ in place | `sudoers.d/nopasswd` configured; two named users (`dockerusr`, `speedybird`) |

### Kafka security (`deploy/infra2/kafka/stack.yml`)

| Control | Status | Notes |
|---|---|---|
| Authentication | ✅ in place | SASL/SCRAM-SHA-512 with 8192 iterations on all client-facing listeners |
| External TLS | ✅ in place | Traefik TCP router with Cloudflare cert on port 9092 |
| PLAINTEXT port 29092 | ✅ in place | Docker-internal only; not in `ports:` mapping; unreachable from host |
| Least-privilege users | ✅ in place | 5 SCRAM users with per-service roles |

### ClickHouse security (`deploy/infra1/clickhouse/stack.yml`)

| Control | Status | Notes |
|---|---|---|
| Network isolation | ✅ in place | No `ports:` mapping; protected by Docker overlay network |
| Authentication | ✅ in place | Three users (`naap_admin`, `naap_writer`, `naap_reader`) with SHA-256 passwords |

### Traefik

| Control | Status | Notes |
|---|---|---|
| TLS | ✅ in place | All public-facing services served via HTTPS (Cloudflare cert) |
| Dashboard | ✅ in place | `insecure: true` set but dashboard port is not exposed externally |

---

## Kafka Listener Architecture

| Listener | Protocol | Port | Advertised address | Who connects |
|---|---|---|---|---|
| `PLAINTEXT` | No auth | 29092 | `naap-kafka:29092` | Docker-internal only (SCRAM provisioning scripts) |
| `SASL_PLAINTEXT` | SCRAM auth | 9092 | `naap-kafka:9092` | Docker overlay clients: Kafka UI, kafka-exporter, MM2 |
| `SASL_PLAINTEXT_EXT` | SCRAM auth | 9094 | `kafka.cloudspe.com:9092` | Traefik TCP router target |
| `CONTROLLER` | Internal | 9093 | `naap-kafka:9093` | KRaft controller only |

### External TLS path

Clients connecting to `kafka.cloudspe.com:9092`:
1. Client negotiates TLS (SNI: `kafka.cloudspe.com`) — appears as `SASL_SSL` to the client.
2. Traefik matches the SNI rule and terminates TLS using the Cloudflare certificate.
3. Traefik forwards the decrypted stream to `naap-kafka:9094` (`SASL_PLAINTEXT_EXT`).
4. Kafka authenticates the SCRAM credentials normally.

### infra1 ClickHouse connects via the external TLS path

`deploy/infra1/clickhouse/stack.yml` sets `KAFKA_SECURITY_PROTOCOL: SASL_SSL`. This is correct by design — in production, Portainer overrides `KAFKA_BROKER_LIST` from the default `naap-kafka:9092` (local Docker) to `kafka.cloudspe.com:9092` (external TLS endpoint). The stack file default is a local-dev fallback only. infra1 ClickHouse always uses `SASL_SSL` against the Traefik-terminated path.

---

## Open Action Items

### Action 1 — Restrict Portainer Agent port 9001

**Current state:** `ufw allow 9001` — world-accessible.

**Risk:** The Portainer Agent (`deploy/start_portainer_agent.sh`) runs with full Docker socket access (`/var/run/docker.sock`) and mounts the entire host filesystem (`/:/host`). Any host that can reach port 9001 has effective root access to the machine.

**Fix** — run on each host where the Portainer Agent is deployed:

```bash
# 1. Remove the open rule
ufw delete allow 9001

# 2. Allow only the Portainer server IP
ufw allow from <PORTAINER_SERVER_IP> to any port 9001 proto tcp \
  comment 'Portainer Agent - Portainer server only'

# 3. Verify
ufw status numbered
```

Replace `<PORTAINER_SERVER_IP>` with the static IP of the Portainer management server.

**Verification:** Log into the Portainer management UI and confirm all agents are still showing as connected.

---

### Action 2 — Restrict Traefik metrics port 8443

**Current state:** `ufw allow 8443` — world-accessible.

**Risk:** Prometheus scrape endpoints exposed on port 8443 reveal internal service topology: container names, IP addresses, service names, and metric cardinality.

**Fix** — run on the host where Traefik metrics is exposed:

```bash
# 1. Remove the open rule
ufw delete allow 8443

# 2. Allow only from your ops/monitoring CIDR
ufw allow from <OPS_CIDR> to any port 8443 proto tcp \
  comment 'Traefik metrics - ops only'

# 3. Verify
ufw status numbered
```

Replace `<OPS_CIDR>` with the IP or CIDR block of your Prometheus server or ops bastion.

**Verification:** Confirm Prometheus is still successfully scraping the infra2 metrics endpoint. Check `Status → Targets` in the Prometheus UI — the `metrics-infra2.*` target should show state `UP`.

---

### Action 3 — Remove stale ClickHouse UFW rule (port 8123)

**Current state:** `ufw allow 8123` is present (applied per `history.txt`) but ClickHouse has no Docker `ports:` mapping — nothing listens on host port 8123.

**Risk:** The rule is a latent hazard. If a `ports:` mapping were accidentally added to `deploy/infra1/clickhouse/stack.yml` during a stack update, the ClickHouse HTTP interface would become publicly accessible without any further firewall change.

**Fix:**

```bash
ufw delete allow 8123
ufw status numbered
```

**Verification:** Confirm ClickHouse internal access still works via `docker exec`:

```bash
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SELECT 1"
```

---

## Post-Hardening Verification Checklist

Run after applying all three action items:

```
[ ] ufw status verbose — confirm rules on each host match expected state
[ ] Portainer UI — all agents show as connected
[ ] Prometheus Targets — metrics-infra2 target shows UP
[ ] Kafka external — connecting client can reach kafka.cloudspe.com:9092
[ ] ClickHouse — docker exec query returns 1 (port 8123 rule removal only)
[ ] API — GET /healthz returns 200
[ ] Grafana — dashboards loading, Kafka and ClickHouse metrics visible
[ ] Grafana — provisioned Discord and Telegram alert contact points load without missing-secret errors
```
