# Incident Response Guide

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-02 |
| **Ticket** | TASK 7.6 / [#273](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/273) |
| **Last reviewed** | 2026-04-09 |

Related: [`operations-runbook.md`](operations-runbook.md) — troubleshooting procedures · [`devops-environment-guide.md`](devops-environment-guide.md) — health check queries

---

## 1. Severity Definitions

| Severity | Definition | Examples |
|---|---|---|
| **P0 — Critical** | Full service outage. No data flowing. API down for all orgs. Immediate user/partner impact. | Kafka broker down; ClickHouse volume lost; infra1 host unreachable |
| **P1 — High** | Significant degradation. One org or major feature broken. Data lag > 15 minutes. | ClickHouse ingest stalled; gateway connectivity lost; MirrorMaker2 down > 30 min |
| **P2 — Medium** | Partial degradation. Non-critical feature affected. Data delayed < 15 minutes. | Resolver dirty partitions stuck; DLQ spike; Grafana unavailable; single dashboard broken |
| **P3 — Low** | Minor issue. No user impact. Workaround available. | Non-critical alert misfiring; Kafka UI unavailable; dashboard visual glitch |

When in doubt, declare a higher severity and downgrade once you have more information.

---

## 2. Response Times

| Severity | Acknowledge | Initial response | Resolution target |
|---|---|---|---|
| **P0** | 5 minutes | 15 minutes | 2 hours |
| **P1** | 15 minutes | 30 minutes | 4 hours |
| **P2** | 1 hour | 4 hours | Next business day |
| **P3** | Next business day | — | Best effort |

Response times apply from the moment the incident is detected or reported, whichever is earlier.

---

## 3. On-Call Procedures

### Declaring an incident

1. Post in **`#naap-incidents`** (or equivalent incident channel — _update with actual channel_): `🚨 Incident declared: [brief description] — Severity: P[N]`
2. Assign an **incident commander** — the person leading coordination. For P0/P1 this is the on-call engineer.
3. Open an incident thread or document to track timeline, actions taken, and status updates.

### Communication cadence

| Severity | Update frequency | Audience |
|---|---|---|
| P0 | Every 15 minutes until resolved | All stakeholders |
| P1 | Every 30 minutes until resolved | Team + affected orgs |
| P2 | On status change | Team |
| P3 | On resolution | Team |

### Resolving an incident

1. Post resolution in the incident channel: `✅ Resolved: [brief description of fix] — Duration: Xh Ym`
2. Mark the incident as resolved in the tracking system.
3. Schedule a post-incident review within 24 hours for P0/P1 (see §6).

---

## 4. Escalation Contacts

### Cloud SPE team

| Role | Name | Contact | Available |
|---|---|---|---|
| Primary on-call | _\<name — fill in\>_ | Slack: `@` · Email: · Phone: | 24/7 |
| Secondary on-call | _\<name — fill in\>_ | Slack: `@` · Email: · Phone: | 24/7 |
| Engineering lead | _\<name — fill in\>_ | Slack: `@` · Email: | Business hours |

### Livepeer Inc

| Role | Name | Contact | When to escalate |
|---|---|---|---|
| Infrastructure | _\<name — fill in\>_ | Slack: `@` · Email: | P0 affecting Livepeer network data |
| Gateway team | _\<name — fill in\>_ | Slack: `@` · Email: | Gateway connectivity issues, new event types |

### Third-party services

| Service | When to contact | How |
|---|---|---|
| **Cloudflare** | TLS certificate renewal failures; DNS issues affecting `kafka.cloudspe.com` | Support ticket via Cloudflare dashboard |
| **Confluent Cloud** | MirrorMaker2 source connectivity; Confluent cluster issues | Support ticket via Confluent portal |
| **Hosting provider** (infra1/infra2 host) | Host-level hardware failures, network outages | Support ticket or emergency line |

---

## 5. P0 First-Response Checklist

Run these checks immediately when a P0 is declared. The goal is to identify the broken layer within 5 minutes.

```bash
# Is the API responding?
curl -s -o /dev/null -w "%{http_code}" https://naap-api.livepeer.cloud/healthz
# Expected: 200

# Is Kafka healthy? (SSH to infra2)
docker logs naap-kafka 2>&1 | tail -5

# Is ClickHouse healthy? (SSH to infra1)
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SELECT 1"

# Are ClickHouse consumers active?
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SELECT table, status FROM system.kafka_consumers"

# Is data flowing?
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SELECT count() FROM naap.accepted_raw_events WHERE ingested_at > now() - INTERVAL 5 MINUTE"
# If 0: ingest is stalled

# Is Traefik routing? (SSH to infra2)
docker logs naap-traefik 2>&1 | grep -i error | tail -10
```

**Next step:** Based on which check fails, go to the relevant section in [`operations-runbook.md`](operations-runbook.md) §4 Troubleshooting.

---

## 6. Post-Incident Review

For all P0 and P1 incidents, conduct a blameless post-mortem within 24 hours of resolution.

### Post-mortem template

```
## Incident Post-Mortem

**Date:** YYYY-MM-DD
**Severity:** P[N]
**Duration:** Xh Ym (detected: HH:MM → resolved: HH:MM)
**Incident commander:** [name]

### Summary
[1–2 sentence description of what happened and the user impact]

### Timeline
| Time (UTC) | Event |
|---|---|
| HH:MM | Incident detected (alert / user report / manual observation) |
| HH:MM | On-call acknowledged |
| HH:MM | Root cause identified |
| HH:MM | Mitigation applied |
| HH:MM | Service restored |
| HH:MM | Incident resolved |

### Root Cause
[What broke and why]

### Contributing Factors
[What made this harder to detect, prevent, or resolve]

### Impact
- Orgs affected:
- Data gap (if any): from HH:MM to HH:MM UTC
- API downtime (if any): X minutes

### Action Items
| Action | Owner | Due date |
|---|---|---|
| [Preventive measure] | [name] | YYYY-MM-DD |
| [Detection improvement] | [name] | YYYY-MM-DD |

### Lessons Learned
[What went well, what could be improved]
```

Store post-mortems in `docs/incidents/YYYY-MM-DD-<short-title>.md` (_create this directory_).
