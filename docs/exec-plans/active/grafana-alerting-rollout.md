# Plan: Grafana Alerting Rollout on the BYOC + Streaming Mainline

## Context

The current branch is authoritative for runtime behavior, resolver ownership,
BYOC request/response support, and published serving contracts. Older Grafana
alerting work from `tasks/jk/alerting-support` must be ported selectively
instead of merged wholesale so alerting lands on top of the current system
without regressing core behavior.

This rollout keeps alerting anomaly-focused:

- system and ingest health
- request/response integrity and density regressions for `ai-batch` and `byoc`
- streaming/live-video health-signal coverage, unserved demand, and density regressions

Email delivery is intentionally out of scope. Repo-managed routing uses Discord
for `warning` / `degraded` and Discord + Telegram for `critical` / `page-now`.

## Scope

1. Add repo-managed Grafana contact points, templates, notification policies,
   and alert rules under `infra/grafana/provisioning/alerting/`.
2. Wire Grafana alerting env vars into local and deployment configs without
   changing API, resolver, ClickHouse, or dbt semantics.
3. Add dashboard panels that expose the exact request/response and streaming
   alert queries so operators can move from notification to diagnosis quickly.
4. Add static tests for alert provisioning structure and validation tests that
   execute the ClickHouse alert SQL against seeded scenarios.
5. Update operations documentation so the runbooks describe provisioned alerting
   as shipped behavior instead of future work.

## Verification

- `go test ./api/internal/validation/...`
- `make test`
- `make test-validation-clean`
- Local Grafana loads the `infra` folder with no provisioning errors.
- Discord and Telegram test notifications render the contextual labels needed to
  identify the affected component or pipeline surface.
