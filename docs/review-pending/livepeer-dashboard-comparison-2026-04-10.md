# Livepeer Dashboard Comparison Findings (2026-04-10)

This note tracks the review prompted by founder feedback comparing our dashboard work
to Livepeer's existing analytics surfaces.

Context:
- `grafana.cloudspe.com` is an older iteration and is not authoritative for current behavior.
- The current implementation reviewed here is the local API and Grafana state in this repo.
- External comparison targets were:
  - Livepeer Explorer: <https://github.com/livepeer/explorer>
  - `other-project/` symlinked in this repo, pointing to the older corporate analytics stack
    (`analytics_dbt`)

## Sources reviewed

Current repo:
- [`api/internal/repo/clickhouse/dashboard.go`](../../api/internal/repo/clickhouse/dashboard.go)
- [`api/internal/runtime/static/openapi.yaml`](../../api/internal/runtime/static/openapi.yaml)
- [`docs/design-docs/data-validation-rules.md`](../design-docs/data-validation-rules.md)
- [`docs/design-docs/selection-centered-attribution.md`](../design-docs/selection-centered-attribution.md)
- [`docs/metrics-and-sla-reference.md`](../metrics-and-sla-reference.md)
- [`infra/grafana/dashboards/naap-jobs.json`](../../infra/grafana/dashboards/naap-jobs.json)
- [`infra/grafana/dashboards/naap-economics.json`](../../infra/grafana/dashboards/naap-economics.json)

Older corporate analytics stack:
- [`other-project/ARCHITECTURE.md`](../../other-project/ARCHITECTURE.md)
- [`other-project/scripts/daily/get_network_serverless_stats.py`](../../other-project/scripts/daily/get_network_serverless_stats.py)
- [`other-project/clickhouse/semantic/daydream_user_usage.sql`](../../other-project/clickhouse/semantic/daydream_user_usage.sql)
- [`other-project/clickhouse/semantic/daydream_streams.sql`](../../other-project/clickhouse/semantic/daydream_streams.sql)
- [`other-project/clickhouse/semantic/stream_trace_summary.sql`](../../other-project/clickhouse/semantic/stream_trace_summary.sql)
- [`other-project/clickhouse/semantic/network_capabilities.sql`](../../other-project/clickhouse/semantic/network_capabilities.sql)
- [`other-project/collections/dZGc8YRw9Zu2QYvGaX5mo_library/KrWZ6_fwjtZwFwA4yThS1_dashboards/b2kH7Xe86YBKpebdH_cp7_dashboard_questions/cards/YN8YlPdDR5bIU7BIj-v6G_startup_kpis.yaml`](../../other-project/collections/dZGc8YRw9Zu2QYvGaX5mo_library/KrWZ6_fwjtZwFwA4yThS1_dashboards/b2kH7Xe86YBKpebdH_cp7_dashboard_questions/cards/YN8YlPdDR5bIU7BIj-v6G_startup_kpis.yaml)
- [`other-project/collections/dZGc8YRw9Zu2QYvGaX5mo_library/KrWZ6_fwjtZwFwA4yThS1_dashboards/cards/0XfV8xU0eGGyfEADwRSn3_daydream_start_up_success___by_gateway.yaml`](../../other-project/collections/dZGc8YRw9Zu2QYvGaX5mo_library/KrWZ6_fwjtZwFwA4yThS1_dashboards/cards/0XfV8xU0eGGyfEADwRSn3_daydream_start_up_success___by_gateway.yaml)

Explorer:
- <https://github.com/livepeer/explorer/blob/c2a7d069036292b3df1b1156cb6b32819ce2fa56/pages/api/usage.tsx>
- <https://github.com/livepeer/explorer/blob/c2a7d069036292b3df1b1156cb6b32819ce2fa56/pages/index.tsx>
- <https://github.com/livepeer/explorer/blob/c2a7d069036292b3df1b1156cb6b32819ce2fa56/pages/api/score/index.tsx>
- <https://github.com/livepeer/explorer/blob/c2a7d069036292b3df1b1156cb6b32819ce2fa56/pages/api/score/%5Baddress%5D.tsx>
- <https://github.com/livepeer/explorer/blob/c2a7d069036292b3df1b1156cb6b32819ce2fa56/components/PerformanceList/index.tsx>

## Updated conclusions

### 1. The usage discrepancy finding is still valid and is now stronger

Explorer's home-page "Estimated Usage" is fee-derived protocol usage, not observed AI
job or session minutes. The Explorer code fetches `feeDerivedMinutes` from
`livepeer.com/data/usage/query/total`.

The older corporate analytics stack measures AI/daydream usage from observed session
activity using `sum(active_stream_time_ms) / 1000 / 60` in
`semantic.daydream_user_usage`, and uses that same observed-minute logic in alerting
and ad hoc reporting.

Conclusion:
- Explorer's 11M+ minutes/week and our AI usage numbers are not apples-to-apples.
- The founder's concern was correct.
- Our earlier answer on this point remains valid.

### 2. "Scope" should still not be treated as a pipeline identifier

The older corporate analytics stack keeps `client_source` separate from
`pipeline_id`, `pipeline_name`, and `model_id`. That separation appears in
`semantic.daydream_streams`, `semantic.daydream_user_usage`, and
`semantic.stream_status`.

Our current repo's design docs also distinguish source/application identity from
pipeline identity.

Conclusion:
- If the dashboard suggests that `Scope` is a pipeline, that is a presentation or
  semantic-mapping problem.
- This finding remains valid and is now supported by both older Livepeer analytics
  conventions and our own documented model.

### 3. The `Effective %` bug remains valid and appears to be ours only

Our orchestrator dashboard code computes:
- `unexcused = startup_failed_sessions - startup_excused_sessions`
- `effectiveRate = (known_sessions - unexcused) / known_sessions`

That conflicts with our own contract, where:
- `startup_failed_sessions` already means unexcused startup failures
- `effective_failed_sessions` also includes loading-only and zero-output failures
- `effective_success_rate = 1 - effective_failed_sessions / requested_sessions`

The older corporate analytics stack does not replicate this exact display math. It
tracks explicit startup status, excusal reasons, app startup success, gateway startup
success, and output-FPS style metrics directly from stream facts.

Conclusion:
- This still looks like a bug in our implementation.
- I did not find corroborating evidence that Explorer or the older corporate stack
  use the same incorrect derivation.

### 4. Our KPI and pipeline/catalog drift remains valid and looks isolated to our repo

The local API implementation mixes `api_unified_demand` jobs into dashboard KPI
totals and pipeline catalog results, while the current OpenAPI contract describes
streaming-only semantics for the top-level KPI and pipeline endpoints.

The older corporate analytics stack keeps distinct surfaces for:
- observed AI usage
- startup/session reliability
- pipeline metadata
- network pricing/capability snapshots

It does not show the same kind of contract drift where a streaming-oriented surface
silently absorbs non-streaming job totals.

Conclusion:
- The earlier finding remains valid.
- This currently appears to be our issue, not a shared Livepeer convention.

### 5. Pricing support is inconsistent across all three projects, but the bugs differ

Our repo:
- The dashboard pricing API advertises `pixelsPerUnit` in schema, but the code
  hardcodes `PixelsPerUnit: 1`.
- This is a direct implementation bug because the repo already parses real
  `PixelsPerUnit` elsewhere.

Explorer:
- The score list endpoint appears to assign the whole matched pricing object to
  `pricePerPixel` instead of `.PricePerPixel`.
- The per-address endpoint uses `.PricePerPixel` correctly.

Older corporate analytics stack:
- `semantic.network_capabilities` parses `price_per_unit` and `pixels_per_unit`
  from network capability events.
- The final projected view exposes `price_per_unit` but drops `pixels_per_unit`,
  so normalized per-unit pricing is not surfaced cleanly there either.

Conclusion:
- Human-readable pricing remains an ecosystem gap.
- Our pricing issue is still valid, but it is not the only weakness in the broader
  Livepeer analytics stack.
- The specific `PixelsPerUnit: 1` bug is ours.

## Issue ownership matrix

| Issue | Our repo | Explorer | `other-project` | Notes |
|---|---|---|---|---|
| Usage mismatch is apples-to-oranges | Yes | Yes | Yes | Explorer uses fee-derived network minutes; older corporate stack uses observed active minutes; our dashboard needs clearer labeling |
| `Scope` should not be a pipeline ID | Yes | Indirectly | Yes | Older corporate stack clearly separates `client_source` from pipeline/model |
| `Effective %` derivation is incorrect | Yes | No evidence | No evidence | Strong signal this is ours only |
| KPI endpoint mixes unified jobs despite streaming-only contract | Yes | No analog found | No analog found | Strong signal this is ours only |
| Pipeline catalog merges unified job pipeline/model pairs | Yes | No analog found | No analog found | Strong signal this is ours only |
| Pricing unit metadata not cleanly surfaced | Yes | Yes | Yes | Shared gap, but the failure mode differs in each project |
| `pricePerPixel` object-assignment bug in score list endpoint | No | Yes | No evidence | Looks Explorer-only |

## Practical implications

- If Explorer and the older corporate analytics stack align against our current
  implementation, that is a strong signal that our current dashboard/API semantics
  should be tightened rather than defended.
- The strongest alignment across pre-existing Livepeer projects is on:
  - separating source/app identity from pipeline identity
  - treating observed usage minutes separately from fee-derived network estimates
  - keeping startup/output reliability as direct operational facts instead of
    collapsing them into ambiguous top-line metrics

## Recommended next steps

1. Fix `Effective %` to use contracted `effective_success_rate` or the underlying
   `effective_failed_sessions / requested_sessions` math directly.
2. Decide whether `/v1/dashboard/kpi`, `/v1/dashboard/pipelines`, and
   `/v1/dashboard/pipeline-catalog` are streaming-only or multi-workload, then align
   code, docs, and labels in one change.
3. Add dashboard tooltips for `Success Rate`, `Usage`, `Effective %`, and `SLA`.
4. Keep `client_source` and app identities like `Scope` separate from pipeline IDs in
   UI labels and filters.
5. Expose real `pixelsPerUnit` in the dashboard pricing API and add preset
   `$ / minute` conversions such as 720p30 and 1080p30.
6. Consider upstreaming two external notes:
   - Explorer's fee-derived usage label should be more explicit.
   - Explorer's score list endpoint may have a `pricePerPixel` assignment bug.
