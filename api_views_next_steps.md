# API Views Next Steps

## Why We Need This
- Current `v_api_*` views are working but carrying too much business logic.
- This increases maintenance cost and makes metric semantics harder to evolve safely.
- Goal: move deterministic lifecycle semantics upstream (Flink), keep ClickHouse serving clean, and keep API contracts stable.

## Recommendations

1. Keep API views thin
- `v_api_*` should primarily project fields from serving-layer views/tables.
- Avoid large multi-CTE semantic logic directly in API views.

2. Move deterministic semantics to Flink
- Sessionization, attribution, edge pairing, and served/unserved classification should be emitted as facts.
- ClickHouse should aggregate and serve, not infer complex workflow behavior.

3. Introduce serving-layer views
- Add internal serving surfaces:
  - `v_serving_gpu_metrics_1m`
  - `v_serving_network_demand_1h`
  - `v_serving_network_supply_1h`
  - optional `v_serving_sla_1h`
- Then map API wrappers 1:1 to these surfaces.

4. Keep explicit quality observability
- Add dedicated coverage views (deferred where noted), rather than embedding fallback heuristics in API views.

5. Version semantics explicitly
- Use fields like `edge_semantics_version` so metric definition updates can be rolled out safely and compared.

## Specific Flink Logic To Add

1. Served vs unserved session classification
- Emit:
  - `served_session` (UInt8)
  - `unserved_session` (UInt8)
  - `service_failure_reason` (LowCardinality(String))

2. Stable orchestrator assignment milestone
- Emit:
  - `orchestrator_assigned_ts` (DateTime64)

3. Session demand duration primitives
- Emit:
  - `served_duration_ms` (UInt64)
  - optional `active_duration_ms` (UInt64)

4. Capacity proxy primitives at attribution time
- Emit:
  - `capacity_units_at_start` (Nullable(Int32/UInt32))
  - `capacity_snapshot_ts` (Nullable(DateTime64))

5. Latency semantics hardening (in progress)
- Keep:
  - `prompt_to_first_frame_ms`
  - `startup_time_ms`
  - `e2e_latency_ms`
  - `edge_semantics_version`
- Add optional null-reason diagnostics per metric.

6. Attribution quality on serving-driving facts
- Ensure serving facts include:
  - `gpu_attribution_method`
  - `gpu_attribution_confidence`

7. Optional compact serving session fact
- Consider a single Flink-emitted session serving fact with:
  - lifecycle classification
  - latency KPIs
  - attribution keys/quality
  - served/unserved flags

## Execution Steps

1. Freeze baseline
- Keep current `v_api_*` definitions as interim baseline for parity checks.

2. Build serving-layer views
- Create `v_serving_*` views with the existing complex logic currently in `v_api_*`.

3. Convert API views to wrappers
- Rewrite:
  - `v_api_gpu_metrics` -> select from `v_serving_gpu_metrics_1m`
  - `v_api_network_demand` -> select from `v_serving_network_demand_1h`
  - `v_api_sla_compliance` -> select from `v_serving_sla_1h`

4. Add/extend Flink facts
- Implement the Flink fields listed above (served/unserved, assignment ts, demand duration, capacity primitives).

5. Wire ClickHouse rollups to new facts
- Update/add rollups that consume new Flink facts and expose stable serving grains.

6. Expand SQL validation
- Add assertions for:
  - grain uniqueness
  - additive-field consistency
  - latency validity
  - served/unserved consistency
  - wrapper parity (`v_api_*` vs `v_serving_*`)

7. Cutover with parity window
- Run interim and new serving in parallel.
- Compare 24h/7d parity before removing old logic.

## Deferred Item
- `v_api_attribution_coverage_1h` remains deferred and tracked in scratchpad backlog.
