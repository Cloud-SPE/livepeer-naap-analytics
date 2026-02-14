# Metrics Schema Design Scratchpad

Purpose: working plan to track lifecycle schema, correlation audit, and downstream metrics tables. Update this file as we evolve the plan; mark items completed when done.

## Key Metrics to Enable
The below are the metrics we need to be able to create.

| Metric Name                     | Aggregation | Category     | Dimensions                                           | Impl. Complexity | Measure               | Source            | Status           | Target / Range                    |
|---------------------------------|-------------|--------------|------------------------------------------------------|------------------|-----------------------|-------------------|------------------|-----------------------------------|
| Up/Down Bandwidth               | —           | Network      | wallet, GPU ID, vram, cuda ver, workflow id, region, time | Medium           | Mbps                  | GPU/Orchestrator  | Blocked (Telemetry Gap) | n/a                          |
| Startup Time                    | —           | Performance  | wallet, GPU ID, workflow id, region, time            | Low              | s                     | Gateway           | Exists Today     | ≤ threshold                       |
| E2E Stream Latency              | —           | Performance  | wallet, GPU ID, workflow id, region, time            | High             | ms                    | Gateway           | Does Not Exist   | ≤ target                          |
| Prompt-to-First-Frame Latency   | —           | Performance  | wallet, GPU ID, workflow id, region, time            | High             | ms                    | Gateway           | Does Not Exist   | ≤ XX ms                           |
| Jitter Coefficient              | —           | Performance  | wallet, GPU ID, workflow id, region, time            | Medium           | σ(fps)/μ(fps)         | GPU/Orchestrator  | Partial          | ≤ 0.1                             |
| Output FPS                      | 5s          | Performance  | wallet, GPU ID, workflow id, region, time            | Low              | Frames per second     | GPU/Orchestrator  | Exists Today     | ≥ target fps (model specific)     |
| Failure Rate                    | —           | Reliability  | wallet, GPU ID, workflow id, region, time            | Medium           | %                     | Gateway           | Partial          | < 1%                              |
| Swap Rate                       | —           | Reliability  | wallet, GPU ID, workflow id, region, time            | Medium           | %                     | Gateway           | Partial          | < 5%                              |

## Plan (Live)

1. Baseline discovery
- Inventory current event schemas and quality gate outputs
- Confirm available correlation fields per event type
- Status: completed

2. Critical contracts
- Dedup contract and idempotency boundary (Flink-only) confirmed
- Serving contracts (filters, time windows, drill-through paths) defined
- Forward-compatible identity model confirmed
- Status: in progress

3. Lifecycle schema (workflow session model)
- Lock deterministic `workflow_session_id` rules
- Define `workflow_sessions` and `workflow_session_segments` grains and fields
- Note: `pipeline_id` required for inference workflows
- Status: completed

4. Correlation field audit
- Map required fields to each event type
- Identify deterministic enrichments vs producer gaps
- Status: completed

5. Curated facts
- Define grains and metric mapping for curated fact tables
- Add required correlation fields to all curated facts
- Validate stream -> model -> GPU attribution as first gating query
- Ensure request id and stream id combination is valid edge
- Status: in progress

6. Aggregate tables (rollups)
- Define rollups (5s/1m/1h) on curated facts
- Specify join-back keys to facts/raw for drill-through
- Status: pending

7. ClickHouse schema/migrations
- Deterministic migration order
- DDL for lifecycle tables, facts, rollups, dictionary sources
- Status: in progress

8. Flink enrichment and sessionization
- Implement deterministic enrichment rules
- Add workflow sessionization outputs
- Add enrichment to attach nearest orchestrator capability snapshot (model/GPU) to stream sessions
- Status: in progress

9. Backfill strategy
- Idempotent backfill via Flink replay
- Partitioned backfills + MV rebuilds
- Future backfill path for orchestrator bandwidth scraper table
- Status: pending

10. Ops/tests/docs
- Health checks (MV lag, dictionary freshness, data freshness)
- Unit/integration tests
- Documentation updates
- Status: pending

## Implementation Progress (Current)

- Completed:
  - Added capability seed catalog: `configs/reference/capability_catalog.v1.json`
  - Added Flink capability normalization utilities and tests.
  - Updated parser + row mapping to emit normalized capability fields.
  - Updated schema proposal/docs for normalized capability fields and attribution quality fields.
  - Added ClickHouse MV-based non-stateful silver fact generation:
    - `mv_ai_stream_status_to_fact_stream_status_samples`
    - `mv_stream_trace_events_to_fact_stream_trace_edges`
    - `mv_stream_ingest_metrics_to_fact_stream_ingest_samples`
  - Added design split documentation: `documentation/PIPELINE_EXECUTION_SPLIT.md`
- In progress:
  - Stateful fact emission and sessionization operators in Flink:
    - `fact_workflow_sessions`
    - `fact_workflow_session_segments`
  - ClickHouse derived `dim_orchestrator_capability_current` roll-forward validation.

## Notes

- Flink is the single source of truth for dedup, schema validation, correlation, and sessionization.
- ClickHouse stores curated facts and rollups only.
- Dictionaries should be fed from data-driven sources, not static config files.
- The data we are focused on is for Live Video AI streams from Livepeer.  The other paths for data processing in livepeer includes BYOC and batch AI jobs. We will need to report metrics on these someday.
- We should add some tables or views that aggregate key network wide information (e.g. GPU inventory, known Orchestrators by job type, known Gateways, etc)

## Metric Validation Inputs (In Progress)

Source file: `scripts/livepeer_samples.jsonl`

### `ai_stream_status`
- Status: partial
- Required Events:
  - event_type: `ai_stream_status`
  - required fields: `stream_id` 114/150, `request_id` 150/150, `pipeline` 150/150, `pipeline_id` 0/150, `gateway` 114/150, `orchestrator_address` 150/150, `orchestrator_url` 150/150, `start_time` 150/150, `output_fps` 150/150, `input_fps` 150/150, `state` 150/150
  - sample count: 150
  - time span: 2026-01-23 to 2026-02-10 (~18 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: `ai_stream_status.start_time` (session start candidate)
  - end event: `ai_stream_status.event_timestamp` (status sample time)
  - formula: supports `Output FPS` and `Jitter` windows directly; `Startup Time` needs first-frame event from trace.
- Gaps / Risks:
  - `pipeline_id` is always empty.
  - `gateway` is missing in 36/150 rows.

### `stream_trace_events`
- Status: partial
- Required Events:
  - event_type: `stream_trace_events`
  - required fields: `stream_id` 146/150, `request_id` 150/150, `pipeline_id` 0/150, `gateway` 0/150, `orchestrator_address` 36/150, `orchestrator_url` 36/150, `trace_type` 150/150, `data_timestamp` 150/150
  - sample count: 150
  - time span: 2026-01-23 to 2026-02-10 (~18 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: `gateway_receive_stream_request`
  - end event: `gateway_receive_first_processed_segment` (first frame candidate), `gateway_receive_first_processed_segment` (stable stream edge candidate), and `gateway_ingest_stream_closed` (session close)
  - formula: can support `Startup Time`, `Prompt-to-First-Frame`, and `E2E Latency` after edge-pair definitions are finalized.
- Gaps / Risks:
  - `gateway` column is absent from curated table.
  - `pipeline_id` is always empty.
  - orchestrator fields are sparse (36/150).

### `ai_stream_events`
- Status: partial
- Required Events:
  - event_type: `ai_stream_events`
  - required fields: `stream_id` 112/150, `request_id` 150/150, `pipeline` 150/150, `pipeline_id` 0/150, `gateway` 0/150, `event_type` 150/150, `capability` 0/150, `message` 150/150
  - sample count: 150
  - time span: 2026-01-23 to 2026-02-10 (~18 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: n/a
  - end event: n/a
  - formula: supports failure numerator (`event_type=error`), denominator must come from session or request counts in lifecycle facts.  some errors are excused as expected part of lifecyle and captured in METRISC_VALIDATION_QUERIES.sql.
- Gaps / Risks:
  - only `event_type=error` observed in sample.
  - `pipeline_id` and `capability` are always empty.
  - `gateway` column is absent from curated table.

### `stream_ingest_metrics`
- Status: partial
- Required Events:
  - event_type: `stream_ingest_metrics`
  - required fields: `stream_id` 100/100, `request_id` 100/100, `pipeline_id` 0/100, `connection_quality` 100/100, `bytes_received` 100/100, `bytes_sent` 100/100, `video_jitter` 100/100, `audio_jitter` 100/100, `video_latency` 100/100, `audio_latency` 100/100
  - sample count: 100
  - time span: 2026-02-10T18:30:03 to 2026-02-10T18:37:08 (~7 minutes)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: sequential ingest samples by `event_timestamp`
  - end event: next ingest sample in sequence
  - formula: supports ingest-leg metrics only (WHIP connection quality/jitter, not orchestrator transport bandwidth).
- Gaps / Risks:
  - currently sampled from only one stream/request, so cadence and out-of-order behavior are not yet representative.
  - `bytes_received`/`bytes_sent` are ingest peer stats and do not represent gateway->orchestrator bandwidth.
  - `pipeline_id` is always empty.

### `payment_events`
- Status: partial
- Required Events:
  - event_type: `payment_events`
  - required fields: `session_id` 150/150, `manifest_id` 150/150, `sender` 150/150, `recipient` 150/150, `orchestrator` 150/150, `request_id` 52/150, `capability` 0/150
  - sample count: 150
  - time span: 2026-01-23 to 2026-02-10 (~18 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: n/a
  - end event: n/a
  - formula: supports payment volume and payer/payee rollups; correlates to lifecycle via either `session_id` (for transcoding events) or `request_id` (for AI events).  This needs to be vetter since many records have no request_id while session_id and manifest_id do not seem to relate to other events (need to prove this out).
- Gaps / Risks:
  - `request_id` coverage is sparse.
  - `capability` is always empty.

### `network_capabilities`
- Status: partial
- Required Events:
  - event_type: `network_capabilities`
  - required fields: `orchestrator_address` 100/100, `orch_uri` 100/100, `pipeline` 100/100, `model_id` 100/100, `gpu_id` 85/100, `gpu_name` 85/100, `runner_version` 80/100
  - sample count: 100
  - time span: 2026-02-03 to 2026-02-10 (~7 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: n/a
  - end event: n/a
  - formula: dimension source for orchestrator/GPU/workflow enrichment, not a latency/failure numerator source.
- Gaps / Risks:
  - GPU and runner fields are not always populated.

### `discovery_results`
- Status: validated
- Required Events:
  - event_type: `discovery_results`
  - required fields: `orchestrator_address` 100/100, `orchestrator_url` 100/100, `latency_ms` 100/100
  - sample count: 100
  - time span: 2026-02-03 to 2026-02-10 (~7 days)
- Sample Sources:
  - file: `scripts/livepeer_samples.jsonl`
- Mapping Notes:
  - start event: n/a
  - end event: n/a
  - formula: supports discovery-latency rollups by orchestrator and time window.
- Gaps / Risks:
  - no direct gateway key in the curated table today, so gateway-level attribution requires enrichment.

### Expected outcome from current data

#### 1) Output FPS
- Status: computable now
- Source: `ai_stream_status.output_fps`, `ai_stream_status.input_fps`, `ai_stream_status.orchestrator_address`, `ai_stream_status.event_timestamp`
- Formula:
  - `avg_output_fps = avg(output_fps)`
  - `p95_output_fps = quantile(0.95)(output_fps)`
  - `fps_efficiency_ratio = avg(output_fps / nullIf(input_fps, 0))`
- Grouping: `orchestrator_address`, `pipeline`, `pipeline_id` (currently empty), time window.

#### 2) Jitter Coefficient
- Status: computable now (FPS jitter), plus separate ingest-network jitter metrics
- Source A (FPS jitter): `ai_stream_status.output_fps`
- Formula A:
  - `jitter_coeff_fps = stddevPop(output_fps) / nullIf(avg(output_fps), 0)`
- Source B (network jitter): `stream_ingest_metrics.video_jitter`, `stream_ingest_metrics.audio_jitter`
- Formula B:
  - `video_jitter_mean = avg(video_jitter)`
  - `audio_jitter_mean = avg(audio_jitter)`
- Grouping: by `request_id`/`stream_id` and time window; optionally by model params (`params_hash`) where available.
- Note: ingest jitter is not equivalent to playback jitter or gateway->orchestrator jitter.

#### 3) Up/Down Bandwidth (inferred)
- Status: blocked for target metric (GPU/Orchestrator bandwidth)
- Current data:
  - ingest-leg throughput can be estimated from `stream_ingest_metrics.bytes_received/bytes_sent`.
  - this reflects WHIP connection behavior, not gateway->orchestrator or orchestrator->worker transport.
- Decision:
  - do not ship target bandwidth KPI from current fields.
  - create future table from orchestrator scraper telemetry for true transport bandwidth.
- Future source (planned):
  - new scraper pulling orchestrator-side per-session transport counters and timestamps.

#### 4) Startup Time
- Status: partial, edge mapping required
- Proposed source edges in `stream_trace_events.trace_type`:
  - start edge candidate: `gateway_receive_stream_request`
  - first playable output candidate: `gateway_receive_first_processed_segment` is a first output edge (`gateway_receive_few_processed_segments` for proxy of a stable session after startup)
- Formula:
  - `startup_ms = ts(first_output_edge) - ts(start_edge)`

#### 5) E2E Stream Latency
- Status: partial, edge mapping required
- Proposed source edges in `stream_trace_events.trace_type`:
  - ingress edge: `gateway_send_first_ingest_segment`
  - returned edge candidate: `gateway_receive_few_processed_segments`
- Formula:
  - `e2e_latency_ms = ts(returned_edge) - ts(ingress_edge)`

#### 6) Prompt-to-First-Frame (or Prompt-to-Playable proxy)
- Status: partial (strict), computable as proxy
- Prompt timestamp candidate: `ai_stream_status.start_time` (or prompt update event if added later)
- First output edge candidate: `stream_trace_events.trace_type = gateway_receive_few_processed_segments`
- Formula:
  - `prompt_to_playable_ms = ts(gateway_receive_few_processed_segments) - start_time`

#### 7) Failure Rate
- Status: partial, computable as startup/failure proxy with current telemetry
- Current approach (mirrors other-project concepts):
  - classify each stream session into `success`, `excused`, or `unexcused`.
  - use session key `stream_id + '|' + request_id` (with fallbacks).
- Numerator:
  - `unexcused` sessions among known streams.
- Denominator:
  - known streams, defined as sessions with trace edge `gateway_receive_stream_request`.
- Formula:
  - `unexcused_failure_rate = unexcused_sessions / known_stream_sessions`
- Excused/success mapping (current proxy):
  - `success`: has `stream_trace_events.trace_type = 'gateway_receive_few_processed_segments'`
  - `excused`: has `gateway_no_orchestrators_available`, or all observed `ai_stream_events.error` messages are in excusable taxonomy
  - `unexcused`: known stream that is neither success nor excused
- Excusable Taxonomoy from Erorr Message Field
  - no orchestrators available
  - mediamtx ingest disconnected
  - whip disconnected
  - missing video
  - ice connection state failed
  - user disconnected

#### 8) Swap Rate
- Status: partial, feasible once swap detection rule is locked
- Source: `stream_trace_events` + sessionized orchestrator assignment
- Detection rule:
  - a swap occurs when orchestrator identity changes within same `workflow_session_id`/`request_id`.
- Formula:
  - `swap_rate = sessions_with_swaps / total_sessions`

#### Edge Trace Mapping Work Needed
- TODO: Build a trace edge dictionary based on METRICS_VALIDATION_QUERIES.sql and kafka_queue_event_catalog.md
- Use validation queries and this doc in the meantime

## Validation Artifacts (Repeatable)

- JSONL validator script: `scripts/validate_metrics_jsonl.py`
- Generated report: `documentation/reports/METRICS_VALIDATION_REPORT.md`
- ClickHouse query pack: `documentation/reports/METRICS_VALIDATION_QUERIES.sql`

Re-run command:

```bash
python3 scripts/validate_metrics_jsonl.py \
  --input scripts/livepeer_samples.jsonl \
  --report documentation/reports/METRICS_VALIDATION_REPORT.md
```

## Transformation Mapping (Trace -> Status -> Metrics)

This section captures the normalization concepts we are retaining from `other-project/clickhouse/semantic/stream_trace_summary.sql` and adapting to current data.

### Session Identity
- Primary validation/session key: `session_key = stream_id + '|' + request_id` (fallback when one side is missing).
- Reason: `request_id` alone is reusable; `stream_id` alone can mask multiple attempts.

### Trace Subtype Mapping (Current)
- `gateway_receive_stream_request` -> marks known-stream denominator membership.
- `gateway_receive_few_processed_segments` -> startup success signal.
- `gateway_no_orchestrators_available` -> excusable startup failure signal.
- `gateway_ingest_stream_closed` -> terminal/supporting signal (classification helper, not sole failure signal).
- `orchestrator_swap` -> explicit swap signal.

### Error Mapping (Current)
- Error source: `ai_stream_events` where `event_type='error'`.
- Excusable taxonomy seed (proxy): messages like `no orchestrators available`, `mediamtx ingest disconnected`, `whip disconnected`, `missing video`, `ICE connection state failed`, `user disconnected`.
- Non-excusable: all other error messages.

### Status Classification Rules (Current Proxy)
- `success`: startup success signal present.
- `excused`: no-orchestrator signal present OR all errors in session are excusable.
- `unexcused`: known stream and not classified as success/excused.

### Fact / Rollup Design Implications
- Fact-level (`fact_workflow_session_status`): one row per `session_key` with derived status, edge flags, error counts, taxonomy counts.
- Rollup-level (`agg_failure_rate_1m|1h|1d`): grouped by time window + dimensions (`gateway`, `orchestrator`, `workflow_id`, `pipeline_id`, `region` when available).
- Drill-through keys: `session_key`, `stream_id`, `request_id`, plus edge/event timestamps to trace and AI-event facts.

## Additional Fact Tables/Views/Dictionaries Needed

This section is a work in progress and will be known more clearly once the intial metrics/queries for these elements are defined.  This is subject to change.

- Current known orchestrator state - IDLE/RUNNING, capabilities, addresses, etc
- Gateway state - # of jobs, job types, fees paid

## SLA Scoring

Once we have all the data collected and stored, we will want to support calculating a SLA score for orchestrators.  The score should allow us to view scores by:

- the entire network
- by jobs through a given orchestrator
- by all jobs through a given gateway
- by pipeline/model
- by GPU
- by region

SLA Scores will be calculated based on a weight formula that balanced various signals.  For example, we might have it based on Jitter component (40%), Latency component (40%), and a Reliability component (20% - error rate detection).

The scoring formula may be Gateway specific and thus needs to be flexible in its defition.  This could mean it is a SQL query off a table that produce key tuples that go through some go code to add weights.  It could be other approaches as well.  This needs to be defined, but we will make a best effort to design something in the first schema without more details.  The end result is being able to show that the Livepeer network is capable of delivering job execution in a predictable and scalable manner.  So being able to report these scores at a given point in time and over various rollup periods will be useful in understanding performance at a given time and over time.  That way we can use this to select an Orchestrator for work not only based on historical work but real time metrics.  This will allow Gateways to make selections based on near-real time data and not potential select an Orchestrator who is historical good, but offline at the moment.

## APIs Needed
/gpu/metrics	GET	Retrieve per-GPU realtime metrics	o_wallet, gpu_id, region, workflow, time_range	JSON with performance/reliability fields
/network/demand	GET	Aggregate network demand data	gateway, region, workflow, interval	Stream/inference mins stats
/sla/compliance	GET	Return SLA compliance score for Orchestrator	orchestrator_id, period	Score (0-100)

## Next Steps (Execution Order)

1. Validate stream attribution to model/GPU before schema freeze.
- Author and run a query that maps stream sessions (`stream_id`, `request_id`) to `orchestrator_address`, `model_id`, and `gpu_id` using `ai_stream_status` + `network_capabilities`.
- Record coverage (% sessions with model_id, % sessions with gpu_id).
g
2. Freeze metric contracts for shippable metrics.
- Startup, E2E proxy, prompt-to-playable proxy, output FPS, jitter, failure proxy, swap.
- Lock edge pairs, timestamp source, denominators, and taxonomy.

3. Finalize fact and rollup schemas for available metrics.
- Keep bandwidth target metric marked blocked.
- Include explicit fields for data quality/coverage in rollups (valid pair counts, missing edge counts).

4. Add future bandwidth telemetry path.
- Define new facts table for orchestrator-side transport bandwidth.
- Plan scraper schedule, schema, and backfill strategy.
