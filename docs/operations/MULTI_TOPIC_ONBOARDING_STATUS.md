# Multi-Topic Onboarding — Status & Open Work

Status snapshot for the `tasks/NaaP-MVP/milestone2/ingest-multi-topics` branch.
Covers changes already implemented, what remains before `network_events` can be
safely onboarded, and known risks.

---

## Suggested Commit Message

```
feat(pipeline): multi-topic ingestion with per-topic org labeling and unknown event capture

Add support for consuming multiple Kafka topics in a single Flink job.
Each topic is mapped to a logical org label (e.g. streaming_events=cloud_spe,
network_events=daydream) propagated as an `org` column through all raw_* and
fact_* ClickHouse tables. Introduce per-topic offset reset strategy config to
safely onboard new topics (e.g. "latest" for network_events to avoid historical
data flood). Add raw_unknown_events table and pipeline sink to capture
unrecognized event types for future promotion. Add NETWORK_EVENTS_ONBOARDING.md
with deployment checklist and risk analysis.

- QualityGateConfig: QUALITY_INPUT_TOPICS (multi-topic), QUALITY_TOPIC_ORG_MAP,
  QUALITY_TOPIC_RESET_STRATEGY_MAP; backwards-compat with QUALITY_INPUT_TOPIC
- Pipeline: one KafkaSource per topic with per-topic OffsetsInitializer, union'd
- RowMapper interface: map(payload, org) — org flows from event.org set at parse time
- ParsedEventRowGuardProcessFunction: passes event.org to mapper
- ClickHouseRowMappers: all mappers accept org param; new unknownEventsRow()
- Schema: org LowCardinality(String) added to all raw_* and fact_* tables;
  new raw_unknown_events table (90-day TTL)
- Tests: all mapper calls updated; unknownEventsRowMatchesSchema() added
- Docs: FLINK_DEPLOYMENT.md updated with real Flink URL
```

---

## What Was Implemented

| Area | Change |
|---|---|
| `QualityGateConfig` | `QUALITY_INPUT_TOPICS` (comma-separated), `QUALITY_TOPIC_ORG_MAP` (topic=org pairs), `QUALITY_TOPIC_RESET_STRATEGY_MAP` (topic=strategy pairs); backwards-compat with `QUALITY_INPUT_TOPIC` |
| Pipeline | One `KafkaSource` per topic with per-topic `OffsetsInitializer`, union'd into a single stream |
| Per-topic offset reset | `"latest"` fallback for new topics avoids historical flood; `"earliest"` preserved for existing topics |
| `RowMapper` interface | Signature changed to `map(payload, org)` — org derived from `event.org` set at ingest time via `topicOrgMap` |
| `ParsedEventRowGuardProcessFunction` | Passes `event.org` to the row mapper |
| `ClickHouseRowMappers` | All mappers accept `org` param; new `unknownEventsRow()` |
| ClickHouse schema | `org LowCardinality(String)` added to all `raw_*` and `fact_*` tables; new `raw_unknown_events` table (90-day TTL) |
| Unknown events sink | `UnknownEvent` payload + pipeline filter for unrecognized event types → `raw_unknown_events` |
| Tests | All mapper calls updated to pass `org`; `unknownEventsRowMatchesSchema()` added |
| Docs | New `NETWORK_EVENTS_ONBOARDING.md`; `FLINK_DEPLOYMENT.md` updated with real Flink URL |

---

## What Remains Before `network_events` Can Be Onboarded

### Code

1. **`CapabilityAttributionSelector.pruneStale` directional fix**

   The current symmetric `Math.abs()` check evicts broadcast state entries that
   are far in the **future** relative to a historical reference timestamp. If any
   historical `network_events` data is processed (even via the backfill job), old
   capability timestamps will evict live `streaming_events` broadcast state and
   break GPU/model attribution for the already-running org.

   ```java
   // Current (symmetric — dangerous during historical processing):
   Math.abs(referenceTs - entry.getValue().snapshotTs) > maxDelta

   // Fix (one-directional — only prune genuinely stale past entries):
   referenceTs - entry.getValue().snapshotTs > maxDelta
   ```

   **Must land before any large-scale historical ingestion from either topic.**

2. **Historical backfill job**

   No implementation exists yet. The onboarding doc describes the design: a
   standalone Flink batch consumer that reads `network_events` from earliest to a
   cutoff offset, applies the same `EventParsers`, and writes only to `raw_*`
   ClickHouse tables — no lifecycle operators, no broadcast state, no fact tables.

   ```
   network_events (earliest → cutoff offset)
     → QualityGate (parse + validate)
     → EventParsers (same as live pipeline)
     → ClickHouse raw_* tables only
     (NO lifecycle aggregators, NO broadcast state, NO fact tables)
   ```

### Operational

3. **Set deployment env vars** before restarting the live Flink job:

   ```
   QUALITY_INPUT_TOPICS=streaming_events,network_events
   QUALITY_TOPIC_ORG_MAP=streaming_events=cloud_spe,network_events=daydream
   QUALITY_TOPIC_RESET_STRATEGY_MAP=streaming_events=earliest,network_events=latest
   ```

4. **Pre-set Kafka offset (optional but recommended)** — run
   `kafka-consumer-groups.sh --reset-offsets --to-datetime ...` for
   `network_events` before restarting Flink if you want to start from a specific
   point rather than the very tip. Must be done _before_ the restart; once a
   committed offset exists the reset strategy map has no effect.

5. **Post-deployment monitoring** — verify `streaming_events` attribution quality
   via `fact_workflow_sessions.attribution_method` and `attribution_confidence`
   after restart to confirm broadcast state was not disturbed.

---

## Open Problems & Risks

| Problem | Severity | Status |
|---|---|---|
| `pruneStale` uses `Math.abs()` — historical timestamps can evict live broadcast state | **High** — live data quality regression for existing org | Documented in onboarding doc; not yet fixed |
| No backfill job implementation | Medium — historical `network_events` raw data is lost without it | Design documented; code missing |
| `docker-compose.yml` Maven cache volume commented out | Low — local builds may be slower | Presumably intentional; reason undocumented |
| `QUALITY_TOPIC_RESET_STRATEGY_MAP` reuses the `envTopicOrgMap` parser — naming is confusing but functional | Low — cosmetic | No action required |

The single highest-priority remaining item is the **broadcast state pruning
direction fix**. Everything else is operational. The pruning bug is the only
change that can cause a live data quality regression for the already-running
`streaming_events` pipeline.
