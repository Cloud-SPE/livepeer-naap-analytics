# GPU ID semantics — why `gpu_id` churns and how to interpret it

Investigation record: 2026-04-18. Triggered by the now-removed
`Orchestrator GPU Changes (7d vs prior 7d)` panel on `naap-supply-inventory`
showing 76 "GPUs Added" for orchestrator
`0x9727b492c4af6c4a8caf4cc1ba6fbb332206d794`, when the orchestrator operator
reported running ~10 physical GPUs.

## TL;DR

- The analytics pipeline is **faithful** — `gpu_id` is not being mutated by
  any layer in naap-analytics. Every layer (raw JSON → staging → canonical →
  api) reports the same 76 distinct UUIDs.
- `gpu_id` is `pynvml.nvmlDeviceGetUUID()` — a hardware-intrinsic NVIDIA
  identifier sourced in ai-runner, passed unchanged through naap-go-livepeer
  into the `network_capabilities` kafka topic.
- The churn has two compounding sources at the orchestrator operator's
  infrastructure — not in our code:
  1. One `orch_address` is advertised by multiple physical hosts (in this
     case three: `dd-us1`, `dd-us2`, `orch.fastagents.biz`).
  2. Each host's UUIDs are themselves not stable across restarts — most
     likely because the underlying GPUs are rented dynamically from a cloud
     pool or the NVIDIA Container Toolkit is configured to synthesize
     per-container device identity.
- The current Supply and Overview dashboards label these values as reported GPU
  UUID counts. The number is faithful, but its *meaning* at the `orch_address`
  level is not "distinct physical GPUs".

## Data flow (end to end)

1. **ai-runner** (Python) —
   `runner/src/runner/utils/hardware.py:86`
   ```python
   uuid = pynvml.nvmlDeviceGetUUID(handle)
   devices[i] = GPUInfo(id=uuid, ...)
   ```
   Exposed at `GET /hardware/info`
   (`runner/src/runner/routes/hardware.py:43`).

2. **naap-go-livepeer worker → orchestrator** —
   The worker fetches `/hardware/info` from the runner container, stores
   into `worker.HardwareInformation`, then registers with the orchestrator:
   `server/ai_worker.go:113`
   ```go
   hdw := workerHardwareToNetWorkerHardware(n.AIWorker.HardwareInformation())
   c.RegisterAIWorker(ctx, &net.RegisterAIWorkerRequest{..., Hardware: hdw})
   ```

3. **Orchestrator aggregate** —
   `core/ai_orchestrator.go:447` `WorkerHardware()` concatenates every
   registered worker's hardware into the announce payload.

4. **Kafka emit** —
   `core/livepeernode.go:356`
   ```go
   lpmon.SendQueueEventAsync("network_capabilities", orchNetworkCapabilities)
   ```

5. **Warehouse landing** —
   Kafka `network_capabilities` topic → `naap.normalized_network_capabilities`
   (the `raw_capabilities` JSON column carries `hardware[].gpu_info[].id`).

6. **Canonical / API** —
   - `mv_canonical_capability_hardware_inventory` unpacks JSON into
     `canonical_capability_hardware_inventory` (one row per
     `(snapshot_row_id, pipeline_id, model_id, gpu_id)`).
   - `mv_canonical_capability_offer_inventory_store_hardware` writes to
     `canonical_capability_offer_inventory_store` (grain: per snapshot ×
     pipeline × model × gpu).
   - `api_observed_capability_offer` is a passthrough view — this is what
     the panel reads.

No `uuid.uuid4()`, no random/salted generation, no re-hashing anywhere in
this chain.

## Verification queries and results

Counts for `0x9727b492c4af6c4a8caf4cc1ba6fbb332206d794` over the last 7
days — every layer reports the same 76:

| Layer | Distinct `gpu_id` in 7d |
|---|---|
| `normalized_network_capabilities` raw events (24,722 events) | — |
| Raw JSON-extracted `gpu_info[].id` | **76** |
| `canonical_capability_hardware_inventory` | **76** |
| `canonical_capability_offer_inventory_store` (hardware_present=1) | **76** |
| `api_observed_capability_offer` | **76** |
| `api_current_gpu_inventory` (last 24h resolver snapshot) | 64 |

Per-single-snapshot physical count:

| metric | value |
|---|---|
| min GPUs per individual snapshot | 1 |
| avg GPUs per individual snapshot | 6.41 |
| max GPUs per individual snapshot | 8 |
| GPUs in the **latest** snapshot | 8 |

Distinct `orch_uri_norm` values behind this `orch_address`:

| orch_uri_norm | distinct_gpus_7d | first_seen | last_seen |
|---|---|---|---|
| `https://dd-us2.fastagents.biz:8935` | 42 | 2026-04-18 | 2026-04-18 |
| `https://dd-us1.fastagents.biz:8935` | 30 | 2026-04-14 | 2026-04-18 |
| `https://orch.fastagents.biz:8935` | 8 | 2026-04-13 | 2026-04-14 |

Per-host live-vs-observed (30 min window):

| orch_uri_norm | live now | total 7d |
|---|---|---|
| `dd-us2` | 4 | 42 |
| `dd-us1` | 4 | 30 |
| `orch.fastagents.biz` | 0 | 8 |

Per-host first-seen histogram (`dd-us2`, today) — continuous ~3–5 new UUIDs
per hour despite only 4 live GPUs, i.e. each GPU is effectively being
re-identified roughly hourly.

### Queries (copy-paste)

```sql
-- Reproduce the panel result for a specific orch.
WITH cur AS (
    SELECT DISTINCT orch_address, gpu_id FROM naap.api_observed_capability_offer
    WHERE orch_address = '0x...'
      AND last_seen >= now() - INTERVAL 7 DAY
      AND hardware_present = 1 AND ifNull(gpu_id, '') != ''
),
pri AS (
    SELECT DISTINCT orch_address, gpu_id FROM naap.api_observed_capability_offer
    WHERE orch_address = '0x...'
      AND last_seen >= now() - INTERVAL 14 DAY
      AND last_seen <  now() - INTERVAL 7 DAY
      AND hardware_present = 1 AND ifNull(gpu_id, '') != ''
)
SELECT (SELECT count() FROM cur) AS current_7d,
       (SELECT count() FROM pri) AS prior_7d;

-- Per-snapshot physical count (truth).
SELECT min(g), round(avg(g), 2), max(g), count() AS snapshots
FROM (
    SELECT uniqExact(gpu_id) AS g
    FROM naap.canonical_capability_hardware_inventory
    WHERE orch_address = '0x...'
      AND snapshot_ts >= now() - INTERVAL 7 DAY AND gpu_id != ''
    GROUP BY snapshot_row_id
);

-- Cluster UUIDs by physical SKU — reveals churn vs real hardware count.
SELECT gpu_model_name, gpu_memory_bytes_total, uniqExact(gpu_id) AS distinct_uuids
FROM naap.canonical_capability_hardware_inventory
WHERE orch_address = '0x...'
  AND snapshot_ts >= now() - INTERVAL 7 DAY AND gpu_id != ''
GROUP BY gpu_model_name, gpu_memory_bytes_total;

-- Distinct hosts behind one orch_address.
SELECT orch_uri_norm,
       uniqExact(gpu_id) AS distinct_gpus_7d,
       min(snapshot_ts) AS first_seen,
       max(snapshot_ts) AS last_seen,
       count() AS snapshot_rows
FROM naap.canonical_capability_hardware_inventory
WHERE orch_address = '0x...'
  AND snapshot_ts >= now() - INTERVAL 7 DAY AND gpu_id != ''
GROUP BY orch_uri_norm
ORDER BY distinct_gpus_7d DESC;

-- Per-host live vs observed.
WITH live AS (
    SELECT orch_uri_norm, uniqExact(gpu_id) AS live_now
    FROM naap.canonical_capability_hardware_inventory
    WHERE orch_address = '0x...'
      AND snapshot_ts >= now() - INTERVAL 30 MINUTE AND gpu_id != ''
    GROUP BY orch_uri_norm
),
total7d AS (
    SELECT orch_uri_norm, uniqExact(gpu_id) AS total_7d
    FROM naap.canonical_capability_hardware_inventory
    WHERE orch_address = '0x...'
      AND snapshot_ts >= now() - INTERVAL 7 DAY AND gpu_id != ''
    GROUP BY orch_uri_norm
)
SELECT t.orch_uri_norm, ifNull(l.live_now, 0) AS live_now, t.total_7d
FROM total7d t LEFT JOIN live l USING (orch_uri_norm)
ORDER BY total_7d DESC;

-- Per-host first-seen-hour histogram for new UUIDs (is churn bursty or continuous?).
SELECT toStartOfHour(first_seen) AS hr, count() AS new_uuids
FROM (
    SELECT min(snapshot_ts) AS first_seen, gpu_id
    FROM naap.canonical_capability_hardware_inventory
    WHERE orch_address = '0x...'
      AND orch_uri_norm = 'https://...:8935'
      AND snapshot_ts >= now() - INTERVAL 7 DAY AND gpu_id != ''
    GROUP BY gpu_id
)
GROUP BY hr ORDER BY hr ASC;
```

## Why `nvmlDeviceGetUUID()` can still churn

NVML's `nvmlDeviceGetUUID()` is hardware-intrinsic and *should* be stable
across reboots / driver reloads / container restarts on bare-metal. The
churn observed here comes from the environment around NVML, not NVML
itself:

- **Multi-host fleet under one `orch_address`.** Multiple physical hosts
  sign announce events with the same on-chain key. Each host contributes
  its own NVML UUIDs — the union across the fleet is much larger than any
  individual host's live count.
- **Dynamic GPU rental pools.** If the orchestrator operator rents from
  services like Lambda Labs, RunPod, Vast.ai, or Prime Intellect, each
  allocation is a fresh VM with different underlying cards. Churn rate
  tracks rental turnover.
- **Container toolkit synthesis.** Certain NVIDIA Container Toolkit
  configurations (particularly with `--gpus all` plus legacy device
  masquerading) can cause containerized NVML to report per-container UUIDs
  rather than host-native ones. Every container restart then looks like a
  fresh GPU.
- **MIG partitioning.** Irrelevant for RTX 5090 (no MIG support), but
  relevant for A100 / H100 / H200 fleets.

Supporting evidence from the sample UUIDs: the fleet mixes UUID versions
(some v5 name-based `...-50e8-...`, some v4 random-looking `...-40fe-...`).
A single GPU firmware does not emit both. Mixed versions within one
`orch_address` indicate multiple hardware / driver generations behind one
identity — consistent with the multi-host hypothesis.

## Local reproducer — container toolkit does NOT cause UUID churn

To rule out the "NVIDIA Container Toolkit synthesizes per-container UUIDs"
hypothesis, a local test on a bare-metal host (1× GTX 1650, NVIDIA Container
Toolkit, Docker 29.4, `nvidia/cuda:12.9.1-base-ubuntu24.04`) launched 20
fresh containers across 4 passthrough configurations:

| Config | Runs | Distinct UUIDs |
|---|---|---|
| `--gpus all` | 5 | 1 |
| `--gpus all -e NVIDIA_VISIBLE_DEVICES=all` | 5 | 1 |
| `--runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=all` | 5 | 1 |
| `--gpus all --ipc=host --pid=host` | 5 | 1 |
| **Overall** | **20** | **1** |

Every container saw `GPU-d8b9bd88-6416-0f7b-d950-fb7c4d528228` — the same
UUID the host reports via `nvidia-smi -L`. On bare-metal with a
straightforward container toolkit install, `nvmlDeviceGetUUID()` is rock
solid stable.

**Conclusion:** the production churn on `0x9727...` is not explained by
container toolkit behavior alone. The combination of evidence (3 distinct
`orch_uri_norm`, ~8-10× per-host UUID churn, mixed UUID versions) points
to:
1. Multi-host fleet under one on-chain identity (confirmed).
2. Per-host churn driven by dynamic GPU allocation from a cloud rental
   pool or a virtualization layer that synthesizes per-allocation identity
   — *not* plain Docker + nvidia-container-toolkit.

Reproducer script: `scripts/test-gpu-uuid-stability.sh`. Run it on any
NVIDIA host to verify toolkit baseline stability in your environment.

## Recommended fixes (dashboard / analytics side)

Not an ai-runner or naap-go-livepeer change. The source of truth is correct;
we just need the analytics to report identity that matches operator intent.

1. **Replace `gpu_id` with a SKU fingerprint in dashboard dedup.**
   Use `(orch_uri_norm, gpu_model_name, gpu_memory_bytes_total)` as the
   identity. "GPUs Added" then reports added SKU capacity per host, not
   UUID churn.
2. **Stable physical identity in the canonical layer.** Add a
   `gpu_physical_id = cityHash64(orch_uri_norm, gpu_model_name,
   gpu_memory_bytes_total, gpu_enumeration_index)` column in
   `canonical_capability_hardware_inventory`, and have dashboards prefer
   that over `gpu_id`. Keep `gpu_id` for auditability / debugging.
3. **Report churn explicitly.** Surface both `distinct_physical_sku_per_host`
   and `distinct_gpu_uuids_observed` on the panel so operators can spot
   rental-pool / container-churn patterns distinct from actual hardware
   changes.

Deferring implementation to a follow-up pass (see `#4` and related items
in the medallion-v2 promotion plan).

## References

- Dashboard usage: `infra/grafana/dashboards/naap-supply-inventory.json`
  reports "GPU UUIDs by Type (Seen in 24h)" and "Latest GPU UUID Inventory
  Seen in 24h". The former GPU change-comparison panels were removed because
  reported UUID churn made physical-add/remove interpretation invalid.
- Canonical MVs: `infra/clickhouse/bootstrap/v1.sql` (`mv_canonical_capability_hardware_inventory`,
  `mv_canonical_capability_offer_inventory_store_hardware`).
- ai-runner NVML call: `ai-runner/runner/src/runner/utils/hardware.py:86`.
- naap-go-livepeer emit path: `core/livepeernode.go:356`,
  `core/ai_orchestrator.go:447`, `server/ai_worker.go:113`.
