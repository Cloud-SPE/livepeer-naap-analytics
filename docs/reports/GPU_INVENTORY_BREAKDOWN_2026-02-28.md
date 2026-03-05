# GPU Inventory Breakdown Report (Prod)

Generated: 2026-02-28 18:26:21 UTC

## Scope

Investigate why Grafana `Known GPUs (active 15m)` looks high and split unique GPUs by workload, prioritizing `live-video-to-video`.

Dedup rule used in all report queries:

- `gpu_id_norm = lower(trimBoth(gpu_id))`
- count unique GPUs by `gpu_id_norm`

## Headline Results

- Unique GPUs (15m): `99`
- Unique GPUs by pipeline (15m):
  - `live-video-to-video`: `93`
  - `text-to-image`: `4`
  - `upscale`: `2`

This indicates the current count is mostly `live-video-to-video`, not mostly "other" workloads.

## Pipeline + Model Breakdown (15m)

- `live-video-to-video / streamdiffusion-sdxl-v2v`: `51`
- `live-video-to-video / streamdiffusion-sdxl`: `41`
- `live-video-to-video / noop`: `1`
- `text-to-image / SG161222/RealVisXL_V4.0_Lightning`: `3`
- `text-to-image / black-forest-labs/FLUX.1-dev`: `1`
- `upscale / stabilityai/stable-diffusion-x4-upscaler`: `2`

## Top Orchestrators by Live-Video-to-Video GPUs (15m)

- `0xb120a72a9264e90092e8197c0fabd210c18bc5be`: `24`  ??
- `0x75fbf65a3dfe93545c9768f163e59a02daf08d36`: `12`  streamplace?
- `0x3b28a7d785356dc67c7970666747e042305bfb79`: `11`  ad astra
- `0xdc28f2842810d1a013ad51de174d02eaba192dc7`: `10` pon 
- `0x104a7ca059a35fd4def5ecb16600b2caa1fe1361`: `5`  elite

## Duplication Diagnostics (15m)

- GPU IDs seen on multiple orchestrators: `0`
- GPU IDs seen in multiple pipelines: `0`

Within the 15m window, there is no evidence of double-count inflation from cross-orchestrator or cross-pipeline GPU ID reuse.

## Freshness Window Sensitivity

- `>= 5m`: `0`
- `>= 15m`: `99`
- `>= 30m`: `104`
- `>= 60m`: `104`

Interpretation:

- Capability snapshots appear bursty/coarse, not continuously updated every few minutes.
- A 15m cutoff currently includes many GPUs, but there were no snapshot updates in the last 5 minutes at query time.

## Important Semantics Caveat

`Known GPUs (active 15m)` is an inventory freshness metric from capability snapshots. It is not a "currently in-use GPU" utilization metric.

## Likely Reasons Count Feels High

- Real network supply can be much larger than concurrent active streams.
- A few orchestrators advertise many GPUs, heavily driving totals.
- Snapshot cadence is coarse, so short windows can still represent broad inventory.
- Identity mismatch between status and capability tables (canonical vs local orchestrator address) makes "active stream GPUs" joins unreliable without normalization mapping.

## Recommended Next Panels

1. `Known GPUs by Pipeline (15m)` (table/bar) with `live-video-to-video` pinned first.
2. `Known GPUs by Model (15m)` for live-video-to-video only.
3. `Top Orchestrators by Known GPUs (15m)` with a cap + drilldown.
4. Optional stricter freshness panel: `Known GPUs (active 5m)` to detect reporting lulls.

## Queries Used

```sql
WITH latest_gpu AS (
  SELECT
    lower(trimBoth(gpu_id)) AS gpu_id_norm,
    argMax(ifNull(nullIf(pipeline,''),'unknown'), snapshot_ts) AS pipeline,
    argMax(ifNull(nullIf(model_id,''),'unknown'), snapshot_ts) AS model_id,
    argMax(lower(orchestrator_address), snapshot_ts) AS orchestrator_address,
    max(snapshot_ts) AS latest_snapshot_ts
  FROM livepeer_analytics.dim_orchestrator_capability_snapshots
  WHERE ifNull(trimBoth(gpu_id), '') != ''
  GROUP BY gpu_id_norm
)
SELECT count() AS unique_gpus_15m
FROM latest_gpu
WHERE latest_snapshot_ts >= now() - INTERVAL 15 MINUTE;
```

```sql
WITH latest_gpu AS (
  SELECT
    lower(trimBoth(gpu_id)) AS gpu_id_norm,
    argMax(ifNull(nullIf(pipeline,''),'unknown'), snapshot_ts) AS pipeline,
    argMax(ifNull(nullIf(model_id,''),'unknown'), snapshot_ts) AS model_id,
    max(snapshot_ts) AS latest_snapshot_ts
  FROM livepeer_analytics.dim_orchestrator_capability_snapshots
  WHERE ifNull(trimBoth(gpu_id), '') != ''
  GROUP BY gpu_id_norm
)
SELECT pipeline, count() AS unique_gpus
FROM latest_gpu
WHERE latest_snapshot_ts >= now() - INTERVAL 15 MINUTE
GROUP BY pipeline
ORDER BY (pipeline = 'live-video-to-video') DESC, unique_gpus DESC;
```

