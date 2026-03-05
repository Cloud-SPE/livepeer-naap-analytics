# GPU Inventory Breakdown Report (Prod, 72h)

Generated: 2026-02-28 20:54:00 UTC

## Scope

Re-run GPU inventory analysis on a 72-hour window to catch intermittently reported capability snapshots.def1

Dedup rule:

- `gpu_id_norm = lower(trimBoth(gpu_id))`
- `latest_snapshot_ts = max(snapshot_ts)` per `gpu_id_norm`
- include GPU if `latest_snapshot_ts >= now() - INTERVAL 72 HOUR`

## Headline Results

- Unique GPUs (72h): `123`
- Unique GPUs by pipeline (72h):
  - `live-video-to-video`: `117`
  - `text-to-image`: `4`
  - `upscale`: `2`

## Pipeline + Model Breakdown (72h)

- `live-video-to-video / streamdiffusion-sdxl`: `54`
- `live-video-to-video / streamdiffusion-sdxl-v2v`: `51`
- `live-video-to-video / streamdiffusion`: `8`
- `live-video-to-video / streamdiffusion-sdxl-faceid`: `2`
- `live-video-to-video / streamdiffusion-sd15-v2v`: `1`
- `live-video-to-video / noop`: `1`
- `text-to-image / SG161222/RealVisXL_V4.0_Lightning`: `3`
- `text-to-image / black-forest-labs/FLUX.1-dev`: `1`
- `upscale / stabilityai/stable-diffusion-x4-upscaler`: `2`

## Top Orchestrators by Live-Video-to-Video GPUs (72h)

- `0xb120a72a9264e90092e8197c0fabd210c18bc5be`: `27`
- `0x75fbf65a3dfe93545c9768f163e59a02daf08d36`: `17`
- `0x3b28a7d785356dc67c7970666747e042305bfb79`: `14`
- `0x180859c337d14edf588c685f3f7ab4472ab6a252`: `12`
- `0xdc28f2842810d1a013ad51de174d02eaba192dc7`: `10`

## Duplication Diagnostics (72h)

- GPU IDs seen on multiple orchestrators: `0`
- GPU IDs seen in multiple pipelines: `0`

No cross-orchestrator or cross-pipeline duplicate GPU IDs were detected in this 72-hour deduped view.

## Interpretation

- The 72-hour window increases coverage versus short freshness windows and does surface additional GPUs that were not in the 15-minute report.
- The inventory is still overwhelmingly `live-video-to-video`; the higher total is not mainly from transcoding/image pipelines.
- Differences between short and long windows are consistent with polling/reporting cadence gaps rather than dedupe failure.
