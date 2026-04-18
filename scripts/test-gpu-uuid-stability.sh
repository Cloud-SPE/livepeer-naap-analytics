#!/usr/bin/env bash
# Test whether NVIDIA GPU UUIDs reported inside Docker containers are stable
# across container restarts under different passthrough configurations.
#
# Context: docs/references/gpu-id-semantics.md explains why gpu_id churns in
# production analytics. This script is the local reproducer — on a bare-metal
# host with NVIDIA Container Toolkit, UUIDs SHOULD be stable. If they are,
# the production churn is coming from the operator's environment (rental
# pool, multi-host fleet, vGPU layer), not from container runtime behavior.
#
# Requirements: docker, nvidia-container-toolkit, one or more NVIDIA GPUs.
# Usage:  bash scripts/test-gpu-uuid-stability.sh [RUNS_PER_CONFIG]
# Default RUNS_PER_CONFIG=5.

set -euo pipefail

IMG="${IMG:-nvidia/cuda:12.9.1-base-ubuntu24.04}"
RUNS="${1:-5}"

echo "=== host =================================================================="
nvidia-smi -L
host_uuids=$(nvidia-smi -L | awk -F'UUID: ' '{print $2}' | tr -d ')' | sort -u)
host_count=$(echo "$host_uuids" | wc -l)
echo "host distinct UUIDs: $host_count"
echo ""

declare -a CONFIGS=(
  "--gpus all"
  "--gpus all -e NVIDIA_VISIBLE_DEVICES=all"
  "--runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=all"
  "--gpus all --ipc=host --pid=host"
)

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

overall_all=$(mktemp)

for cfg in "${CONFIGS[@]}"; do
  echo "=== config: $cfg (x$RUNS fresh containers) ================================="
  cfg_out="$TMPDIR/$(echo "$cfg" | tr ' /' '__').txt"
  : > "$cfg_out"
  for i in $(seq 1 "$RUNS"); do
    out=$(docker run --rm $cfg "$IMG" nvidia-smi -L 2>&1 || true)
    echo "$out" | awk -F'UUID: ' '/UUID:/ {print $2}' | tr -d ')' >> "$cfg_out"
    first=$(echo "$out" | head -1)
    printf "  run %2d: %s\n" "$i" "$first"
  done
  distinct=$(sort -u < "$cfg_out" | wc -l)
  total=$(wc -l < "$cfg_out")
  echo "  -> $distinct distinct UUIDs across $total UUID observations for this config"
  echo ""
  cat "$cfg_out" >> "$overall_all"
done

echo "=== overall ==============================================================="
overall_distinct=$(sort -u "$overall_all" | wc -l)
overall_total=$(wc -l < "$overall_all")
echo "$overall_distinct distinct UUIDs across $overall_total UUID observations"
echo ""
echo "distinct UUIDs observed:"
sort -u "$overall_all"
echo ""

if [ "$overall_distinct" -eq "$host_count" ]; then
  echo "RESULT: STABLE. Container UUIDs match host ($host_count GPU) — NVIDIA"
  echo "        Container Toolkit preserves NVML identity across container starts."
  echo "        Production churn is NOT caused by container toolkit alone on"
  echo "        bare metal; look for multi-host fleet or dynamic rental pool."
  exit 0
else
  echo "RESULT: CHURN. Observed $overall_distinct distinct UUIDs vs $host_count on the"
  echo "        host. Something in the container passthrough is synthesizing per-"
  echo "        container identity. Investigate toolkit version and config."
  exit 1
fi
