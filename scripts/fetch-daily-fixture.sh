#!/usr/bin/env bash
# Fetch the daily dev-iteration fixture.
#
# 24-hour window picked to be the trailing day of the golden 8-day
# window, so both fixtures target the same underlying behaviour and the
# daily's deterministic identity only changes if the source re-emits
# the same day (rare).
#
# Usage is identical to fetch-golden-fixture.sh; this is just a named
# preset. See docs/operations/replay-harness.md for the dev iteration
# pattern that pairs the daily fixture with `make replay-verify-canonical`.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec "${REPO_ROOT}/scripts/fetch-golden-fixture.sh" \
  --fixture-name raw_events_daily \
  --window-start "2026-04-15 18:00:00" \
  --window-end   "2026-04-16 18:00:00" \
  "$@"
