#!/usr/bin/env bash
set -euo pipefail

LOOKBACK_HOURS="${LOOKBACK_HOURS:-24}"
SCENARIO_LOOKBACK_HOURS="${SCENARIO_LOOKBACK_HOURS:-720}"

python scripts/run_clickhouse_query_pack.py --lookback-hours "${LOOKBACK_HOURS}"

python scripts/run_clickhouse_data_tests.py \
  --sql-file tests/integration/sql/assertions_raw_typed.sql \
  --lookback-hours "${LOOKBACK_HOURS}"

python scripts/run_clickhouse_data_tests.py \
  --sql-file tests/integration/sql/assertions_pipeline.sql \
  --lookback-hours "${LOOKBACK_HOURS}"

python scripts/run_clickhouse_data_tests.py \
  --sql-file tests/integration/sql/assertions_scenario_candidates.sql \
  --lookback-hours "${SCENARIO_LOOKBACK_HOURS}"
