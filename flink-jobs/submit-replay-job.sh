#!/bin/bash
set -e

echo "==================================="
echo "Livepeer Analytics - DLQ Replay Job Submission"
echo "Flink Version: 1.20.3"
echo "==================================="

JOB_MANAGER_RPC_ADDRESS="flink-jobmanager"
JAR_PATH="/opt/flink/usrlib/livepeer-analytics-flink-0.1.0.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found at $JAR_PATH"
    exit 1
fi

echo "JAR file found: $JAR_PATH"
echo "Submitting replay job to Flink cluster ($JOB_MANAGER_RPC_ADDRESS)..."

/opt/flink/bin/flink run \
    -m $JOB_MANAGER_RPC_ADDRESS:8081 \
    -d \
    -c com.livepeer.analytics.pipeline.DlqReplayJob \
    "$JAR_PATH"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "Replay job submitted successfully!"
    echo "View the Flink UI at: http://localhost:8081"
else
    echo "Replay job submission failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi
