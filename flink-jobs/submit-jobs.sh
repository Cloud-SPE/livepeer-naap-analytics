#!/bin/bash
set -e

echo "===================================\n"
echo "Livepeer Analytics - Flink Job Redeployment\n"
echo "Flink Version: 1.20.3\n"
echo "===================================\n"

JOB_MANAGER_RPC_ADDRESS="flink-jobmanager"
JOB_MANAGER_URL="$JOB_MANAGER_RPC_ADDRESS:8081"
JAR_PATH="/opt/flink/usrlib/livepeer-analytics-flink-0.1.0.jar"
# This must match the string in env.execute() in your Java code
JOB_NAME="Livepeer Analytics Java 1.20"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found at $JAR_PATH"
    exit 1
fi

# 1. Identify and stop existing jobs with the same name
echo "Checking for existing jobs named: $JOB_NAME..."
EXISTING_JOB_IDS=$(/opt/flink/bin/flink list -m $JOB_MANAGER_URL | grep "$JOB_NAME" | grep "RUNNING" | awk '{print $4}')

if [ -z "$EXISTING_JOB_IDS" ]; then
    echo "No existing running jobs found."
else
    for JOB_ID in $EXISTING_JOB_IDS; do
        echo "Stopping existing job ID: $JOB_ID..."
        /opt/flink/bin/flink cancel -m $JOB_MANAGER_URL $JOB_ID
    done
    # Optional: wait a few seconds for resources to clear
    sleep 2
fi

# 2. Submit the new job
echo "Submitting new job to Flink cluster ($JOB_MANAGER_RPC_ADDRESS)..."
/opt/flink/bin/flink run \
    -m $JOB_MANAGER_URL \
    -d \
    -c com.livepeer.analytics.pipeline.StreamingEventsToClickHouse \
    "$JAR_PATH"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "Job deployed successfully!"
    echo "View the Flink UI at: http://localhost:8081"
else
    echo "Job submission failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi
