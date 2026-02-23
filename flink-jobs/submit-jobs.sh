#!/bin/bash
set -e

echo "===================================\n"
echo "Livepeer Analytics - Flink Job Redeployment\n"
echo "Flink Version: 1.20.3\n"
echo "===================================\n"

JOB_MANAGER_RPC_ADDRESS="${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
JOB_MANAGER_ALT_ADDRESS="${FLINK_JOBMANAGER_ALT_HOST:-livepeer-analytics-flink-jobmanager}"
JOB_MANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_MANAGER_URL="$JOB_MANAGER_RPC_ADDRESS:$JOB_MANAGER_PORT"
JAR_STABLE="/opt/flink/usrlib/livepeer-analytics-flink.jar"
JAR_GLOB="/opt/flink/usrlib/livepeer-analytics-flink-*.jar"
# This must match the string in env.execute() in your Java code
JOB_NAME="Livepeer Analytics Java 1.20"

JAR_PATH=""
if [ -f "$JAR_STABLE" ]; then
    JAR_PATH="$JAR_STABLE"
else
    JAR_PATH=$(ls -1t $JAR_GLOB 2>/dev/null | grep -v "/original-" | head -n 1 || true)
fi
if [ -z "$JAR_PATH" ] || [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found. Tried stable path $JAR_STABLE and glob $JAR_GLOB"
    echo "Contents of /opt/flink/usrlib:"
    ls -lah /opt/flink/usrlib || true
    exit 1
fi
echo "Using JAR: $JAR_PATH"

# Wait for a reachable JobManager endpoint (service DNS can lag during startup).
echo "Waiting for Flink JobManager endpoint..."
MAX_ATTEMPTS=24
ATTEMPT=1
RESOLVED=0
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    for CANDIDATE in "$JOB_MANAGER_RPC_ADDRESS" "$JOB_MANAGER_ALT_ADDRESS"; do
        CANDIDATE_URL="$CANDIDATE:$JOB_MANAGER_PORT"
        if /opt/flink/bin/flink list -m "$CANDIDATE_URL" >/dev/null 2>&1; then
            JOB_MANAGER_RPC_ADDRESS="$CANDIDATE"
            JOB_MANAGER_URL="$CANDIDATE_URL"
            RESOLVED=1
            break
        fi
    done
    if [ $RESOLVED -eq 1 ]; then
        break
    fi
    echo "JobManager not reachable yet (attempt $ATTEMPT/$MAX_ATTEMPTS), retrying in 5s..."
    ATTEMPT=$((ATTEMPT + 1))
    sleep 5
done

if [ $RESOLVED -ne 1 ]; then
    echo "ERROR: Could not reach Flink JobManager via $JOB_MANAGER_RPC_ADDRESS:$JOB_MANAGER_PORT or $JOB_MANAGER_ALT_ADDRESS:$JOB_MANAGER_PORT"
    exit 1
fi

echo "Using JobManager endpoint: $JOB_MANAGER_URL"

# 1. Identify and stop existing jobs with the same name
echo "Checking for existing jobs named: $JOB_NAME..."
EXISTING_JOB_IDS=$(/opt/flink/bin/flink list -m "$JOB_MANAGER_URL" | grep "$JOB_NAME" | grep "RUNNING" | awk '{print $4}')

if [ -z "$EXISTING_JOB_IDS" ]; then
    echo "No existing running jobs found."
else
    for JOB_ID in $EXISTING_JOB_IDS; do
        echo "Stopping existing job ID: $JOB_ID..."
        /opt/flink/bin/flink cancel -m "$JOB_MANAGER_URL" "$JOB_ID"
    done
    # Optional: wait a few seconds for resources to clear
    sleep 2
fi

# 2. Submit the new job
echo "Submitting new job to Flink cluster ($JOB_MANAGER_RPC_ADDRESS)..."
/opt/flink/bin/flink run \
    -m "$JOB_MANAGER_URL" \
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
