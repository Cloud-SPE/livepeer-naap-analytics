#!/bin/bash

set -e

FLINK_JOBMANAGER="flink-jobmanager:8081"
JAR_DIR="/opt/flink/usrlib"

# Function to wait for JobManager to be ready
wait_for_jobmanager() {
    echo "Waiting for Flink JobManager to be ready..."
    for i in {1..30}; do
        if curl -s "http://${FLINK_JOBMANAGER}/overview" > /dev/null 2>&1; then
            echo "JobManager is ready!"
            return 0
        fi
        echo "Attempt $i: JobManager not ready yet, waiting..."
        sleep 2
    done
    echo "ERROR: JobManager failed to start within timeout"
    return 1
}

# Function to check if a job is already running
job_exists() {
    local job_name=$1
    curl -s "http://${FLINK_JOBMANAGER}/jobs/overview" | grep -q "\"name\":\"${job_name}\""
}

# Wait for JobManager
wait_for_jobmanager

# Find the JAR file
JAR_FILE=$(find ${JAR_DIR} -name "*assembly*.jar" -type f | head -n 1)

if [ -z "$JAR_FILE" ]; then
    echo "ERROR: No JAR file found in ${JAR_DIR}"
    ls -la ${JAR_DIR}
    exit 1
fi

echo "Found JAR: ${JAR_FILE}"

# Submit Job 1: Streaming Events to ClickHouse
JOB_NAME="StreamingEventsToClickHouse"
if job_exists "$JOB_NAME"; then
    echo "Job '$JOB_NAME' is already running, skipping submission"
else
    echo "Submitting job: $JOB_NAME"

    # KEY FIX: Explicitly set the JobManager address
    /opt/flink/bin/flink run \
        -d \
        -m ${FLINK_JOBMANAGER} \
        -c com.livepeer.analytics.StreamingEventsToClickHouse \
        "$JAR_FILE"

    if [ $? -eq 0 ]; then
        echo "Job '$JOB_NAME' submitted successfully"
    else
        echo "ERROR: Failed to submit job '$JOB_NAME'"
        exit 1
    fi
fi

echo "All jobs submitted!"