#!/bin/bash
set -e

echo "==================================="
echo "Livepeer Analytics - Flink Job Submission"
echo "==================================="

JAR_PATH="./flink-jobs/target/livepeer-analytics-flink-0.1.0.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found at $JAR_PATH"
    echo "Please build the project first:"
    echo "  cd flink-jobs && mvn clean package"
    exit 1
fi

echo "JAR file found: $JAR_PATH"
echo "Submitting job to Flink cluster..."

docker exec flink-jobmanager /opt/flink/bin/flink run \
    -d \
    -c com.livepeer.analytics.StreamingEventsToClickHouse \
    /opt/flink/usrlib/livepeer-analytics-flink-0.1.0.jar

echo ""
echo "Job submitted successfully!"
echo "View jobs at: http://localhost:8081"