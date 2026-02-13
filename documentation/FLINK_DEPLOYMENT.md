This guide provides the necessary commands and steps to manage, upload, and update Flink jobs on the Livepeer cluster.

---

## 1. Monitoring Cluster Health

Before deploying, verify the available resources and identify running jobs.

* **Check Cluster Health & Slots:**


`curl http://YOUR_FLINK_SERVER:8081/overview`


* **Get Running Job IDs:**


`curl http://YOUR_FLINK_SERVER:8081/jobs/overview`



---

## 2. Standard Deployment (First Time)

Follow these steps for an initial deployment or a simple restart without state recovery.

### Step 1: Upload the JAR

```bash
curl -X POST -H "Expect:" -F "jarfile=@livepeer-analytics-flink-0.1.0.jar" http://YOUR_FLINK_SERVER:8081/jars/upload
```

### Step 2: Run the Job
Use the `jarid` returned from the upload step to execute the job:

```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<jarid>/run \
     -H "Content-Type: application/json" \
     -d '{"entryClass": "com.livepeer.analytics.StreamingEventsToClickHouse", "parallelism": 1}'
```

## 3. "Clean Update" Process
Use this workflow to update a JAR or restart a service while preserving state via a **Savepoint**—a manually triggered, consistent snapshot meant for migrations.

### Step 1: Stop the Job with a Savepoint
Trigger a stop that creates a savepoint to capture the current state:
```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jobs/<jid>/stop \
     -H "Content-Type: application/json" \
     -d '{"drain": false}'
```

### Step 2: Retrieve the Savepoint Path
Using the `jobid` and the `request-id` from the previous step, fetch the storage location:
```bash
curl http://YOUR_FLINK_SERVER:8081/jobs/<jid>/savepoints/<request-id>
```

> **Note:** Look for the `"location"` field in the output (e.g., `file:/opt/flink/storage/savepoints/...`)[cite: 2].

### Step 3: Upload the New JAR

```bash
curl -X POST -H "Expect:" -F "jarfile=@livepeer-analytics-flink-0.1.0.jar" http://YOUR_FLINK_SERVER:8081/jars/upload
```

### Step 4: Run with State Recovery
Execute the new JAR while pointing to the savepoint path retrieved in Step 2:

```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<new_jarid>/run \
     -H "Content-Type: application/json" \
     -d '{
       "entryClass": "com.livepeer.analytics.pipeline.StreamingEventsToClickHouse",
       "parallelism": 1,
       "savepointPath": "/opt/flink/storage/savepoints/<savepoint-id>"
     }'
```