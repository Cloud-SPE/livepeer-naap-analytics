select
    toStartOfHour(event_ts) as hour,
    org,
    orch_address,
    avg(latency_ms) as avg_latency_ms,
    quantile(0.5)(latency_ms) as p50_latency_ms,
    quantile(0.95)(latency_ms) as p95_latency_ms,
    quantile(0.99)(latency_ms) as p99_latency_ms,
    count() as sample_count
from {{ ref('canonical_discovery_results') }}
where latency_ms > 0
group by hour, org, orch_address
