# Warehouse

dbt owns the canonical analytics model downstream of the ClickHouse typed
tables.

Ownership boundary:

- ClickHouse: raw Kafka ingest and `raw -> typed`
- dbt: `typed -> facts -> serving`
- Go API: reads serving views only

The project intentionally stays small. New facts or serving layers are added
only when they close a named v3 defect or validation gap.

Fail-safe attribution is an explicit invariant:

- unresolved, ambiguous, stale, and hardware-less states stay visible in fact
  and serving outputs
- those rows must not be dropped from demand, SLA, or reliability views
- only `resolved` rows count as fully attributed identity
