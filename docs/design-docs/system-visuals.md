# System Visuals

This document gives the supported end-to-end view of the analytics system.
For the detailed component rules, see [`architecture.md`](architecture.md).
For operational modes and recovery procedures, see [`../operations/run-modes-and-recovery.md`](../operations/run-modes-and-recovery.md).

## End-To-End Data Flow

```mermaid
flowchart LR
    K1[Kafka: network_events]
    K2[Kafka: streaming_events]
    CHK[ClickHouse Kafka engine tables]
    RAW[accepted_raw_events / ignored_raw_events]
    NORM[normalized_* tables and rollups]
    RES[resolver]
    CSTORE[canonical_*_store tables]
    DBT[dbt semantic publish]
    CVIEW[canonical_* views]
    ABASE[api_base_* views]
    AVIEW[api_* views]
    API[Go API]
    GRAF[Grafana]

    K1 --> CHK
    K2 --> CHK
    CHK --> RAW
    RAW --> NORM
    NORM --> RES
    RES --> CSTORE
    CSTORE --> DBT
    DBT --> CVIEW
    DBT --> ABASE
    ABASE --> AVIEW
    AVIEW --> API
    AVIEW --> GRAF
```

## ClickHouse And dbt Responsibilities

```mermaid
flowchart TB
    subgraph ClickHouse
        INGEST[Kafka engine + ingest MVs]
        PHYS[raw_* / normalized_* / resolver runtime / canonical_*_store / api_*_store]
    end

    subgraph Warehouse
        DBT[dbt models]
        CAN[canonical_* semantic views]
        APIB[api_base_* semantic helper views]
        APIV[api_* semantic views]
    end

    INGEST --> PHYS
    PHYS --> DBT
    DBT --> CAN
    DBT --> APIB
    APIB --> APIV
```

## Resolver Publication Spine

```mermaid
flowchart LR
    RAW[accepted_raw_events]
    SEL[canonical_selection_events]
    ATTR[canonical_selection_attribution_current]
    CUR[canonical_session_current_store]
    ROLL[canonical_status_hours_store / demand inputs / serving stores]
    DIRTY[resolver_dirty_partitions]
    WINDOWS[resolver_dirty_windows]
    REQUESTS[resolver_repair_requests]
    CLAIMS[resolver_window_claims]

    RAW --> DIRTY
    RAW --> WINDOWS
    DIRTY --> CLAIMS
    WINDOWS --> CLAIMS
    REQUESTS --> CLAIMS
    CLAIMS --> SEL
    SEL --> ATTR
    ATTR --> CUR
    CUR --> ROLL
```

## Standard Deployment Topology

```mermaid
flowchart LR
    subgraph Runtime
        CKH[ClickHouse]
        RES[resolver]
        API[api]
        GRAF[Grafana / dashboards]
    end

    subgraph Tooling
        DBT[warehouse dbt run / compile]
    end

    subgraph Inputs
        KAFKA[Kafka]
        LP[Livepeer API]
    end

    KAFKA --> CKH
    LP --> API
    CKH <--> RES
    CKH <--> API
    CKH <--> DBT
    API --> GRAF
    CKH --> GRAF
```

## Read Priorities

- API-first reads come from resolver-fed org/window serving stores and their dbt-published `api_*` views.
- Ingest and replay correctness come next: accepted raw, normalized tables, and resolver repair state are optimized for append and bounded repair.
- Dashboards should prefer published `api_*` views and `agg_*` tables rather than rebuilding semantics from raw history.
