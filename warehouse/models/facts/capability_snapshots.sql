{{ config(
    engine='MergeTree()',
    order_by=['orch_address', 'snapshot_ts'],
    partition_by='toYYYYMM(snapshot_ts)'
) }}

-- orch_address in the raw event is the node's ephemeral signing key, not the on-chain wallet.
-- local_address in the raw_capabilities JSON is the actual orchestrator wallet address —
-- this is what the gateway uses as orch_raw_address when routing sessions.
-- We use local_address as the canonical orch_address for joins against session data.
-- signing_address preserves the original field for reference.
select
    row_id as snapshot_row_id,
    event_id as source_event_id,
    event_ts as snapshot_ts,
    org,
    lower(JSONExtractString(raw_capabilities, 'local_address')) as orch_address,
    orch_address as signing_address,
    orch_name,
    orch_uri,
    version,
    raw_capabilities
from {{ ref('stg_network_capabilities') }}
where JSONExtractString(raw_capabilities, 'local_address') != ''
