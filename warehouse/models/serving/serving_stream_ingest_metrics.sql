select *
from {{ ref('stg_stream_ingest_metrics') }}
