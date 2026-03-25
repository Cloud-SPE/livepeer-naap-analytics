select *
from {{ ref('fact_workflow_status_hours') }}
where is_terminal_tail_artifact = 0
