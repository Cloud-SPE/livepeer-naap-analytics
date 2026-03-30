select
    hour,
    org,
    ifNull(orch_address, '') as orch_address,
    canonical_pipeline as pipeline,
    sum(avg_output_fps * status_samples) as inference_fps_sum,
    sum(avg_input_fps * status_samples) as input_fps_sum,
    sum(status_samples) as sample_count
from {{ ref('canonical_status_hours') }}
where is_terminal_tail_artifact = 0
group by hour, org, orch_address, pipeline
