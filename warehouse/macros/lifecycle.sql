{% macro canonical_session_key(org_expr, stream_id_expr, request_id_expr) -%}
multiIf(
    nullIf({{ org_expr }}, '') IS NULL,
    '',
    nullIf({{ stream_id_expr }}, '') IS NOT NULL AND nullIf({{ request_id_expr }}, '') IS NOT NULL,
    concat({{ org_expr }}, '|', {{ stream_id_expr }}, '|', {{ request_id_expr }}),
    nullIf({{ stream_id_expr }}, '') IS NOT NULL,
    concat({{ org_expr }}, '|', {{ stream_id_expr }}, '|_missing_request'),
    nullIf({{ request_id_expr }}, '') IS NOT NULL,
    concat({{ org_expr }}, '|_missing_stream|', {{ request_id_expr }}),
    ''
)
{%- endmacro %}

{% macro canonical_startup_outcome(playable_seen_expr, no_orch_expr, started_expr) -%}
multiIf(
    {{ started_expr }} = 0, 'outside_denominator',
    {{ playable_seen_expr }} = 1, 'success',
    {{ no_orch_expr }} = 1, 'excused',
    'unexcused'
)
{%- endmacro %}

{% macro running_state_expr(state_expr) -%}
({{ state_expr }} IN ('ONLINE', 'DEGRADED_INFERENCE', 'DEGRADED_INPUT'))
{%- endmacro %}
