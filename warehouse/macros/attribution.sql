{% macro attribution_status_expr(resolved_expr, ambiguous_expr, stale_expr, hardware_less_expr) -%}
multiIf(
    {{ resolved_expr }} = 1, 'resolved',
    {{ ambiguous_expr }} = 1, 'ambiguous',
    {{ hardware_less_expr }} = 1, 'hardware_less',
    {{ stale_expr }} = 1, 'stale',
    'unresolved'
)
{%- endmacro %}

{% macro attribution_reason_expr(resolved_expr, ambiguous_expr, stale_expr, hardware_less_expr) -%}
multiIf(
    {{ resolved_expr }} = 1, 'matched',
    {{ ambiguous_expr }} = 1, 'ambiguous_candidates',
    {{ hardware_less_expr }} = 1, 'matched_without_hardware',
    {{ stale_expr }} = 1, 'stale_candidate',
    'missing_candidate'
)
{%- endmacro %}
