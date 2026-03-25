{% macro canonical_payment_pipeline(manifest_id_expr) -%}
replaceRegexpOne({{ manifest_id_expr }}, '^[0-9]+_', '')
{%- endmacro %}
