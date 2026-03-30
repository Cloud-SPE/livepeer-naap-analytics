{% macro join_on_columns(left_alias, right_alias, columns) -%}
{%- for column in columns -%}
{{ left_alias }}.{{ column }} = {{ right_alias }}.{{ column }}{% if not loop.last %} and {% endif %}
{%- endfor -%}
{%- endmacro %}

{% macro latest_value_cte(cte_name, source_relation, key_columns, value_column, order_column='refreshed_at') -%}
{{ cte_name }} as (
    select
        {% for column in key_columns -%}
        {{ column }}{{ "," if not loop.last }}
        {% endfor %},
        argMax({{ value_column }}, {{ order_column }}) as {{ value_column }}
    from {{ source_relation }}
    group by
        {% for column in key_columns -%}
        {{ column }}{{ "," if not loop.last }}
        {% endfor %}
)
{%- endmacro %}
