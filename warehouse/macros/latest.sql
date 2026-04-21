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

{% macro latest_rows_cte(cte_name, source_relation, key_columns, order_columns) -%}
{{ cte_name }} as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    {% for column in key_columns -%}
                    {{ column }}{{ "," if not loop.last }}
                    {% endfor %}
                order by
                    {% for column in order_columns -%}
                    {{ column }}{{ "," if not loop.last }}
                    {% endfor %}
            ) as _latest_row_number
        from {{ source_relation }}
    )
    where _latest_row_number = 1
)
{%- endmacro %}
