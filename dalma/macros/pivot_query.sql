{% macro pivot_query(source,
                     primary_keys,
                     column,
                     values,
                     alias=True,
                     agg='sum',
                     cmp='=',
                     prefix='',
                     suffix='',
                     then_value=1,
                     else_value=0,
                     quote_identifiers=True,
                     distinct=False) %}
    {% set comma_separated_pk = primary_keys|join(', ') %}

    {% set sql -%}
    select {{comma_separated_pk}}
    {{ HelloWorld_SQL.pivot(column, values, alias, agg, cmp, prefix, suffix, then_value, else_value, quote_identifiers, distinct) }}
    from {{source}}
    group by {{comma_separated_pk}}

    {%- endset -%}
{% endmacro %} 