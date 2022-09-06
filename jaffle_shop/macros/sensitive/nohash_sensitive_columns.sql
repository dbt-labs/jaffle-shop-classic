{% macro nohash_sensitive_columns(source_table, join_key=none, project='jaffle_shop') %}

    {% set meta_columns = jaffle_shop.get_meta_columns(source_table, "sensitive", project=project) %}

    {% if join_key is not none -%}
        {{ hash_of_column(join_key) }}
    {%- endif  %}

    {%- for column in meta_columns %}
        {{ column }}  {% if not loop.last %} , {% endif %}
    {%- endfor %}

{% endmacro %}