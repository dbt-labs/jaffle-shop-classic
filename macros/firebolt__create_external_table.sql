{% macro firebolt__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

    {# no need to add `IF EXISTS` #}
    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {{adapter.quote(column.name)}} {{column.data_type}}
            {{- ',' if not loop.last or partitions -}}
        {% endfor %}
    {% if partitions -%}
        {%- for partition in partitions -%}
            {{adapter.quote(partition.name)}} {{partition.data_type}} PARTITION('{{partition.regex}}'){{', ' if not loop.last}}
        {%- endfor -%}
    {%- endif %} )
    {% if external.url -%} URL = '{{external.url}}' {%- endif %}
    {% if external.object_pattern -%} OBJECT_PATTERN = '{{external.object_pattern}}' {%- endif %}
    {% if external.type -%} type = {{external.type}} {%- endif %}
{% endmacro %}