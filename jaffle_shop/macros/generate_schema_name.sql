
{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
    Overrise the default behaviour for schema name generation.

    This is so that schema names are no longer prefixed by the profile name and
    instead reflect the custom schema name exactly.
    -#}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema | trim }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
