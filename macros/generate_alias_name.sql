{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        {{ node.schema }}_{{ node.name }}

    {%- else -%}

        {{ node.schema }}_{{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}