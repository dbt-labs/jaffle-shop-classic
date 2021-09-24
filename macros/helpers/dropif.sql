{% macro firebolt__dropif(node) %}
    
    {% set ddl %}
        drop table if exists {{source(node.source_name, node.name)}}
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
