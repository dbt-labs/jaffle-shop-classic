{% macro firebolt__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}

    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    
    {% set create_or_replace = (old_relation is none or var('ext_full_refresh', false)) %}
    
    {% if create_or_replace %}

        {% set build_plan = [
                dbt_external_tables.dropif(source_node),
                dbt_external_tables.create_external_table(source_node)
            ] + dbt_external_tables.refresh_external_table(source_node) 
        %}
        
    {% else %}
    
        {% set build_plan = dbt_external_tables.refresh_external_table(source_node) %}
        
    {% endif %}
    
    {% do return(build_plan) %}

{% endmacro %}
