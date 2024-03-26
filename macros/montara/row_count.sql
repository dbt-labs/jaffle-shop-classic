{% macro row_count() %}
    {% set sql_statement %}
            select count(*) from {{ this }}
    {% endset %}
    {{ print(query) }}
    {% if execute %}
        {% set results = run_query(sql_statement) %}
        {% set records = results.columns[0].values() %}
    {% else %}
        {% set records = [] %}
    {% endif %}
    
    {{ log("total rows count: " ~ records[0]) }}
{% endmacro %}