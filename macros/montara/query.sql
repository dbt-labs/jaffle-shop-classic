{% macro query(sql, results_filename) %}
    {% set results = run_query(sql) %}
    {% do print('<SCHEMA>') %}
    {% do results.print_structure() %}
    {% do print('</SCHEMA>') %}
    {% do print('<ROW_COUNT>') %}
    {% do print(results|length) %}
    {% do print('</ROW_COUNT>') %}
    {% do results.to_json(results_filename) %}
{% endmacro %}
