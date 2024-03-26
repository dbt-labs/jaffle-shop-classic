{% macro export(sql, results_filename) %}
    {% set results = run_query(sql) %}
    {% do results.to_csv(results_filename) %}
{% endmacro %}
